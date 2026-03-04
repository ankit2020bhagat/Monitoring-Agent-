"""
AI-powered monitoring agent using LangChain.
Analyzes anomalies, determines root causes, and executes auto-remediation.
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import structlog

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage, HumanMessage

from app.core.config import settings
from app.agents.tools import ALL_TOOLS, DIAGNOSTIC_TOOLS, REMEDIATION_TOOLS
from app.models.alerts import RemediationAction

logger = structlog.get_logger()


def _get_llm():
    """Instantiate the configured LLM provider."""
    if settings.ai_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=settings.ai_model,
            anthropic_api_key=settings.anthropic_api_key,
            max_tokens=4096,
            temperature=0,
        )
    else:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model="gpt-4o",
            openai_api_key=settings.openai_api_key,
            max_tokens=4096,
            temperature=0,
        )


SYSTEM_PROMPT = """You are an expert DevOps/SRE monitoring agent responsible for analyzing 
system telemetry and resolving incidents on production infrastructure.

Your capabilities:
1. Analyze system metrics (CPU, memory, disk, network, processes)
2. Identify root causes of performance issues
3. Execute targeted remediation actions when safe to do so
4. Escalate complex issues that require human intervention

Decision framework:
- CPU > 90% for sustained periods: investigate top processes, consider killing runaway processes
- Memory > 90%: check for memory leaks, consider restarting services if safe
- Disk > 85%: check for large files, clear /tmp if needed
- Network errors > 100/min: investigate connectivity, check specific interfaces

Auto-remediation rules:
- ALWAYS run diagnostics first before taking action
- Only kill processes with PID > 100 (avoid system processes)
- Only restart services in the allowlist
- If unsure, escalate rather than act
- Document every action taken

Output structure:
1. Summary of issue
2. Root cause analysis
3. Actions taken (with results)
4. Recommendations
5. Escalation needed: yes/no and reason
"""

ANALYSIS_PROMPT = ChatPromptTemplate.from_messages([
    SystemMessage(content=SYSTEM_PROMPT),
    HumanMessage(content="{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])


class MonitoringAgent:
    """LangChain-powered agent for incident analysis and auto-remediation."""

    def __init__(self, enable_remediation: bool = True):
        self.llm = _get_llm()
        tools = ALL_TOOLS if enable_remediation else DIAGNOSTIC_TOOLS
        self.agent = create_tool_calling_agent(self.llm, tools, ANALYSIS_PROMPT)
        self.executor = AgentExecutor(
            agent=self.agent,
            tools=tools,
            verbose=True,
            max_iterations=10,
            handle_parsing_errors=True,
        )
        self.enable_remediation = enable_remediation

    async def analyze_alert(
        self,
        alert_id: str,
        endpoint_id: str,
        alert_type: str,
        metrics: Dict[str, Any],
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze an alert with AI, run diagnostics, and attempt remediation.
        Returns structured analysis result.
        """
        prompt = self._build_analysis_prompt(
            alert_id=alert_id,
            endpoint_id=endpoint_id,
            alert_type=alert_type,
            metrics=metrics,
            context=context,
        )

        logger.info("agent_analysis_start", alert_id=alert_id, endpoint_id=endpoint_id)
        start_time = datetime.utcnow()

        try:
            result = await self.executor.ainvoke({"input": prompt})
            output = result.get("output", "")

            analysis_result = {
                "alert_id": alert_id,
                "endpoint_id": endpoint_id,
                "analysis": output,
                "root_cause": self._extract_root_cause(output),
                "escalation_needed": self._check_escalation(output),
                "actions_taken": self._extract_actions(result),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "analyzed_at": datetime.utcnow().isoformat(),
            }

            logger.info(
                "agent_analysis_complete",
                alert_id=alert_id,
                escalation=analysis_result["escalation_needed"],
                duration=analysis_result["duration_seconds"],
            )
            return analysis_result

        except Exception as e:
            logger.error("agent_analysis_failed", alert_id=alert_id, error=str(e))
            return {
                "alert_id": alert_id,
                "analysis": f"Agent analysis failed: {str(e)}",
                "root_cause": "Analysis unavailable",
                "escalation_needed": True,
                "actions_taken": [],
                "error": str(e),
            }

    async def analyze_anomaly(
        self,
        endpoint_id: str,
        current_metrics: Dict[str, Any],
        baseline_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Analyze metrics for anomalies compared to baseline."""
        prompt = f"""Analyze the following system metrics for endpoint '{endpoint_id}' and identify any anomalies.

Current Metrics:
{json.dumps(current_metrics, indent=2)}

{f'Baseline (historical average):{chr(10)}{json.dumps(baseline_metrics, indent=2)}' if baseline_metrics else ''}

Please:
1. Identify any metrics that are concerning or anomalous
2. Run diagnostics to investigate
3. Determine if any immediate action is needed
4. Provide a severity assessment (normal/warning/critical)
"""
        try:
            result = await self.executor.ainvoke({"input": prompt})
            return {
                "endpoint_id": endpoint_id,
                "analysis": result.get("output", ""),
                "is_anomaly": self._check_anomaly(result.get("output", "")),
                "severity": self._extract_severity(result.get("output", "")),
                "analyzed_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error("anomaly_analysis_failed", error=str(e))
            return {"endpoint_id": endpoint_id, "error": str(e)}

    def _build_analysis_prompt(
        self,
        alert_id: str,
        endpoint_id: str,
        alert_type: str,
        metrics: Dict[str, Any],
        context: Optional[str],
    ) -> str:
        return f"""INCIDENT ALERT — Requires immediate investigation and potential remediation.

Alert ID: {alert_id}
Endpoint: {endpoint_id}
Alert Type: {alert_type}
Timestamp: {datetime.utcnow().isoformat()}
Auto-Remediation: {"ENABLED" if self.enable_remediation else "DISABLED — analysis only"}

Triggering Metrics:
{json.dumps(metrics, indent=2)}

{f'Additional Context: {context}' if context else ''}

Please investigate this alert:
1. Run diagnostics to understand the current system state
2. Identify the root cause
3. {"Execute appropriate remediation actions if safe" if self.enable_remediation else "Provide remediation recommendations"}
4. Summarize findings and whether escalation to human admin is needed
"""

    def _extract_root_cause(self, analysis: str) -> str:
        lines = analysis.split("\n")
        for i, line in enumerate(lines):
            if "root cause" in line.lower():
                subsequent = lines[i:i+3]
                return " ".join(l.strip() for l in subsequent if l.strip())
        return "See full analysis"

    def _check_escalation(self, analysis: str) -> bool:
        escalation_keywords = ["escalat", "human intervention", "manual", "cannot resolve", "requires admin"]
        return any(kw in analysis.lower() for kw in escalation_keywords)

    def _check_anomaly(self, analysis: str) -> bool:
        anomaly_keywords = ["anomaly", "unusual", "spike", "abnormal", "concerning", "critical", "warning"]
        return any(kw in analysis.lower() for kw in anomaly_keywords)

    def _extract_severity(self, analysis: str) -> str:
        if any(w in analysis.lower() for w in ["critical", "immediate", "urgent"]):
            return "critical"
        if any(w in analysis.lower() for w in ["warning", "elevated", "high"]):
            return "high"
        if any(w in analysis.lower() for w in ["normal", "healthy", "no issues"]):
            return "normal"
        return "medium"

    def _extract_actions(self, result: dict) -> List[dict]:
        """Parse intermediate steps to extract tool calls made."""
        actions = []
        steps = result.get("intermediate_steps", [])
        for step in steps:
            if isinstance(step, tuple) and len(step) == 2:
                action, observation = step
                actions.append({
                    "tool": getattr(action, "tool", "unknown"),
                    "input": getattr(action, "tool_input", {}),
                    "result": str(observation)[:500],
                })
        return actions


# Singleton instance
_agent_instance: Optional[MonitoringAgent] = None


def get_monitoring_agent() -> MonitoringAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = MonitoringAgent(
            enable_remediation=settings.auto_remediation_enabled
        )
    return _agent_instance