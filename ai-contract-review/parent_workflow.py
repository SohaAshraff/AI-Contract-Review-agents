import asyncio
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    import json_repair

    from activities import callLLMInput, call_llm
    from child_workflow import PDFSummaryWorkflow, PDFSummaryWorkflowInput
    from prompt import _REVISION_PROMPT, _SYNTHESIS_PROMPT

MAX_ALLOWED_REVISIONS = 5


@dataclass
class ContractReviewWorkflowOutput:
    reports: dict
    sources: list[str] = field(default_factory=list)
    approved_by: str = ""


@dataclass
class ContractReviewWorkflowInput:
    s3_paths: list[str]
    max_revisions: int = 2


DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4,
)


def _parse_report(raw: str) -> dict:
    """Parse LLM output into a dict; wrap anything unexpected so the output type stays stable."""
    parsed: Any = json_repair.loads(raw)
    if isinstance(parsed, dict):
        return parsed
    return {"raw_response": parsed}


@workflow.defn
class ContractReviewWorkflow:

    def __init__(self) -> None:
        self._status: str = "processing"
        self._summaries: list = []
        self._reports: dict = {}

        self._review_decision: Optional[str] = None
        self._review_feedback: str = ""
        self._approved_by: str = ""

    @workflow.query
    def get_status(self) -> dict:
        return {
            "status": self._status,
            "pdfs_processed": len(self._summaries),
            "report_preview": json.dumps(self._reports, ensure_ascii=False)[:500],
            "approved_by": self._approved_by,
        }

    @workflow.query
    def get_report(self) -> dict:
        return {
            "status": self._status,
            "report": self._reports,
            "approved_by": self._approved_by,
            "sources": [s["s3_path"] for s in self._summaries],
        }

    @workflow.signal
    async def assign_reviewer(self, name: str) -> None:
        self._approved_by = name.strip()[:100]

    @workflow.update
    async def submit_decision(self, decision: str, feedback: str = "") -> str:
        self._review_decision = decision
        self._review_feedback = feedback
        return f"Decision '{decision}' recorded."

    @submit_decision.validator
    def validate_decision(self, decision: str, feedback: str = "") -> None:
        if self._status != "awaiting-review":
            raise ValueError(f"Workflow is not awaiting review (status: '{self._status}').")
        if decision not in ("approve", "revise"):
            raise ValueError(f"Must be 'approve' or 'revise', got: '{decision}'")
        if decision == "revise" and not feedback.strip():
            raise ValueError("Feedback is required when requesting a revision.")

    @workflow.run
    async def run(self, params: ContractReviewWorkflowInput) -> ContractReviewWorkflowOutput:
        # Validate input
        if not params.s3_paths:
            raise ApplicationError("s3_paths must not be empty", non_retryable=True)
        bad_paths = [p for p in params.s3_paths if not isinstance(p, str) or not p.startswith("s3://")]
        if bad_paths:
            raise ApplicationError(f"Invalid S3 paths (must start with s3://): {bad_paths}", non_retryable=True)
        max_revisions = max(0, min(params.max_revisions, MAX_ALLOWED_REVISIONS))

        # Fan out: one child workflow per contract
        self._status = "extraction"
        workflow.logger.info(f"Fanning out to {len(params.s3_paths)} child workflows")

        workflow_id = workflow.info().workflow_id
        task_queue = workflow.info().task_queue
        handles = await asyncio.gather(
            *[
                workflow.start_child_workflow(
                    PDFSummaryWorkflow.run,
                    PDFSummaryWorkflowInput(s3_path=s3_path),
                    id=f"{workflow_id}-pdf-{idx + 1}",
                    task_queue=task_queue,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
                for idx, s3_path in enumerate(params.s3_paths)
            ]
        )
        raw_results = await asyncio.gather(*handles, return_exceptions=True)

        for i, res in enumerate(raw_results):
            if isinstance(res, BaseException):
                workflow.logger.warning(f"PDF {i + 1} failed: {res}")
            else:
                self._summaries.append({
                    "s3_path": res.s3_path,
                    "summary": res.summary,
                    "keyrisks": res.keyrisks,
                })

        if not self._summaries:
            raise ApplicationError("All PDFs failed", non_retryable=True)

        # Synthesize the summaries into one report
        self._status = "analyzing"
        workflow.logger.info(f"Synthesizing {len(self._summaries)} summaries")

        combined_summary = "\n\n".join(
            f"**Contract {i + 1}** (`{s['s3_path']}`):\n"
            f"Summary: {s['summary']}\n"
            f"Risks: {s['keyrisks']}"
            for i, s in enumerate(self._summaries)
        )
        llm_result = await workflow.execute_activity(
            call_llm,
            callLLMInput(prompt=_SYNTHESIS_PROMPT.format(summaries=combined_summary, n=len(self._summaries))),
            start_to_close_timeout=timedelta(minutes=3),
            heartbeat_timeout=timedelta(seconds=180),
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        self._reports = _parse_report(llm_result.response)

        # Human in the loop: up to max_revisions revisions, then a final approval round
        for revision_no in range(max_revisions + 1):
            self._status = "awaiting-review"
            workflow.logger.info(f"Waiting for human review (cycle {revision_no})")
            self._review_decision = None

            try:
                await workflow.wait_condition(
                    lambda: self._review_decision is not None,
                    timeout=timedelta(days=3),
                )
            except asyncio.TimeoutError:
                workflow.logger.warning("Review timed out after 3 days - auto-completing")
                break

            if self._review_decision == "approve":
                workflow.logger.info(f"Approved by: {self._approved_by or 'unassigned reviewer'}")
                break

            if revision_no == max_revisions:
                workflow.logger.info("Maximum revisions reached - completing with current report")
                break

            self._status = "revising"
            workflow.logger.info("Revising report based on reviewer feedback")

            revised = await workflow.execute_activity(
                call_llm,
                callLLMInput(prompt=_REVISION_PROMPT.format(
                    report=json.dumps(self._reports, ensure_ascii=False, indent=2),
                    feedback=self._review_feedback,
                )),
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(seconds=180),
                retry_policy=DEFAULT_RETRY_POLICY,
            )
            self._reports = _parse_report(revised.response)

        self._status = "completed"
        return ContractReviewWorkflowOutput(
            reports=self._reports,
            sources=[s["s3_path"] for s in self._summaries],
            approved_by=self._approved_by,
        )
