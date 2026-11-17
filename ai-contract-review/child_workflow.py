from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import json_repair

    from activities import ExtractPdfInput, callLLMInput, call_llm, extract_pdf
    from prompt import _SUMMARY_PROMPT


@dataclass
class PDFSummaryWorkflowInput:
    s3_path: str


@dataclass
class PDFSummaryWorkflowOutput:
    s3_path: str
    summary: str
    keyrisks: str


DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
)


@workflow.defn
class PDFSummaryWorkflow:

    @workflow.run
    async def run(self, params: PDFSummaryWorkflowInput) -> PDFSummaryWorkflowOutput:
        # Extract the PDF from S3 as Markdown
        extracted = await workflow.execute_activity(
            extract_pdf,
            ExtractPdfInput(s3_path=params.s3_path),
            start_to_close_timeout=timedelta(minutes=20),
            retry_policy=DEFAULT_RETRY_POLICY,
            heartbeat_timeout=timedelta(seconds=30),
        )

        # Summarize the contract and list key risks
        summary_response = await workflow.execute_activity(
            call_llm,
            callLLMInput(prompt=_SUMMARY_PROMPT.format(text=extracted.markdown_text)),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY_POLICY,
            heartbeat_timeout=timedelta(seconds=180),
        )
        parsed = json_repair.loads(summary_response.response)
        if not isinstance(parsed, dict):
            parsed = {"summary": str(parsed), "key_risks": ""}

        return PDFSummaryWorkflowOutput(
            s3_path=params.s3_path,
            summary=str(parsed.get("summary", "")),
            keyrisks=str(parsed.get("key_risks", "")),
        )
