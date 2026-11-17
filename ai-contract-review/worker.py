import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from activities import call_llm, extract_pdf
from child_workflow import PDFSummaryWorkflow
from parent_workflow import ContractReviewWorkflow

load_dotenv()

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_CONTRACT_TASK_QUEUE")


async def main() -> None:
    if not TEMPORAL_HOST or not TEMPORAL_TASK_QUEUE:
        raise SystemExit("TEMPORAL_HOST and TEMPORAL_CONTRACT_TASK_QUEUE must be set (see .env.example)")

    temporal_client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)

    # Thread pool is required for the sync extract_pdf activity.
    with ThreadPoolExecutor(max_workers=10) as activity_executor:
        worker = Worker(
            temporal_client,
            task_queue=TEMPORAL_TASK_QUEUE,
            workflows=[ContractReviewWorkflow, PDFSummaryWorkflow],
            activities=[extract_pdf, call_llm],
            activity_executor=activity_executor,
        )
        print(f"Worker running on task queue: {TEMPORAL_TASK_QUEUE}")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
