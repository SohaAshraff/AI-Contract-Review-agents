import os
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from temporalio.client import Client
from temporalio.client import WorkflowExecutionStatus as WES
from temporalio.service import RPCError

load_dotenv()

# Configuration comes from environment variables / .env (see .env.example).
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_CONTRACT_TASK_QUEUE = os.getenv("TEMPORAL_CONTRACT_TASK_QUEUE")

app = FastAPI(
    title="AI Contract Review API",
    description="Start AI contract reviews, track progress, and approve or request revisions (Temporal workflows).",
    version="1.0.0",
)


class StartReviewRequest(BaseModel):
    s3_paths: list[str] = Field(min_length=1, max_length=50)
    max_revisions: int = Field(default=2, ge=0, le=5)


class AssignRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ReviseRequest(BaseModel):
    feedback: str = Field(min_length=1, max_length=5000)


_temporal_client: Client | None = None


async def get_temporal_client() -> Client:
    """Connect once and reuse the client across requests."""
    global _temporal_client
    if _temporal_client is None:
        if not TEMPORAL_HOST:
            raise HTTPException(status_code=500, detail="TEMPORAL_HOST is not configured")
        _temporal_client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)
    return _temporal_client


async def get_handle(workflow_id: str):
    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        desc = await handle.describe()
    except RPCError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found")
    return handle, desc


async def run_decision(workflow_id: str, decision: str, feedback: str = "") -> dict:
    handle, _ = await get_handle(workflow_id)
    try:
        result = await handle.execute_update("submit_decision", args=[decision, feedback])
    except Exception as e:
        # Raised when the workflow's update validator rejects the decision
        raise HTTPException(status_code=409, detail=str(e))
    return {"ok": True, "message": result}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/contract-review/start")
async def start_contract_review(request: StartReviewRequest):
    bad_paths = [p for p in request.s3_paths if not p.startswith("s3://") or not p.lower().endswith(".pdf")]
    if bad_paths:
        raise HTTPException(status_code=422, detail=f"Paths must look like s3://bucket/file.pdf: {bad_paths}")
    if not TEMPORAL_CONTRACT_TASK_QUEUE:
        raise HTTPException(status_code=500, detail="TEMPORAL_CONTRACT_TASK_QUEUE is not configured")

    workflow_id = f"contract-review-{uuid.uuid4()}"
    client = await get_temporal_client()
    await client.start_workflow(
        "ContractReviewWorkflow",
        args=[{
            "s3_paths": request.s3_paths,
            "max_revisions": request.max_revisions,
        }],
        id=workflow_id,
        task_queue=TEMPORAL_CONTRACT_TASK_QUEUE,
    )
    return {"workflow_id": workflow_id}


@app.get("/contract-review/{workflow_id}/status")
async def get_review_status(workflow_id: str):
    """Temporal execution status + brief workflow state (Query)."""
    handle, desc = await get_handle(workflow_id)

    workflow_state = None
    if desc.status == WES.RUNNING:
        try:
            workflow_state = await handle.query("get_status", result_type=dict)
        except Exception as e:
            workflow_state = {"error": str(e)}

    return {
        "workflow_id": workflow_id,
        "execution_status": desc.status.name,
        "workflow_state": workflow_state,
    }


@app.get("/contract-review/{workflow_id}/report")
async def get_review_report(workflow_id: str):
    """Live report while running (Query), or the final result once completed."""
    handle, desc = await get_handle(workflow_id)

    workflow_report = None
    if desc.status == WES.RUNNING:
        try:
            workflow_report = await handle.query("get_report", result_type=dict)
        except Exception as e:
            workflow_report = {"error": str(e)}
    elif desc.status == WES.COMPLETED:
        workflow_report = await handle.result(result_type=dict)

    return {
        "workflow_id": workflow_id,
        "execution_status": desc.status.name,
        "workflow_report": workflow_report,
    }


@app.post("/contract-review/{workflow_id}/assign")
async def assign_reviewer(workflow_id: str, request: AssignRequest):
    handle, _ = await get_handle(workflow_id)
    await handle.signal("assign_reviewer", request.name)
    return {"status": "ok", "message": f"Reviewer '{request.name}' assigned."}


@app.post("/contract-review/{workflow_id}/revise")
async def submit_revise(workflow_id: str, request: ReviseRequest):
    return await run_decision(workflow_id, "revise", request.feedback)


@app.post("/contract-review/{workflow_id}/approve")
async def submit_approve(workflow_id: str):
    return await run_decision(workflow_id, "approve")
