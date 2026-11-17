import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import boto3
import fitz
import pymupdf4llm
from dotenv import load_dotenv
from openai import AsyncOpenAI
from temporalio import activity
from temporalio.exceptions import ApplicationError

load_dotenv()

# All credentials come from environment variables / .env (never hardcode them).
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


@dataclass
class ExtractPdfInput:
    s3_path: str
    batch_size: int = 2  # Number of pages to convert per batch


@dataclass
class ExtractPdfOutput:
    s3_path: str
    markdown_text: str
    page_count: int


@dataclass
class callLLMInput:
    prompt: str


@dataclass
class callLLMOutput:
    response: str


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        endpoint_url=AWS_S3_ENDPOINT,
    )


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Split 's3://bucket/key.pdf' into (bucket, key), rejecting malformed paths."""
    if not isinstance(s3_path, str) or not s3_path.startswith("s3://"):
        raise ApplicationError(f"Invalid S3 path (must start with s3://): {s3_path}", non_retryable=True)
    bucket_name, _, key = s3_path[len("s3://"):].partition("/")
    if not bucket_name or not key:
        raise ApplicationError(f"Invalid S3 path (expected s3://bucket/key): {s3_path}", non_retryable=True)
    if not key.lower().endswith(".pdf"):
        raise ApplicationError(f"Only PDF files are supported: {s3_path}", non_retryable=True)
    return bucket_name, key


# Activity 1: download a PDF from S3 and convert it to Markdown.
# Sync activity (PyMuPDF is blocking) - runs on the worker's thread pool.
@activity.defn
def extract_pdf(params: ExtractPdfInput) -> ExtractPdfOutput:
    activity.logger.info(f"Starting PDF extraction for {params.s3_path}")
    bucket_name, key = parse_s3_path(params.s3_path)
    batch_size = max(1, params.batch_size)

    # A unique temp dir per call avoids clashes between PDFs with the same filename,
    # and is deleted automatically when done.
    with tempfile.TemporaryDirectory(prefix="contract-review-") as tmp_dir:
        local_pdf_path = str(Path(tmp_dir) / Path(key).name)
        get_s3_client().download_file(bucket_name, key, local_pdf_path)

        with fitz.open(local_pdf_path) as doc:
            page_count = doc.page_count
        activity.logger.info(f"PDF downloaded, total pages: {page_count}")

        chunks = []
        num_batches = math.ceil(page_count / batch_size)
        for batch_idx in range(num_batches):
            start_page = batch_idx * batch_size
            end_page = min(start_page + batch_size, page_count)
            chunks.append(pymupdf4llm.to_markdown(local_pdf_path, pages=list(range(start_page, end_page))))
            activity.heartbeat({
                "status": "extracting",
                "current_batch": batch_idx + 1,
                "pages_processed": end_page,
            })

    return ExtractPdfOutput(
        s3_path=params.s3_path,
        markdown_text="\n\n".join(chunks),
        page_count=page_count,
    )


# Activity 2: call the LLM via OpenRouter (OpenAI-compatible API).
@activity.defn
async def call_llm(params: callLLMInput) -> callLLMOutput:
    api_key = os.getenv("OPENROUTER_API_KEY")
    model = os.getenv("OPENROUTER_MODEL")
    if not api_key or not model:
        raise ApplicationError("OPENROUTER_API_KEY and OPENROUTER_MODEL must be set", non_retryable=True)

    # Log only the size - prompts contain contract text, which is sensitive.
    activity.logger.info(f"Calling LLM, prompt length: {len(params.prompt)} chars")
    activity.heartbeat({"status": "calling_llm", "prompt_length": len(params.prompt)})

    async with AsyncOpenAI(api_key=api_key, base_url=OPENROUTER_BASE_URL) as llm_client:
        response = await llm_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": params.prompt}],
            max_tokens=9000,
        )

    if not response.choices or not response.choices[0].message.content:
        raise ApplicationError("LLM returned an empty response")  # retryable

    llm_response = response.choices[0].message.content
    activity.logger.info(f"LLM response received, length: {len(llm_response)}")
    return callLLMOutput(response=llm_response)
