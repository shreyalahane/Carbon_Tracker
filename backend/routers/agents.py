"""AI agent output endpoints: advice, plan, and ESG report.

Also serves the generated ESG report PDF for download.
"""

import os
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..services import mongo_service as mongo

router = APIRouter(prefix="/agents", tags=["agents"])

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def _latest_or_404(collection):
    doc = mongo.latest_doc(collection)
    if not doc:
        raise HTTPException(status_code=404,
                            detail=f"No data in collection '{collection}'")
    return doc


@router.get("/advice")
def agent_advice():
    try:
        return _latest_or_404("advisor_advice")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")


@router.get("/plan")
def agent_plan():
    try:
        return _latest_or_404("weekly_plans")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")


@router.get("/report")
def agent_report():
    try:
        return _latest_or_404("esg_reports")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")


@router.get("/report/pdf")
def agent_report_pdf():
    doc = mongo.latest_doc("esg_reports")
    if not doc or not doc.get("pdf_path"):
        raise HTTPException(status_code=404,
                            detail="No report PDF available yet")
    pdf_path = Path(doc["pdf_path"])
    if not pdf_path.exists():
        # fall back to the reports directory next to the project
        pdf_path = BASE_DIR / "reports" / doc.get("pdf_file", "")
    if not pdf_path.exists():
        raise HTTPException(status_code=404,
                            detail="Report PDF file not found on disk")
    return FileResponse(
        path=str(pdf_path),
        media_type="application/pdf",
        filename=pdf_path.name)
