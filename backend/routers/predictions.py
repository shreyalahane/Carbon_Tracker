"""Prediction endpoints: tomorrow's forecast and weekly outlook."""

from fastapi import APIRouter, HTTPException

from ..services import mysql_service as mysql

router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/tomorrow")
def prediction_tomorrow():
    try:
        row = mysql.prediction_tomorrow()
        if not row:
            raise HTTPException(status_code=404,
                                detail="No prediction available yet")
        return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@router.get("/weekly")
def predictions_weekly(days: int = 7):
    try:
        rows = mysql.predictions_weekly(min(days, 90))
        if not rows:
            raise HTTPException(status_code=404,
                                detail="No predictions available yet")
        return {"days": days, "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
