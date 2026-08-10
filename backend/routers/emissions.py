"""Emission endpoints: today's summary and historical data."""

from fastapi import APIRouter, HTTPException

from ..services import mysql_service as mysql

router = APIRouter(prefix="/emissions", tags=["emissions"])


@router.get("/today")
def emissions_today():
    try:
        today = mysql.emissions_today()
        yesterday = mysql.emissions_yesterday()
        if not today:
            raise HTTPException(status_code=404,
                                detail="No emission data available yet")
        response = {
            "today": today,
            "yesterday": yesterday,
            "change_pct": None,
        }
        if yesterday and yesterday.get("total_co2_kg"):
            today_val = today.get("total_co2_kg") or 0
            yesterday_val = yesterday["total_co2_kg"]
            response["change_pct"] = round(
                ((today_val - yesterday_val) / yesterday_val) * 100, 2)
        return response
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@router.get("/history")
def emissions_history(days: int = 30):
    try:
        rows = mysql.emissions_history(min(days, 365))
        if not rows:
            raise HTTPException(status_code=404,
                                detail="No emission history available")
        return {"days": days, "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
