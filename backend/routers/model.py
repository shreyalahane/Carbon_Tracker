"""Model performance endpoints backed by MLflow."""

import os
from pathlib import Path
from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/model", tags=["model"])

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / "myfile.env")

MODEL_NAME = "CarbonFootprintPredictor"


def _metrics_from_run(client, run_id):
    run = client.get_run(run_id)
    metrics = dict(run.data.metrics)
    params = dict(run.data.params)
    return {
        "run_id": run_id,
        "status": run.info.status,
        "metrics": metrics,
        "params": params,
    }


@router.get("/performance")
def model_performance():
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient(
            tracking_uri=os.getenv("MLFLOW_TRACKING_URI",
                                   "http://localhost:5000"))
        versions = client.get_latest_versions(MODEL_NAME)
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"MLflow unavailable: {e}")

    if not versions:
        raise HTTPException(status_code=404,
                            detail="Model not registered in MLflow yet")

    version = max(versions, key=lambda v: v.creation_timestamp or 0)

    try:
        info = _metrics_from_run(client, version.run_id)
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Could not read run metrics: {e}")

    # Filter to the metrics that matter for the dashboard
    wanted = ["MAE", "RMSE", "R2", "Train_Size", "Test_Size"]
    performance = {k: info["metrics"].get(k)
                   for k in wanted if k in info["metrics"]}
    shap_keys = sorted(k for k in info["metrics"] if k.startswith("shap_"))
    importance_keys = sorted(k for k in info["metrics"]
                             if k.startswith("importance_"))
    source = shap_keys if shap_keys else importance_keys
    top_features = [
        {"feature": k.replace("shap_", "").replace("importance_", ""),
         "importance": info["metrics"][k]}
        for k in source
    ]
    top_features.sort(key=lambda f: f["importance"], reverse=True)
    top_features = top_features[:5]

    return {
        "model_name": MODEL_NAME,
        "version": version.version,
        "stage": version.current_stage,
        "performance": performance,
        "top_features": top_features,
        "run_id": info["run_id"],
    }
