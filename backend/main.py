"""FastAPI application entry point.

Run with:
    uvicorn backend.main:app --reload --port 8000

Auto-documented at http://localhost:8000/docs
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import agents, emissions, model, predictions

app = FastAPI(
    title="Carbon Tracker API",
    description="Serves emissions, predictions, AI agent outputs and "
                "model performance for the Carbon Tracker dashboard.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(emissions.router)
app.include_router(predictions.router)
app.include_router(agents.router)
app.include_router(model.router)


@app.get("/health", tags=["system"])
def health():
    return {
        "status": "ok",
        "service": "carbon-tracker-api",
        "version": app.version,
    }
