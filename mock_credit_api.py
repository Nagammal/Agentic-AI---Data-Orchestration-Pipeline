#Mock credit-score service (FastAPI). Includes both single & batch endpoints and occasional simulated failure to test retries.
# mock_credit_api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from random import randint, choice, random
from datetime import datetime
from typing import List
import uvicorn

app = FastAPI()

# Use in-memory store of credit scores (for deterministic-ish behavior)
SCORES = {}

def _generate_score_for(customer_id: str):
    # deterministic-ish seed could be added; for now random
    return {
        "customer_id": customer_id,
        "credit_score": randint(300, 850),
        "credit_score_provider": choice(["Equifax", "Experian", "TransUnion"]),
        "last_updated": datetime.utcnow().isoformat()
    }

class BatchRequest(BaseModel):
    customer_ids: List[str]

@app.get("/credit-score/{customer_id}")
async def get_credit_score(customer_id: str):
    # simulate intermittent failure (10% chance) to test retry behavior
    if random() < 0.10:
        raise HTTPException(status_code=500, detail="Simulated transient error")
    if customer_id not in SCORES:
        SCORES[customer_id] = _generate_score_for(customer_id)
    return SCORES[customer_id]

@app.post("/credit-scores")
async def get_credit_scores(req: BatchRequest):
    # simulate intermittent failure (10% chance)
    if random() < 0.10:
        raise HTTPException(status_code=500, detail="Simulated transient error")
    out = {}
    for cid in req.customer_ids:
        if cid not in SCORES:
            SCORES[cid] = _generate_score_for(cid)
        out[cid] = SCORES[cid]
    return out

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
