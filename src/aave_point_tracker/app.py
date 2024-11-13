from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pathlib import Path
import json
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


class TVLResponse(BaseModel):
    user_id: str
    tvl: float


@app.get("/tvl/{user_id}")
async def get_tvl(user_id: str):
    user_tvls = get_tvl_values()
    try:
        tvl = user_tvls[user_id]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No data for user: {user_id}")
    return TVLResponse(user_id=user_id, tvl=tvl)


def get_tvl_values() -> dict[str, str]:
    with open(Path(os.environ["DATA_PATH"], "calculated", "user_tvls.json"), "r") as f:
        user_tvls = json.load(f)
    return user_tvls
