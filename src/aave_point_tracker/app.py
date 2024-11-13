import json
import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

app = FastAPI()


class TVLResponse(BaseModel):
    user_id: str
    tvl: float


@app.get("/tvl/{user_id}")
async def get_tvl(user_id: str):
    """
    Get the total value locked (TVL) for the given user, in USD.

    Args:
        user_id: The user's Ethereum address.

    Returns:
        A TVLResponse object containing the user's ID and TVL, or raises a 404 error if the user is not found.

    """
    user_tvls = get_tvl_values()
    try:
        tvl = user_tvls[user_id]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No data for user: {user_id}")
    return TVLResponse(user_id=user_id, tvl=tvl)


def get_tvl_values() -> dict[str, float]:
    """
    Load and return the total value locked (TVL) for users.

    Returns:
        A dictionary mapping user IDs to their respective TVL in USD.
    """
    with open(Path(os.environ["DATA_PATH"], "calculated", "user_tvls.json"), "r") as f:
        user_tvls = json.load(f)
    return user_tvls
