import json
import os
from datetime import datetime, timedelta
from typing import Literal

from dotenv import load_dotenv

load_dotenv()


def save_data(data_to_save: dict | list, filename: str):
    os.makedirs(os.environ["DATA_PATH"], exist_ok=True)

    with open(f"{os.environ['DATA_PATH']}/{filename}.json", "w") as f:
        json.dump(data_to_save, f, indent=4)


def date_str_to_posix(
    date_str: str, buffer: Literal["early", "late"] | None = None
) -> float:
    """
    Convert a date string to a POSIX timestamp

    date_str, str: date string in ISO format (YYYY-MM-DD)

    returns: POSIX timestamp
    """

    dt = datetime.strptime(f"{date_str}T00:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z")

    match buffer:
        case "early":
            return (dt - timedelta(hours=1)).timestamp()
        case "late":
            return (dt + timedelta(hours=1)).timestamp()
        case _:
            return dt.timestamp()
