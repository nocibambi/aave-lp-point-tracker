import json
import os
from datetime import datetime, timedelta
from typing import Literal
from importlib.resources import files
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def load_configs() -> dict:
    return json.loads(files("aave_point_tracker").joinpath("config.json").read_text())


def save_data(data_to_save: dict | list, filename: str) -> None:
    """
    Save data to a JSON file

    data_to_save, dict | list: data to save
    filename, str: filename

    returns: None
    """
    os.makedirs(os.environ["DATA_PATH"], exist_ok=True)

    with open(os.path.join(os.environ["DATA_PATH"], f"{filename}.json"), "w") as f:
        json.dump(data_to_save, f, indent=4)


def load_data(filename: str) -> dict | list:
    with open(Path(os.environ["DATA_PATH"], f"{filename}.json"), "r") as f:
        return json.load(f)


def date_str_to_datetime(date_str: str) -> datetime:
    return datetime.strptime(f"{date_str}T00:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z")


def datetime_to_posix(
    dt: datetime, buffer: Literal["early", "late"] | None = None
) -> int:
    match buffer:
        case "early":
            return int((dt - timedelta(hours=1)).timestamp())
        case "late":
            return int((dt + timedelta(hours=1)).timestamp())
        case _:
            return int(dt.timestamp())
