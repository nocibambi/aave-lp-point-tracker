import json
import os
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv

load_dotenv()


class DataLayer(Enum):
    raw = auto()
    prepared = auto()


vars(DataLayer)

DataLayer._member_names_


def load_configs() -> dict:
    return json.loads(files("aave_point_tracker").joinpath("config.json").read_text())


def save_data(data_to_save: dict | list, filename: str, data_layer: str) -> None:
    """
    Save data to a JSON file

    data_to_save, dict | list: data to save
    filename, str: filename

    returns: None
    """
    if data_layer not in DataLayer._member_names_:
        raise ValueError(
            f"Data layer must be one of {DataLayer._member_names_}, got {data_layer}"
        )
    data_path = Path(os.environ["DATA_PATH"], data_layer)
    os.makedirs(data_path, exist_ok=True)
    with open(os.path.join(data_path, f"{filename}.json"), "w") as f:
        json.dump(data_to_save, f, indent=4)


def load_data(filename: str, data_layer: str) -> Any:
    if data_layer not in DataLayer._member_names_:
        raise ValueError(
            f"Data layer must be one of {DataLayer._member_names_}, got {data_layer}"
        )
    with open(Path(os.environ["DATA_PATH"], data_layer, f"{filename}.json"), "r") as f:
        return json.load(f)


def date_str_to_datetime(date_str: str) -> datetime:
    return datetime.strptime(f"{date_str}T00:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z")


def datetime_to_posix(
    dt: datetime, buffer: Literal["early", "late"] | None = None
) -> int:
    assert dt.tzinfo == timezone.utc, f"Timezone must be UTC: {dt}, {dt.tzinfo}"
    match buffer:
        case "early":
            return int((dt - timedelta(hours=1)).timestamp())
        case "late":
            return int((dt + timedelta(hours=1)).timestamp())
        case _:
            return int(dt.timestamp())
