import logging
import os
import numpy as np
import pandas as pd

from pathlib import Path

try:
    from src.db.db_config import db_obj
    from src.db.models import WeatherData
except ModuleNotFoundError:
    import sys
    sys.path.append(str(Path(__file__).parents[1]))
    from db.db_config import db_obj
    from db.models import WeatherData

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).parents[2]


def get_files():
    files = []
    for r, d, f in os.walk(str(BASE_DIR) + '/wx_data'):
        for file in f:
            files.append(file)

    return sorted(files)


def read_file(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        sep="\t",
        header=None,
        names=["date", "max_temp", "min_temp", "precipitation_amt"],
    )

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

    df["max_temp"].replace(-9999, np.NaN, inplace=True)
    df["min_temp"].replace(-9999, np.NaN, inplace=True)
    df["precipitation_amt"].replace(-9999, np.NaN, inplace=True)

    df["max_temp"] = df["max_temp"] / 10
    df["min_temp"] = df["min_temp"] / 10
    df["precipitation_amt"] = df["precipitation_amt"] / 10

    return df


def ingest_records(df: pd.DataFrame, station_id: str) -> None:
    df["station"] = station_id

    try:
        db_obj.bulk_save_objects(
            [WeatherData(**row) for row in df.to_dict(orient="records")]
        )
        db_obj.commit()

        logger.info("Station {}: inserted {} records".format(station_id,
                                                             len(df)))
    except Exception as e:
        if "IntegrityError" in str(e):
            logger.warning(
                "Station {}: duplicate records found.".format(
                    station_id)
            )
        else:
            raise e


def main():
    files = get_files()
    if not files:
        logger.error("No files found in the folder.")
        sys.exit(1)

    for file in files:
        station, _ = os.path.splitext(file)
        logger.info("Station {}: starting records ingestion".format(
            station))

        df = read_file(os.path.join(str(BASE_DIR) + '/wx_data', file))
        ingest_records(df, station)


main()
