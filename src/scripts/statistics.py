from __future__ import annotations

import logging
import pathlib

from sqlalchemy import extract, func
from sqlalchemy.orm import Session

try:
    from src.db.db_config import db_obj
    from src.db.models import WeatherData, WeatherStats
except ModuleNotFoundError:
    import sys

    sys.path.append(str(pathlib.Path(__file__).parents[1]))
    from db.db_config import db_obj
    from db.models import WeatherData, WeatherStats

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    years = get_all_years(db_obj)
    stations = get_stations(db_obj)

    insert_count, update_count = calculate_stats(db_obj, years, stations)

    logger.info(
        "Process completed. Inserted: {} records. Updated: {} records".format(
            insert_count, update_count
        )
    )


def calculate_stats(db_obj, years, stations):
    insert_count = 0
    update_count = 0

    for year in years:
        for station in stations:
            logger.info(
                "Station {} - Year {}: calculating stats".format(
                    station, year
                )
            )

            (
                avg_max_temp,
                avg_min_temp,
                precipitation_amt,
            ) = calculate(db_obj, year, station)

            updated = save_stats(
                db_obj,
                year,
                station,
                avg_max_temp,
                avg_min_temp,
                precipitation_amt,
            )

            if updated:
                update_count += 1
            else:
                insert_count += 1

    return insert_count, update_count


def get_all_years(db_obj: Session):
    return sorted(
        [
            item[0]
            for item in db_obj.query(
            extract("year", WeatherData.date).distinct().label("year")
        ).all()
        ]
    )


def get_stations(db_obj):
    return sorted(
        [item[0] for item in
         db_obj.query(WeatherData.station).distinct().all()]
    )


def calculate(db_obj, year, station):
    query = db_obj.query(WeatherData).filter(
        extract("year", WeatherData.date) == year,
        WeatherData.station == station,
    )

    avg_max_temp = (
        query.filter(WeatherData.max_temp.isnot(None))
        .with_entities(func.avg(WeatherData.max_temp))
        .scalar()
    )
    avg_min_temp = (
        query.filter(WeatherData.min_temp.isnot(None))
        .with_entities(func.avg(WeatherData.min_temp))
        .scalar()
    )
    precipitation_amt = (
        query.filter(WeatherData.precipitation_amt.isnot(None))
        .with_entities(func.sum(WeatherData.precipitation_amt))
        .scalar()
    )

    if precipitation_amt:
        precipitation_amt = precipitation_amt / 10

    return avg_max_temp, avg_min_temp, precipitation_amt


def save_stats(
        db_obj: Session,
        year: int,
        station: str,
        avg_max_temp: float | None,
        avg_min_temp: float | None,
        precipitation_amt: float | None,
):
    updated = False

    statistic = (
        db_obj.query(WeatherStats).filter_by(year=year,
                                             station=station).first()
    )

    if statistic:
        updated = True
        statistic.avg_max_temp = avg_max_temp
        statistic.avg_min_temp = avg_min_temp
        statistic.precipitation_amt = precipitation_amt
    else:
        db_obj.add(
            WeatherStats(
                year=year,
                station=station,
                avg_max_temp=avg_max_temp,
                avg_min_temp=avg_min_temp,
                precipitation_amt=precipitation_amt,
            )
        )

    db_obj.commit()

    return updated


main()
