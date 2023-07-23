import json
import pathlib
from datetime import datetime

from flask import Flask

from utils import AlchemyEncoder

try:
    from src.db.db_config import db_obj
    from src.db.models import WeatherData, WeatherStats
except ModuleNotFoundError:
    import sys

    sys.path.append(str(pathlib.Path(__file__).parents[1]))
    from db.db_config import db_obj
    from db.models import WeatherData, WeatherStats

app = Flask(__name__)


@app.route('/api/weather')
@app.route('/api/weather/<int:page>/<string:date_filter>/<string'
           ':station_filter>')
def weather_data(page=1, date_filter=None, station_filter=None):
    query = db_obj.query(WeatherData)

    if date_filter != "None" and date_filter is not None:
        date_filter = datetime.strptime(date_filter, '%Y-%m-%d').date()
        query = query.filter(WeatherData.date == date_filter)
    if station_filter != "None" and station_filter is not None:
        query = query.filter(WeatherData.station == station_filter)

    records = json.dumps(query.limit(10).offset(page * 10).all(),
                         cls=AlchemyEncoder)
    return {"result": json.loads(records)}


@app.route('/api/stats')
@app.route('/api/stats/<int:page>')
def weather_stats(page=1):
    query = db_obj.query(WeatherStats)
    records = json.dumps(query.limit(10).offset(page * 10).all(),
                         cls=AlchemyEncoder)
    return {"result": json.loads(records)}


if __name__ == '__main__':
    app.run(debug=True)
