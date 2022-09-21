import sys
from pprint import pprint
from datetime import timedelta

import dateutil
import pandas as pd
import xmltodict

from entsoe import EntsoeRawClient


def yield_day_ahead_rates(api_key: str, country_code: str, start: pd.Timestamp, end: pd.Timestamp):
    client = EntsoeRawClient(api_key=api_key)
    xml_string = client.query_day_ahead_prices(country_code, start, end)
    doc = xmltodict.parse(xml_string)

    for timeseries in doc ['Publication_MarketDocument']['TimeSeries']:
        currency = timeseries['currency_Unit.name']
        timeseries['Period']['resolution']
        start_at = dateutil.parser.isoparse(timeseries['Period']['timeInterval']['start'])
        interval = timeseries['Period']['resolution']
        if interval != 'PT60M':
            continue
        for point in timeseries['Period']['Point']:
            hour_offset = int(point['position']) - 1
            current_timepoint = start_at + timedelta(hours=hour_offset)
            point['price.amount']
            price_kwh = float(point['price.amount']) / 1000
            yield { 
                'timepoint': current_timepoint, 
                'price': price_kwh,
                'currency': currency,
                'unit': 'kWh'
            }


def main():
    api_key = sys.argv[1]

    start = pd.Timestamp.now(tz='UTC').replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + pd.Timedelta(value=1, unit='day')

    for t in yield_day_ahead_rates(api_key=api_key, country_code='AT', start=start, end=end):
        pprint(t)


if __name__ == '__main__':
    main()
