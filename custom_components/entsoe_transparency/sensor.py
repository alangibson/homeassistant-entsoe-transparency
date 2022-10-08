"""
ENTSO-E Transparency sensors
"""

import logging
from datetime import timedelta, datetime, date
from dataclasses import dataclass

import dateutil
from homeassistant.const import (
    CONF_REGION,
    CONF_API_KEY,
    CONF_CURRENCY,
    EVENT_STATE_CHANGED
)

import pandas as pd
import xmltodict

from homeassistant.core import (
    HomeAssistant,
    State
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import (
    Entity
)
from homeassistant.components.sensor import (
    PLATFORM_SCHEMA,
    SensorEntity
)
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.const import (
    DEVICE_CLASS_MONETARY
)
from homeassistant.components.sensor import (
    SensorStateClass,
    SensorEntityDescription
)

from entsoe import EntsoeRawClient

from .const import DOMAIN


logger = logging.getLogger(__name__)


async def yield_day_ahead_rates(hass: HomeAssistant, api_key: str, country_code: str, start: pd.Timestamp, end: pd.Timestamp):
    """Get and yield hourly day-ahead rates"""
    client = EntsoeRawClient(api_key=api_key)
    xml_string = await hass.async_add_executor_job(lambda: client.query_day_ahead_prices(country_code, start, end))
    doc = xmltodict.parse(xml_string)

    for timeseries in doc['Publication_MarketDocument']['TimeSeries']:
        currency = timeseries['currency_Unit.name']
        timeseries['Period']['resolution']
        start_at = dateutil.parser.isoparse(
            timeseries['Period']['timeInterval']['start'])
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


# Called automagically by Home Assistant
async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback
):
    currency = config_entry.data[CONF_CURRENCY]
    country_code = config_entry.data[CONF_REGION]
    api_key = config_entry.data[CONF_API_KEY]

    # Register sensor entity description
    description = EntsoeTransparencySensorEntityDescription(
        key=f'dayahead_rate_{country_code}',
        name="Day-ahead Electricity Price ({country_code})",
        unit_of_measurement=currency,
        device_class=DEVICE_CLASS_MONETARY,
        native_unit_of_measurement=currency,
        state_class=SensorStateClass.MEASUREMENT,
        country_code=country_code,
        api_key=api_key
    )

    async_add_entities([EntsoeTransparencyDayAheadEntity(description)],
                       update_before_add=True
                       )


@dataclass
class EntsoeTransparencySensorEntityDescription(SensorEntityDescription):
    country_code: str = None
    api_key: str = None


class EntsoeTransparencyDayAheadEntity(SensorEntity):

    entity_description: EntsoeTransparencySensorEntityDescription

    def __init__(self, description: EntsoeTransparencySensorEntityDescription):
        self.entity_description = description
        country_code = self.entity_description.country_code
        self._attr_unique_id = f'entsoe_transparency_day_ahead_{country_code}'
        self._attr_name = f'ENTSOE Day-ahead Prices {country_code}'
        self._attr_should_poll = True
        self.last_successful_update: datetime = None
        # TODO Can't we get this from somewhere?
        self.entity_id = f'sensor.entsoe_day_ahead_prices_{country_code}'.lower()

    async def async_update(self) -> None:
        """Called occasionally by HA to update data"""
        logger.debug('EntsoeTransparencyDayAheadEntity.async_update called')

        # Decide if we should update or not
        # last successful update was yesterday or earlier
        if self.last_successful_update and \
                self.last_successful_update.date() >= date.today():
            logger.debug('Skipping update because its not time yet')
            return

        # Make sure we can connect to ENTSOE Transparency API
        api_key = self.entity_description.api_key
        country_code = self.entity_description.country_code
        start = pd.Timestamp.now(tz='UTC').replace(
            hour=0, minute=0, second=0, microsecond=0)
        end = start + pd.Timedelta(value=1, unit='day')
        logger.debug(f'Querying {country_code} rates between {start} and {end}')

        # Update pricing info
        old_state = None
        async for t in yield_day_ahead_rates(hass=self.hass, api_key=api_key, country_code=country_code, start=start, end=end):
            logger.debug(f'{self.entity_id} {t["timepoint"]} {t["price"]}')
            new_state = State(
                entity_id=self.entity_id,
                state=t['price'],
                last_changed=t['timepoint'],
                last_updated=t['timepoint'],
                validate_entity_id=True
            )
            event_data = {
                "entity_id": self.entity_id,
                "old_state": old_state,
                "new_state": new_state
            }
            self.hass.bus.async_fire(
                EVENT_STATE_CHANGED,
                event_data,
                time_fired=t['timepoint']
            )
            old_state = new_state

        # TODO handle errors if we can't connect

        self.last_successful_update = datetime.now()
