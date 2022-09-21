"""
ENTSO-E Transparency sensors
"""

from collections.abc import Iterable
from curses import meta
from datetime import (datetime, timezone)
import logging
import sys
from datetime import timedelta
from dataclasses import dataclass

import dateutil
from homeassistant.const import (
    CONF_REGION, 
    CONF_API_KEY, 
    CONF_CURRENCY
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
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
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
        # TODO add country code to unique id
        self._attr_unique_id = 'entsoe_transparency_day_ahead'
        # TODO add country code to name
        self._attr_name = 'ENTSO-E Transparency Day-ahead Rates'
        # TODO set polling time to 1 day
        self._attr_should_poll = False

    async def async_update(self) -> None:
        logger.debug('EntsoeTransparencyDayAheadEntity.async_update')

        # TODO one StatisticData for each time block
        # metadata: StatisticMetaData = StatisticMetaData()
        # metadata['statistic_id'] = DOMAIN + ':day_ahead_rate'
        # metadata['source'] = DOMAIN
        # metadata['unit_of_measurement'] = 'EUR'
        # metadata['name'] = 'Spot Price'
        # metadata['has_mean'] = False
        # metadata['has_sum'] = False

        # now = datetime.now(timezone.utc)
        # statistics: Iterable[StatisticData] = [
        #     {'start': now.replace(minute=0, second=0,
        #                           microsecond=0), 'state': '10'},
        #     {'start': now.replace(hour=now.hour+1, minute=0,
        #                           second=0, microsecond=0), 'state': '20'},
        #     {'start': now.replace(hour=now.hour+2, minute=0,
        #                           second=0, microsecond=0), 'state': '30'},
        #     {'start': now.replace(hour=now.hour+3, minute=0,
        #                           second=0, microsecond=0), 'state': '40'},
        #     {'start': now.replace(hour=now.hour+4, minute=0,
        #                           second=0, microsecond=0), 'state': '50'},
        #     {'start': now.replace(hour=now.hour+5, minute=0,
        #                           second=0, microsecond=0), 'state': '40'},
        #     {'start': now.replace(hour=now.hour+6, minute=0,
        #                           second=0, microsecond=0), 'state': '30'},
        #     {'start': now.replace(hour=now.hour+7, minute=0,
        #                           second=0, microsecond=0), 'state': '50'},
        #     {'start': now.replace(hour=now.hour+8, minute=0,
        #                           second=0, microsecond=0), 'state': '60'},
        #     {'start': now.replace(hour=now.hour+9, minute=0,
        #                           second=0, microsecond=0), 'state': '70'},
        #     {'start': now.replace(hour=now.hour+10, minute=0,
        #                           second=0, microsecond=0), 'state': '80'},
        #     {'start': now.replace(hour=now.hour+11, minute=0,
        #                           second=0, microsecond=0), 'state': '60'},
        #     {'start': now.replace(hour=now.hour+12, minute=0,
        #                           second=0, microsecond=0), 'state': '50'},
        # ]

        entity_id = "sensor.entso_e_transparency_day_ahead_rates"

        # Make sure we can connect to ENTSOE Transparency API
        api_key = self.entity_description.api_key
        country_code = self.entity_description.country_code
        start = pd.Timestamp.now(tz='UTC').replace(
            hour=0, minute=0, second=0, microsecond=0)
        end = start + pd.Timedelta(value=1, unit='day')

        old_state = None
        async for t in yield_day_ahead_rates(hass=self.hass, api_key=api_key, country_code=country_code, start=start, end=end):
            new_state = State(
                entity_id=entity_id,
                state=t['price'],
                last_changed=t['timepoint'],
                last_updated=t['timepoint']
            )
            event_data = {
                "entity_id": entity_id,
                "old_state": old_state,
                "new_state": new_state
            }
            old_state = new_state
            self.hass.bus.async_fire(
                "state_changed", event_data, time_fired=t['timepoint'])

            # FIXME this call freezes forever
            # self.hass.states.set(entity_id, new_state, force_update=True)

            # state = self.hass.states.get(self._attr_unique_id)
            # logger.debug('Retrieved state object', state)
            # inputState = state.state
            # inputAttributesObject = state.attributes.copy()
            # self.hass.states.set(self, inputState, inputAttributesObject)
