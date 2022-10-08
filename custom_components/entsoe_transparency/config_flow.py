"""ENTSO-E Transparency config flow."""
from __future__ import annotations

import logging

import voluptuous as vol
import pandas as pd
import xmltodict

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigFlow
from homeassistant.const import (
    CONF_REGION,
    CONF_API_KEY,
    CONF_CURRENCY
)

from entsoe import EntsoeRawClient

from .const import DOMAIN, DEFAULT_NAME

logger = logging.getLogger(__name__)


DATA_SCHEMA = vol.Schema({
    vol.Required(CONF_API_KEY): str,
    vol.Required(CONF_REGION): str,
    vol.Required(CONF_CURRENCY): str
})


class EntsoeTransparencyConfigFlow(ConfigFlow, domain=DOMAIN):
    """ENTSO-E Transparency config flow."""

    VERSION = 1

    async def validate_input(self, hass: HomeAssistant, user_input):

        # TODO validate that CONF_REGION and CONF_API_KEY are set
        # Make sure we can connect to ENTSOE Transparency API
        api_key = user_input[CONF_API_KEY]
        country_code = user_input[CONF_REGION]
        currency = user_input[CONF_CURRENCY]
        logger.debug(
            f'API key {api_key} country_code {country_code} currency {currency}')

        # Figure out start and end times for query
        start = pd.Timestamp.now(tz='UTC').replace(
            hour=0, minute=0, second=0, microsecond=0)
        end = start + pd.Timedelta(value=1, unit='day')
        logger.debug('Query time start: {start}, end: {end}')

        # TODO validate that we can connect to ENTSO-E Transparency api
        client = EntsoeRawClient(api_key=api_key)
        xml_string = await hass.async_add_executor_job(
            lambda: client.query_day_ahead_prices(country_code, start, end))
        logger.debug(xml_string)
        # TODO error if we could not get data

        return (None, user_input)

    async def async_step_user(self, user_input=None):
        """Handle the initial configuration step."""

        if not user_input:
            # Just show the modal form and return if no user input
            return self.async_show_form(step_id="user", data_schema=DATA_SCHEMA)
        else:
            # We got user input, so do something with it

            # Validate input
            logger.debug(f'Validating input {user_input}')
            errors, user_input = await self.validate_input(self.hass, user_input)
            logger.debug('Validation returned {errors} {user_input}')

            if not errors:
                # Create sensor entry
                self._abort_if_unique_id_configured(
                    updates={CONF_REGION: user_input[CONF_REGION]})
                await self.async_set_unique_id(f'entsoe-transparency-{user_input[CONF_REGION]}')
                return self.async_create_entry(title=f'{DEFAULT_NAME} ({user_input[CONF_REGION]})', data=user_input)
            else:
                # Show form again and display errors
                return self.async_show_form(
                    step_id="user", data_schema=DATA_SCHEMA, errors=errors
                )
