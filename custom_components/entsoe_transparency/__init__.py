"""
Support for ENTSO-E Transparency.
"""

from collections.abc import Iterable

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    Platform
)

async def async_setup_entity_platforms(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    platforms: Iterable[str],
) -> None:
    hass.config_entries.async_setup_platforms(config_entry, platforms)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up from a config entry."""
    await async_setup_entity_platforms(hass, entry, [Platform.SENSOR])
    return True

