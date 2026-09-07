"""The unofficial EcoFlow BLE devices integration"""

import logging
from collections.abc import Callable
from functools import partial

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import (
    BluetoothCallbackMatcher,
    BluetoothChange,
    BluetoothScanningMode,
    BluetoothServiceInfoBleak,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import (
    ConfigEntryError,
    ConfigEntryNotReady,
)
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from . import eflib
from .config_flow import CONF_COLLECT_PACKETS, ConfLogOptions, LogOptions
from .connection_manager import DeviceConnectionManager
from .const import (
    CONF_ADVANCED_CONNECTION_OPTIONS,
    CONF_BLUEZ_START_NOTIFY,
    CONF_COLLECT_PACKETS_AMOUNT,
    CONF_CONNECTION_TIMEOUT,
    CONF_DIAGNOSTICS_ON_EXCEPTION,
    CONF_DIAGNOSTICS_OPTIONS,
    CONF_EXTRA_BATTERY,
    CONF_LOCAL_NAME,
    CONF_MANUFACTURER_DATA,
    CONF_UPDATE_PERIOD,
    CONF_USER_ID,
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_UPDATE_PERIOD,
    DOMAIN,
)
from .eflib.connection import Connection
from .eflib.logging_util import ConnectionLog

PLATFORMS: list[Platform] = [
    Platform.BUTTON,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SWITCH,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.CLIMATE,
]

type DeviceConfigEntry = ConfigEntry[eflib.DeviceBase]

_LOGGER = logging.getLogger(__name__)

ConfigEntryNotReady = partial(ConfigEntryNotReady, translation_domain=DOMAIN)
ConfigEntryError = partial(ConfigEntryError, translation_domain=DOMAIN)

_REAPPEAR_CALLBACKS_KEY = f"{DOMAIN}_reappear_callbacks"
_CONNECTION_MANAGERS_KEY = f"{DOMAIN}_connection_managers"


async def async_setup_entry(hass: HomeAssistant, entry: DeviceConfigEntry) -> bool:
    """Set up EF BLE device from a config entry."""
    _LOGGER.debug("Init EcoFlow BLE Integration")

    address = entry.data.get(CONF_ADDRESS)
    user_id = entry.data.get(CONF_USER_ID)

    if address is None or user_id is None:
        # Returning False here would fail setup without any log or UI message, so
        # raise instead to tell the user which of the two is missing
        raise ConfigEntryError(
            translation_key="missing_address_or_user_id",
            translation_placeholders={
                "missing": "address" if address is None else "user ID"
            },
        )

    device = _async_get_device(hass, entry, address)
    _cancel_reappear_callback(hass, entry)
    _async_cache_advertisement(hass, entry, device)

    _LOGGER.debug("Creating entities")
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_update_listener))

    manager = DeviceConnectionManager(hass, entry, device)
    hass.data.setdefault(_CONNECTION_MANAGERS_KEY, {})[entry.entry_id] = manager
    manager.async_start()

    _LOGGER.debug("Setup done")
    return True


@callback
def _async_get_device(
    hass: HomeAssistant, entry: DeviceConfigEntry, address: str
) -> eflib.DeviceBase:
    """Return the device for this entry, creating it if this is the first setup"""
    device: eflib.DeviceBase | None = getattr(entry, "runtime_data", None)
    discovery_info = bluetooth.async_last_service_info(hass, address, connectable=True)

    if device is not None:
        if discovery_info is not None:
            device.update_ble_device(discovery_info.device)
        return device

    if discovery_info is not None:
        device = eflib.NewDevice(discovery_info.device, discovery_info.advertisement)
    elif (cached := _cached_manufacturer_data(entry)) is not None:
        try:
            device = eflib.NewDeviceFromCache(
                address, entry.data.get(CONF_LOCAL_NAME), cached
            )
        except Exception:
            _LOGGER.exception(
                "Could not recreate %s from its cached advertisement", address
            )
            device = None
    else:
        _register_reappear_callback(hass, entry, address)
        raise ConfigEntryNotReady(translation_key="device_not_present")

    if device is None:
        raise ConfigEntryNotReady(translation_key="unable_to_create_device")

    entry.runtime_data = device
    return device


def _cached_manufacturer_data(entry: DeviceConfigEntry) -> bytes | None:
    """Advertisement stored for this entry, if it holds a usable one"""
    if (manufacturer_data := entry.data.get(CONF_MANUFACTURER_DATA)) is None:
        return None

    try:
        return bytes.fromhex(manufacturer_data)
    except ValueError:
        _LOGGER.warning(
            "Ignoring unreadable cached advertisement data %r", manufacturer_data
        )
        return None


@callback
def _async_cache_advertisement(
    hass: HomeAssistant, entry: DeviceConfigEntry, device: eflib.DeviceBase
) -> None:
    manufacturer_data = device.manufacturer_data.hex()
    if entry.data.get(CONF_MANUFACTURER_DATA) == manufacturer_data:
        return

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_MANUFACTURER_DATA: manufacturer_data}
    )


async def async_unload_entry(hass: HomeAssistant, entry: DeviceConfigEntry) -> bool:
    """Unload a config entry."""
    _cancel_reappear_callback(hass, entry)

    managers: dict[str, DeviceConnectionManager] = hass.data.get(
        _CONNECTION_MANAGERS_KEY, {}
    )
    if (manager := managers.pop(entry.entry_id, None)) is not None:
        await manager.async_stop()

    device = entry.runtime_data
    device.with_logging_options(LogOptions.no_options())
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_remove_entry(hass: HomeAssistant, entry: DeviceConfigEntry):
    _cancel_reappear_callback(hass, entry)
    ConnectionLog.clean_cache_for(entry.data[CONF_ADDRESS])


async def async_migrate_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Migrate old config entry to a newer version."""
    _LOGGER.debug(
        "Migrating configuration from version %s.%s",
        config_entry.version,
        config_entry.minor_version,
    )

    if config_entry.version < 2:
        if config_entry.minor_version < 1:
            address = config_entry.data.get(CONF_ADDRESS)
            device_reg = dr.async_get(hass)
            device_entry = device_reg.async_get_device(identifiers={(DOMAIN, address)})

            if device_entry is not None and device_entry.serial_number is not None:
                serial_number = device_entry.serial_number
                old_prefix = device_entry.name + "_"

                entity_reg = er.async_get(hass)
                for entity_entry in er.async_entries_for_config_entry(
                    entity_reg, config_entry.entry_id
                ):
                    old_unique_id = entity_entry.unique_id
                    if old_unique_id.startswith(old_prefix):
                        key = old_unique_id[len(old_prefix) :]
                        new_unique_id = f"ef_{serial_number}_{key}"
                        entity_reg.async_update_entity(
                            entity_entry.entity_id, new_unique_id=new_unique_id
                        )

            hass.config_entries.async_update_entry(config_entry, minor_version=1)

        if config_entry.minor_version < 2:
            data = {**config_entry.data}
            data.setdefault(CONF_EXTRA_BATTERY, [])
            hass.config_entries.async_update_entry(
                config_entry, data=data, minor_version=2
            )

    return True


def _register_reappear_callback(
    hass: HomeAssistant, entry: ConfigEntry, address: str
) -> None:
    callbacks: dict[str, Callable] = hass.data.setdefault(_REAPPEAR_CALLBACKS_KEY, {})

    if entry.entry_id in callbacks:
        return

    def _on_device_reappear(
        service_info: BluetoothServiceInfoBleak,
        change: BluetoothChange,
    ) -> None:
        _LOGGER.info(
            "Device %s reappeared via BLE advertisement, scheduling reload",
            address,
        )
        _cancel_reappear_callback(hass, entry)
        hass.config_entries.async_schedule_reload(entry.entry_id)

    cancel = bluetooth.async_register_callback(
        hass,
        _on_device_reappear,
        BluetoothCallbackMatcher(address=address, connectable=True),
        BluetoothScanningMode.PASSIVE,
    )
    callbacks[entry.entry_id] = cancel
    _LOGGER.debug("Registered BLE reappear callback for %s", address)


def _cancel_reappear_callback(hass: HomeAssistant, entry: ConfigEntry) -> None:
    callbacks: dict[str, Callable] = hass.data.get(_REAPPEAR_CALLBACKS_KEY, {})
    if cancel := callbacks.pop(entry.entry_id, None):
        cancel()


async def _update_listener(hass: HomeAssistant, entry: DeviceConfigEntry):
    device = entry.runtime_data
    merged_options = entry.data | entry.options
    update_period = merged_options.get(CONF_UPDATE_PERIOD, DEFAULT_UPDATE_PERIOD)
    diag_options = merged_options.get(CONF_DIAGNOSTICS_OPTIONS, {})
    packet_collection = diag_options.get(
        CONF_COLLECT_PACKETS, eflib.is_unsupported(device)
    )
    diagnostics_buffer_size = diag_options.get(CONF_COLLECT_PACKETS_AMOUNT, 100)
    diagnostics_on_exception = diag_options.get(CONF_DIAGNOSTICS_ON_EXCEPTION, False)
    advanced = merged_options.get(CONF_ADVANCED_CONNECTION_OPTIONS, {})
    options = Connection.Options(
        timeout=advanced.get(CONF_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT),
        bluez_start_notify=advanced.get(CONF_BLUEZ_START_NOTIFY, False),
    )

    (
        device.with_update_period(period=update_period)
        .with_logging_options(ConfLogOptions.from_config(merged_options))
        .with_enabled_packet_diagnostics(
            enabled=packet_collection,
            buffer_size=diagnostics_buffer_size,
        )
        .with_diagnostics_on_exception(diagnostics_on_exception)
        .with_connection_options(options)
    )
