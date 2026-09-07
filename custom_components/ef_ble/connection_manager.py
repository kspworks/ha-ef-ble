"""Keeping the BLE link up without tying entity existence to it"""

import asyncio
import contextlib
import logging
from collections.abc import Callable

import homeassistant.helpers.issue_registry as ir
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import (
    BluetoothCallbackMatcher,
    BluetoothChange,
    BluetoothScanningMode,
    BluetoothServiceInfoBleak,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant, callback

from . import eflib
from .config_flow import CONF_COLLECT_PACKETS, ConfLogOptions, PacketVersion
from .const import (
    CONF_ADVANCED_CONNECTION_OPTIONS,
    CONF_BLUEZ_START_NOTIFY,
    CONF_CONNECTION_DELAY,
    CONF_CONNECTION_TIMEOUT,
    CONF_DIAGNOSTICS_ON_EXCEPTION,
    CONF_DIAGNOSTICS_OPTIONS,
    CONF_PACKET_VERSION,
    CONF_PREFERRED_PROXY,
    CONF_PREFERRED_PROXY_TIMEOUT,
    CONF_UPDATE_PERIOD,
    CONF_USER_ID,
    DEFAULT_CONNECTION_DELAY,
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_PREFERRED_PROXY_TIMEOUT,
    DEFAULT_UPDATE_PERIOD,
    DOMAIN,
    INITIAL_RECONNECT_DELAY,
    MAX_RECONNECT_DELAY,
    NO_PREFERRED_PROXY,
)
from .eflib.connection import (
    BleakError,
    Connection,
    ConnectionTimeout,
    MaxConnectionAttemptsReached,
)
from .eflib.exceptions import AuthErrors, UnsupportedBluetoothProtocol
from .proxy import connect_gate, wait_for_preferred_proxy

_LOGGER = logging.getLogger(__name__)

_FAILURES_BEFORE_ISSUE = 10
_AUTH_FAILURES_BEFORE_ISSUE = 2


class DeviceConnectionManager:
    """Owns the connection to one device for the lifetime of its config entry"""

    def __init__(
        self, hass: HomeAssistant, entry: ConfigEntry, device: eflib.DeviceBase
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._device = device
        self._address: str = entry.data[CONF_ADDRESS]

        self._advertising = asyncio.Event()
        self._disconnected = asyncio.Event()
        self._unsubscribe: list[Callable[[], None]] = []
        self._task: asyncio.Task[None] | None = None

        self._failures = 0
        self._auth_failures = 0

    @property
    def _connect_issue_id(self) -> str:
        return f"{self._entry.entry_id}_max_connection_attempts"

    @property
    def _auth_issue_id(self) -> str:
        return f"{self._entry.entry_id}_authentication_failed"

    @callback
    def async_start(self) -> None:
        """Watch for the device on air and keep it connected from now on"""
        self._unsubscribe.append(
            bluetooth.async_register_callback(
                self._hass,
                self._async_on_advertisement,
                BluetoothCallbackMatcher(address=self._address, connectable=True),
                BluetoothScanningMode.PASSIVE,
            )
        )
        self._unsubscribe.append(self._device.on_disconnect(self._on_disconnect))

        if bluetooth.async_address_present(self._hass, self._address, connectable=True):
            self._advertising.set()

        self._task = self._entry.async_create_background_task(
            self._hass,
            self._async_run(),
            name=f"{DOMAIN} {self._address} connection",
        )

    async def async_stop(self) -> None:
        """Stop connecting and drop the link"""
        for unsubscribe in self._unsubscribe:
            unsubscribe()
        self._unsubscribe.clear()

        if (task := self._task) is not None:
            self._task = None
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self._async_disconnect()

    @callback
    def _async_on_advertisement(
        self, service_info: BluetoothServiceInfoBleak, change: BluetoothChange
    ) -> None:
        self._device.update_ble_device(service_info.device)

        if not self._advertising.is_set():
            _LOGGER.debug("%s: device is advertising again", self._device.name)
            self._advertising.set()

    def _on_disconnect(self, exc: Exception | type[Exception] | None) -> None:
        self._disconnected.set()

    async def _async_run(self) -> None:
        """Keep the device connected for as long as the config entry is loaded"""
        while True:
            try:
                await self._async_connect_and_wait()
            except Exception:
                _LOGGER.exception(
                    "%s: unexpected error in the connection loop", self._device.name
                )
                await asyncio.sleep(self._backoff)

    async def _async_connect_and_wait(self) -> None:
        await self._advertising.wait()

        ble_device = bluetooth.async_ble_device_from_address(
            self._hass, self._address, connectable=True
        )
        if ble_device is None:
            _LOGGER.debug("%s: no connectable path to the device", self._device.name)
            self._advertising.clear()
            await asyncio.sleep(INITIAL_RECONNECT_DELAY)
            return

        self._device.update_ble_device(ble_device)

        self._disconnected.clear()
        if not await self._async_connect():
            if not self._advertising.is_set():
                return
            await asyncio.sleep(self._backoff)
            return

        await self._disconnected.wait()
        _LOGGER.debug("%s: connection lost, reconnecting", self._device.name)

    @property
    def _backoff(self) -> float:
        return min(
            INITIAL_RECONNECT_DELAY * 2 ** (max(self._failures, 1) - 1),
            MAX_RECONNECT_DELAY,
        )

    async def _async_connect(self) -> bool:
        """Bring the link up and authenticate, reporting whether that worked"""
        entry = self._entry
        device = self._device

        merged_options = entry.data | entry.options
        update_period = merged_options.get(CONF_UPDATE_PERIOD, DEFAULT_UPDATE_PERIOD)
        packet_version = PacketVersion.from_str(
            entry.data.get(CONF_PACKET_VERSION, PacketVersion.V3)
        )
        diag_options = merged_options.get(CONF_DIAGNOSTICS_OPTIONS, {})
        packet_collection_enabled = diag_options.get(
            CONF_COLLECT_PACKETS, eflib.is_unsupported(device)
        )
        diagnostics_on_exception = diag_options.get(
            CONF_DIAGNOSTICS_ON_EXCEPTION, False
        )

        advanced = merged_options.get(CONF_ADVANCED_CONNECTION_OPTIONS, {})
        timeout = advanced.get(CONF_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT)
        connection_delay = advanced.get(CONF_CONNECTION_DELAY, DEFAULT_CONNECTION_DELAY)
        preferred_proxy = advanced.get(CONF_PREFERRED_PROXY) or NO_PREFERRED_PROXY
        options = Connection.Options(
            timeout=timeout,
            bluez_start_notify=advanced.get(CONF_BLUEZ_START_NOTIFY, False),
        )
        preference_wait = (
            advanced.get(CONF_PREFERRED_PROXY_TIMEOUT, DEFAULT_PREFERRED_PROXY_TIMEOUT)
            if preferred_proxy != NO_PREFERRED_PROXY
            else 0.0
        )

        try:
            async with connect_gate(
                self._hass, device.name, connection_delay, timeout, preference_wait
            ):
                if preference_wait:
                    await wait_for_preferred_proxy(
                        self._hass,
                        self._address,
                        device.name,
                        preferred_proxy,
                        preference_wait,
                    )
                await (
                    device.with_update_period(update_period)
                    .with_logging_options(ConfLogOptions.from_config(merged_options))
                    .with_disabled_reconnect()
                    .with_packet_version(packet_version.to_num())
                    .with_enabled_packet_diagnostics(packet_collection_enabled)
                    .with_diagnostics_on_exception(diagnostics_on_exception)
                    .with_connection_options(options)
                    .connect(
                        user_id=entry.data.get(CONF_USER_ID),
                        max_attempts=0 if eflib.is_solar_only(device) else None,
                    )
                )
            async with asyncio.timeout(timeout):
                state = await device.wait_until_authenticated_or_error(
                    raise_on_error=True
                )
        except (
            ConnectionTimeout,
            BleakError,
            TimeoutError,
            UnsupportedBluetoothProtocol,
        ) as e:
            await self._async_failed(
                "%s: could not connect within %s seconds: %s",
                device.name,
                timeout,
                e,
            )
        except AuthErrors.BaseException as e:
            self._auth_failures += 1
            await self._async_failed("%s: authentication failed: %s", device.name, e)
            if self._auth_failures >= _AUTH_FAILURES_BEFORE_ISSUE:
                self._async_create_issue(self._auth_issue_id, "authentication_failed")
        except MaxConnectionAttemptsReached as e:
            await self._async_failed(
                "%s: giving up on this attempt after %s tries: %s",
                device.name,
                e.attempts,
                e,
            )
        except Exception as e:  # noqa: BLE001 - the loop must survive anything
            await self._async_failed(
                "%s: unknown error while connecting: %s",
                device.name,
                e,
                exc_info=True,
            )
        else:
            if not state.authenticated:
                await self._async_failed(
                    "%s: connected but did not authenticate, last state: %s",
                    device.name,
                    state,
                )
                return False

            if self._failures or self._auth_failures:
                _LOGGER.info("%s: connected again", device.name)
            self._failures = 0
            self._auth_failures = 0
            ir.async_delete_issue(self._hass, DOMAIN, self._connect_issue_id)
            ir.async_delete_issue(self._hass, DOMAIN, self._auth_issue_id)
            return True

        return False

    async def _async_failed(
        self, message: str, *args: object, exc_info: bool = False
    ) -> None:
        """Tear the half-open link down and report the failed attempt"""
        await self._async_disconnect()

        if not bluetooth.async_address_present(
            self._hass, self._address, connectable=True
        ):
            _LOGGER.debug(message, *args, exc_info=exc_info)
            self._failures = 0
            self._advertising.clear()
            return

        self._failures += 1
        _LOGGER.log(
            logging.WARNING if self._failures == 1 else logging.DEBUG,
            message,
            *args,
            exc_info=exc_info,
        )

        if self._failures == _FAILURES_BEFORE_ISSUE:
            self._async_create_issue(
                self._connect_issue_id,
                "max_connection_attempts_reached",
                attempts=str(self._failures),
            )

    async def _async_disconnect(self) -> None:
        if self._device.connection_state is None:
            return
        try:
            await self._device.disconnect()
        except Exception:
            _LOGGER.exception("%s: error while disconnecting", self._device.name)

    @callback
    def _async_create_issue(
        self, issue_id: str, translation_key: str, **placeholders: str
    ) -> None:
        ir.async_create_issue(
            self._hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key=translation_key,
            translation_placeholders={
                "device_name": self._device.name,
                **placeholders,
            },
        )
