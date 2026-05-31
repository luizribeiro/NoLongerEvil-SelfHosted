"""Device availability tracking service."""

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from nolongerevil.lib.logger import get_logger

if TYPE_CHECKING:
    from nolongerevil.integrations.integration_manager import IntegrationManager
    from nolongerevil.services.subscription_manager import SubscriptionManager

logger = get_logger(__name__)

# Default timeout values (in seconds)
# Must exceed the subscribe long-poll cycle: the device holds ~290s
# (connection_hold_timeout) then re-subscribes, so its contact interval is
# ~300-305s. A 300s timeout marks it unavailable right before each re-subscribe
# (it flaps, esp. on wall power where radio power-save adds wake latency). Use
# ~2x the hold so a normal cycle — and one missed one — never trips it.
DEFAULT_DEVICE_TIMEOUT = 600  # 10 minutes (> one long-poll hold cycle)
DEFAULT_CHECK_INTERVAL = 30  # 30 seconds


@dataclass
class DeviceStatus:
    """Tracking data for a device's availability."""

    serial: str
    last_seen: datetime = field(default_factory=datetime.now)
    is_available: bool = True


class DeviceAvailability:
    """Watchdog service for tracking device availability.

    Monitors when devices were last seen and marks them as
    unavailable after a timeout period.
    """

    def __init__(
        self,
        subscription_manager: "SubscriptionManager",
        timeout_seconds: int = DEFAULT_DEVICE_TIMEOUT,
        check_interval_seconds: int = DEFAULT_CHECK_INTERVAL,
    ) -> None:
        """Initialize the device availability service.

        Args:
            subscription_manager: Subscription manager for heartbeat checks
            timeout_seconds: Time after which a device is considered unavailable
            check_interval_seconds: How often to check for stale devices
        """
        self._subscription_manager = subscription_manager
        self._timeout = timedelta(seconds=timeout_seconds)
        self._check_interval = check_interval_seconds
        self._devices: dict[str, DeviceStatus] = {}
        self._integration_manager: IntegrationManager | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    def set_integration_manager(self, manager: "IntegrationManager") -> None:
        """Set the integration manager for availability notifications.

        Args:
            manager: Integration manager instance
        """
        self._integration_manager = manager

    def initialize_from_serials(self, serials: list[str]) -> None:
        """Initialize tracking for known devices loaded from storage.

        Devices are initially marked as available since they exist in the database.
        The monitoring loop will mark them unavailable if they don't communicate.

        Args:
            serials: List of known device serial numbers
        """
        for serial in serials:
            if serial not in self._devices:
                self._devices[serial] = DeviceStatus(
                    serial=serial,
                    last_seen=datetime.now(),
                    is_available=True,
                )
        if serials:
            logger.info(f"Initialized availability tracking for {len(serials)} device(s)")

    async def start(self) -> None:
        """Start the availability monitoring task."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(
            f"Device availability monitoring started "
            f"(timeout={self._timeout.total_seconds()}s, "
            f"interval={self._check_interval}s)"
        )

    async def stop(self) -> None:
        """Stop the availability monitoring task."""
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Device availability monitoring stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            try:
                await self._check_devices()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in availability monitor: {e}")
                await asyncio.sleep(self._check_interval)

    async def _check_devices(self) -> None:
        """Check all devices for timeout."""
        now = datetime.now()
        stale_threshold = now - self._timeout

        for serial, status in list(self._devices.items()):
            # Check if device has active subscriptions (heartbeat)
            if self._subscription_manager.has_active_subscription(serial):
                await self.mark_device_seen(serial)
                continue

            # Check for timeout
            if status.is_available and status.last_seen < stale_threshold:
                await self._mark_device_unavailable(serial)

    async def mark_device_seen(self, serial: str) -> None:
        """Mark a device as recently seen.

        Args:
            serial: Device serial number
        """
        now = datetime.now()

        if serial not in self._devices:
            self._devices[serial] = DeviceStatus(serial=serial, last_seen=now)
            logger.info(f"Device {serial} is now being tracked")

            # Notify integrations of new device
            if self._integration_manager:
                await self._integration_manager.on_device_connected(serial)
        else:
            was_available = self._devices[serial].is_available
            self._devices[serial].last_seen = now

            # Device came back online
            if not was_available:
                self._devices[serial].is_available = True
                logger.info(f"Device {serial} is now available")

                if self._integration_manager:
                    await self._integration_manager.on_device_connected(serial)

    async def _mark_device_unavailable(self, serial: str) -> None:
        """Mark a device as unavailable.

        Args:
            serial: Device serial number
        """
        if serial not in self._devices:
            return

        if self._devices[serial].is_available:
            self._devices[serial].is_available = False
            logger.warning(f"Device {serial} is now unavailable (timeout)")

            if self._integration_manager:
                await self._integration_manager.on_device_disconnected(serial)

    def is_available(self, serial: str) -> bool:
        """Check if a device is currently available.

        Args:
            serial: Device serial number

        Returns:
            True if device is available
        """
        status = self._devices.get(serial)
        return status.is_available if status else False

    def get_last_seen(self, serial: str) -> datetime | None:
        """Get when a device was last seen.

        Args:
            serial: Device serial number

        Returns:
            Last seen timestamp or None if never seen
        """
        status = self._devices.get(serial)
        return status.last_seen if status else None

    def get_all_statuses(self) -> dict[str, dict[str, Any]]:
        """Get availability status for all tracked devices.

        Returns:
            Dictionary mapping serial to status info
        """
        return {
            serial: {
                "is_available": status.is_available,
                "last_seen": status.last_seen.isoformat(),
            }
            for serial, status in self._devices.items()
        }
