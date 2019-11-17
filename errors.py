"""Provides exceptions used by the hana_os_monitor modules.
All exceptions here start with "Monitor" and end with "Error"."""


class MonitorError(Exception):
    """The root of all Monitor errors."""
    pass


class MonitorOSOpError(MonitorError):
    """Raised by OS Operator"""
    pass


class MonitorDBOpError(MonitorError):
    """Raised by DB Operator"""
    pass


class MonitorDBError(MonitorError):
    """Raised by HANA DB """
    pass


class MonitorUtilError(MonitorError):
    """Raised by Util, eg: can't get configuration"""
    pass