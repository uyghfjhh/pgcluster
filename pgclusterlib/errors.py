class PgClusterError(Exception):
    """Expected user-facing error."""


class ConfigError(PgClusterError):
    pass


class OperationError(PgClusterError):
    pass


class SafetyError(PgClusterError):
    pass
