import enum


class Status(enum.Enum):
    UNTRIED = "NEW"
    REISSUE = "RERUN"
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    UNKNOWN = "UNKNOWN"
