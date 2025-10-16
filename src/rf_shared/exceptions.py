class ChecksumMismatchError(ValueError):
    """Raised when a calculated file checksum does not match the expected one."""

    pass


class EnvelopeParsingError(ValueError):
    """A fatal error when the message envelope is malformed."""

    pass


class MetadataRecordParsingError(ValueError):
    """A fatal error when the MetadataRecord is malformed."""

    pass
