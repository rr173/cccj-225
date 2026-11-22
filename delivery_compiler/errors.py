"""Typed errors raised by the delivery compiler pipeline."""


class DeliveryError(Exception):
    """Base class for all delivery-compiler errors."""


class PolicyConflictError(DeliveryError):
    """Two rules collide at equal priority/specificity; file order is never a tie-breaker."""


class DanglingReferenceError(DeliveryError):
    """A projection, group key, relation or tag points at a field that does not exist."""


class CyclicLineageError(DeliveryError):
    """Derived fields form a cycle."""


class UnresolvableLineageError(DeliveryError):
    """An expression references something that cannot be resolved; it must not be published."""


class OutputNameConflictError(DeliveryError):
    """Two outputs share the same name."""


class UncoveredSensitiveError(DeliveryError):
    """A sensitive-tagged field is requested but no policy rule covers it."""


class InvalidProjectionError(DeliveryError):
    """A projection violates structural rules (bad aggregate, row-level aggregate, ...)."""


class SourceDriftError(DeliveryError):
    """The source no longer matches the pinned version the plan was compiled against."""


class RowKeyError(DeliveryError):
    """A ``_row_keys`` declaration is missing/incomplete or violates a key guarantee.

    Raised when a business table has no declared stable row key, a declared
    key column does not exist, any key value is NULL, or a composite key
    combination is not unique. Error messages name tables and columns only;
    they never embed key values or other source data.
    """


class PackageExistsError(DeliveryError):
    """A package directory already exists with different content (packages are immutable)."""


class VerificationError(DeliveryError):
    """Independent verification failed."""


class HistoryError(DeliveryError):
    """Consumer-side history store error.

    Raised for unsupported package bindings, missing files/columns, primary-key
    violations, revision conflicts (reused revision with new content, or a
    never-ingested revision below the current one) and similar load/show
    failures. A failed load never consumes an ingestion sequence number and
    never leaves partial state behind.
    """
