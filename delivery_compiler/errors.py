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


class PackageExistsError(DeliveryError):
    """A package directory already exists with different content (packages are immutable)."""


class VerificationError(DeliveryError):
    """Independent verification failed."""
