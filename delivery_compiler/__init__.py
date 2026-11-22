"""Declarative delivery compiler (Python + DuckDB).

Public interfaces:
  compile_plan   -- validate + plan a delivery for one consumer
  generate       -- execute a plan into an immutable, content-addressed package
  verify_package -- independent verification (does not trust the compiler)
  read_package   -- read a package as its bound consumer
  history        -- consumer-side customer history store (load/show)
"""
from .compiler import Plan, compile_plan, format_decisions
from .errors import (CyclicLineageError, DeliveryError, DanglingReferenceError,
                     HistoryError, InvalidProjectionError,
                     OutputNameConflictError, PackageExistsError,
                     PolicyConflictError, RowKeyError, SourceDriftError,
                     UncoveredSensitiveError, UnresolvableLineageError,
                     VerificationError)
from .generator import generate
from .history import connect as history_connect
from .history import load_package, show_history
from .reader import lookup_row_lineage, read_package
from .schema import SourceSchema
from .verifier import verify_package

__all__ = [
    "Plan", "compile_plan", "format_decisions", "generate", "verify_package",
    "read_package", "lookup_row_lineage", "SourceSchema",
    "history_connect", "load_package", "show_history",
    "DeliveryError", "PolicyConflictError", "DanglingReferenceError",
    "CyclicLineageError", "UnresolvableLineageError", "OutputNameConflictError",
    "UncoveredSensitiveError", "InvalidProjectionError", "SourceDriftError",
    "PackageExistsError", "VerificationError", "HistoryError", "RowKeyError",
]
