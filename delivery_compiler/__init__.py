"""Declarative delivery compiler (Python + DuckDB).

Public interfaces:
  compile_plan   -- validate + plan a delivery for one consumer
  generate       -- execute a plan into an immutable, content-addressed package
  verify_package -- independent verification (does not trust the compiler)
  read_package   -- read a package as its bound consumer
"""
from .compiler import Plan, compile_plan, format_decisions
from .errors import (CyclicLineageError, DeliveryError, DanglingReferenceError,
                     InvalidProjectionError, OutputNameConflictError,
                     PackageExistsError, PolicyConflictError, SourceDriftError,
                     UncoveredSensitiveError, UnresolvableLineageError,
                     VerificationError)
from .generator import generate
from .reader import read_package
from .schema import SourceSchema
from .verifier import verify_package

__all__ = [
    "Plan", "compile_plan", "format_decisions", "generate", "verify_package",
    "read_package", "SourceSchema",
    "DeliveryError", "PolicyConflictError", "DanglingReferenceError",
    "CyclicLineageError", "UnresolvableLineageError", "OutputNameConflictError",
    "UncoveredSensitiveError", "InvalidProjectionError", "SourceDriftError",
    "PackageExistsError", "VerificationError",
]
