"""Compatibility package for vendored Hive Metastore Thrift bindings.

The generated Hive bindings import ``fb303`` as a top-level package. Keep the
vendored source under ``etl.vendor.hms`` while exposing that import path.
"""

from pathlib import Path

_vendor_path = Path(__file__).resolve().parent.parent / "etl" / "vendor" / "hms" / "fb303"
if _vendor_path.exists():
    __path__.append(str(_vendor_path))
