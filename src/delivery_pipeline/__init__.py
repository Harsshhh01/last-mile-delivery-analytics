"""Last-mile delivery analytics pipeline.

Kept import-light on purpose: the AWS Lambda ships this package without PySpark,
so nothing here may import Spark at module load.
"""

__version__ = "1.0.0"
