# tests/test_data_quality.py
"""
Data quality validation framework.
Runs automated checks after each pipeline stage.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, max as spark_max
from dataclasses import dataclass
from typing import List
from datetime import datetime, timedelta


@dataclass
class QualityCheckResult:
    table: str
    check_name: str
    severity: str  # critical, warning
    passed: bool
    details: str
    checked_at: str


class DataQualityChecker:
    """Reusable data quality checker for any Delta table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: List[QualityCheckResult] = []

    def check_not_null(self, df: DataFrame, table: str, columns: list) -> None:
        """Verify that key columns have no null values."""
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            self.results.append(QualityCheckResult(
                table=table,
                check_name=f"not_null_{column}",
                severity="critical",
                passed=null_count == 0,
                details=f"{null_count} null values found in {column}",
                checked_at=str(datetime.now())
            ))

    def check_unique(self, df: DataFrame, table: str, columns: list) -> None:
        """Verify uniqueness of key columns."""
        total = df.count()
        distinct = df.select(*columns).distinct().count()
        self.results.append(QualityCheckResult(
            table=table,
            check_name=f"unique_{'_'.join(columns)}",
            severity="critical",
            passed=total == distinct,
            details=f"{total - distinct} duplicate rows found",
            checked_at=str(datetime.now())
        ))

    def check_accepted_values(self, df: DataFrame, table: str, column: str, values: list) -> None:
        """Verify column only contains expected values."""
        invalid = df.filter(~col(column).isin(values)).count()
        self.results.append(QualityCheckResult(
            table=table,
            check_name=f"accepted_values_{column}",
            severity="warning",
            passed=invalid == 0,
            details=f"{invalid} rows with invalid {column}",
            checked_at=str(datetime.now())
        ))

    def check_freshness(self, df: DataFrame, table: str, timestamp_col: str, max_hours: int = 24) -> None:
        """Verify data is fresh (within max_hours)."""
        latest = df.agg(spark_max(col(timestamp_col))).collect()[0][0]
        threshold = datetime.now() - timedelta(hours=max_hours)
        is_fresh = latest is not None and latest > threshold
        self.results.append(QualityCheckResult(
            table=table,
            check_name="freshness",
            severity="warning",
            passed=is_fresh,
            details=f"Latest record: {latest}, threshold: {threshold}",
            checked_at=str(datetime.now())
        ))

    def check_row_count(self, df: DataFrame, table: str, min_rows: int = 1) -> None:
        """Verify table is not empty."""
        row_count = df.count()
        self.results.append(QualityCheckResult(
            table=table,
            check_name="row_count",
            severity="critical",
            passed=row_count >= min_rows,
            details=f"{row_count} rows (minimum: {min_rows})",
            checked_at=str(datetime.now())
        ))

    def check_non_negative(self, df: DataFrame, table: str, columns: list) -> None:
        """Verify numeric columns have no negative values."""
        for column in columns:
            negative_count = df.filter(col(column) < 0).count()
            self.results.append(QualityCheckResult(
                table=table,
                check_name=f"non_negative_{column}",
                severity="critical",
                passed=negative_count == 0,
                details=f"{negative_count} negative values in {column}",
                checked_at=str(datetime.now())
            ))

    def get_summary(self) -> dict:
        """Return summary of all checks."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed_critical = [r for r in self.results if not r.passed and r.severity == "critical"]
        failed_warning = [r for r in self.results if not r.passed and r.severity == "warning"]

        return {
            "total_checks": total,
            "passed": passed,
            "failed_critical": len(failed_critical),
            "failed_warning": len(failed_warning),
            "status": "FAIL" if failed_critical else "PASS",
            "details": [
                {"check": r.check_name, "table": r.table, "severity": r.severity, "details": r.details}
                for r in self.results if not r.passed
            ]
        }

    def assert_quality(self) -> None:
        """Raise exception if any critical checks failed."""
        summary = self.get_summary()
        if summary["status"] == "FAIL":
            raise ValueError(f"Data quality check FAILED: {summary}")
        print(f"All quality checks PASSED ({summary['passed']}/{summary['total_checks']})")


# --- Usage Example ---
#
# checker = DataQualityChecker(spark)
# df = spark.table("main.silver.orders")
#
# checker.check_not_null(df, "orders", ["order_id", "customer_id"])
# checker.check_unique(df, "orders", ["order_id"])
# checker.check_accepted_values(df, "orders", "status", ["pending", "shipped", "delivered", "cancelled"])
# checker.check_non_negative(df, "orders", ["quantity", "unit_price"])
# checker.check_freshness(df, "orders", "_ingestion_timestamp", max_hours=24)
# checker.check_row_count(df, "orders", min_rows=100)
#
# checker.assert_quality()  # Raises if critical failures
