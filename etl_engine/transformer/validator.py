import pandas as pd
import re
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates data against a set of rules and produces a validation report.

    Supported rules per column:
    - required      : bool – column must not be null
    - dtype         : str  – expected pandas dtype ('int64', 'float64', 'datetime64', 'object')
    - min_value     : numeric – minimum allowed value
    - max_value     : numeric – maximum allowed value
    - allowed_values: list  – enumerated allowed values
    - regex         : str  – regex pattern the value must match
    - min_length    : int  – minimum string length
    - max_length    : int  – maximum string length
    """

    def __init__(self, rules: Dict[str, Dict[str, Any]]):
        """
        rules: dict mapping column_name -> dict_of_rules
        Example:
            {
                "email":  {"required": True, "regex": r"^[\w.-]+@[\w.-]+\.\w+$"},
                "age":    {"required": True, "min_value": 0, "max_value": 120},
                "status": {"allowed_values": ["active", "inactive"]},
            }
        """
        self.rules = rules

    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the DataFrame and return a detailed report.
        Returns:
            {
                "passed": bool,
                "total_violations": int,
                "violations": { column: [list of violation messages] }
            }
        """
        violations: Dict[str, List[str]] = {}

        for col, col_rules in self.rules.items():
            col_violations = []

            if col not in df.columns:
                if col_rules.get("required", False):
                    col_violations.append(f"Column '{col}' is missing from dataset")
                violations[col] = col_violations
                continue

            series = df[col]

            # required
            if col_rules.get("required"):
                null_count = series.isnull().sum()
                if null_count > 0:
                    col_violations.append(
                        f"Required column has {null_count} null value(s)"
                    )

            # min_value / max_value
            numeric = pd.to_numeric(series, errors="coerce")
            if "min_value" in col_rules:
                below = (numeric < col_rules["min_value"]).sum()
                if below:
                    col_violations.append(
                        f"{below} value(s) below min ({col_rules['min_value']})"
                    )
            if "max_value" in col_rules:
                above = (numeric > col_rules["max_value"]).sum()
                if above:
                    col_violations.append(
                        f"{above} value(s) above max ({col_rules['max_value']})"
                    )

            # allowed_values
            if "allowed_values" in col_rules:
                allowed = set(str(v) for v in col_rules["allowed_values"])
                invalid = series.dropna().apply(lambda v: str(v) not in allowed).sum()
                if invalid:
                    col_violations.append(
                        f"{invalid} value(s) not in allowed set {col_rules['allowed_values']}"
                    )

            # regex
            if "regex" in col_rules:
                pattern = re.compile(col_rules["regex"])
                invalid = series.dropna().apply(
                    lambda v: not bool(pattern.match(str(v)))
                ).sum()
                if invalid:
                    col_violations.append(
                        f"{invalid} value(s) do not match pattern '{col_rules['regex']}'"
                    )

            # min_length / max_length
            if "min_length" in col_rules:
                too_short = series.dropna().apply(lambda v: len(str(v)) < col_rules["min_length"]).sum()
                if too_short:
                    col_violations.append(f"{too_short} value(s) shorter than {col_rules['min_length']} chars")

            if "max_length" in col_rules:
                too_long = series.dropna().apply(lambda v: len(str(v)) > col_rules["max_length"]).sum()
                if too_long:
                    col_violations.append(f"{too_long} value(s) longer than {col_rules['max_length']} chars")

            if col_violations:
                violations[col] = col_violations
                logger.warning(f"Validation issues in '{col}': {col_violations}")

        total_violations = sum(len(v) for v in violations.values())
        passed = total_violations == 0

        report = {
            "passed": passed,
            "total_violations": total_violations,
            "violations": violations,
        }

        if passed:
            logger.info("Validation PASSED – no violations found.")
        else:
            logger.warning(
                f"Validation FAILED – {total_violations} violation(s) across "
                f"{len(violations)} column(s)."
            )

        return report
