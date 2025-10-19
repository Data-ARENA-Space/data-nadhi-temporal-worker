import copy
from typing import Any

from .general import PipelineUtils


class RuleEngine:
    def __init__(self, data):
        self.data = data

    def get_value_from_data(self, key: str) -> Any:
        """Extract value from data using keychain or direct key"""
        if PipelineUtils._is_keychain_str(key):
            keys = PipelineUtils.get_keychain_from_str(key)
            current = copy.deepcopy(self.data)
            for k in keys:
                if isinstance(current, dict) and k in current:
                    current = current[k]
                else:
                    return None
            return current
        return self.data.get(key)

    def evaluate_check(self, check: dict) -> bool:
        """Evaluate a single check condition"""
        key = check.get("key")
        value = check.get("value")
        operator = check.get("operator")

        # Get actual values from data
        key_value = self.get_value_from_data(key)

        # Handle value - it could be a keychain reference or literal value
        if value and isinstance(value, str) and PipelineUtils._is_keychain_str(value):
            compare_value = self.get_value_from_data(value)
        else:
            compare_value = value

        if operator == "exists":
            return key_value is not None
        if operator == "not_exists":
            return key_value is None
        # Perform comparison based on operator
        if operator == "et":  # equal to
            return key_value == compare_value
        if operator == "gte":  # greater than or equal
            return (
                key_value is not None
                and compare_value is not None
                and key_value >= compare_value
            )
        if operator == "gt":  # greater than
            return (
                key_value is not None
                and compare_value is not None
                and key_value > compare_value
            )
        if operator == "lte":  # less than or equal
            return (
                key_value is not None
                and compare_value is not None
                and key_value <= compare_value
            )
        if operator == "lt":  # less than
            return (
                key_value is not None
                and compare_value is not None
                and key_value < compare_value
            )
        if operator == "ne":  # not equal
            return key_value != compare_value
        return False

    def evaluate_filter(self, filter: dict) -> bool:
        """Evaluate filter conditions with AND/OR logic"""
        if "and" in filter:
            if len(filter["and"]) == 0:
                return True
            # All conditions must be true
            result = True
            for condition in filter["and"]:
                result = result and self.evaluate_filter(condition)
            return result
        if "or" in filter:
            if len(filter["or"]) == 0:
                return True
            result = False
            # At least one condition must be true
            for condition in filter["or"]:
                result = result or self.evaluate_filter(condition)
            return result
        if "check" in filter:
            # Single check condition
            return self.evaluate_check(filter["check"])
        return False
