import copy
from typing import Any


def get_keychain_from_str(keychain_str: str) -> Any:
    return keychain_str[2:].split(".")


def _is_keychain_str(keychain_str: str) -> bool:
    return type(keychain_str) is str and keychain_str.startswith("$.")


def get_value_from_data(data: dict, key: str) -> Any:
    """Extract value from data using keychain or direct key"""
    if _is_keychain_str(key):
        keys = get_keychain_from_str(key)
        current = copy.deepcopy(data)
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return None
        return current
    return data.get(key)
