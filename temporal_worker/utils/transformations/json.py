import copy

from ..general import PipelineUtils


class JSONTransformation:
    @staticmethod
    def remove_key(data: dict, transformation_params: dict) -> dict:
        key = transformation_params.get("key")
        data_copy = copy.deepcopy(data)

        if PipelineUtils._is_keychain_str(key):
            keys = PipelineUtils.get_keychain_from_str(key)
            # Navigate to the parent of the key to be deleted
            current = data_copy
            for k in keys[:-1]:  # All keys except the last one
                if k in current and isinstance(current[k], dict):
                    current = current[k]
                else:
                    # Key path doesn't exist, nothing to remove
                    return data_copy

            # Remove the final key if it exists
            final_key = keys[-1]
            if final_key in current:
                del current[final_key]
        else:
            # Simple key deletion at root level
            if key in data_copy:
                del data_copy[key]

        return data_copy

    @staticmethod
    def add_key(data: dict, transformation_params: dict) -> dict:
        key = transformation_params.get("key")
        value = transformation_params.get("value")

        if PipelineUtils._is_keychain_str(key):
            keys = PipelineUtils.get_keychain_from_str(key)
            current = data
            for k in keys[:-1]:
                if k not in current or type(current[k]) is not dict:
                    current[k] = {}
                current = current[k]
            current[keys[-1]] = value
        else:
            data[key] = value
        return data
