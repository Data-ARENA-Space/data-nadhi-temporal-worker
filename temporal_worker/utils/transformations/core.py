from .json import JSONTransformation


class Transformation:
    JSON = JSONTransformation


def apply_transformation(
    activity: str, transformation_params: dict, input_data: dict
) -> dict:
    # Remove "Transformation." prefix if it exists
    if activity.startswith("Transformation."):
        activity_chain = activity[15:]  # Remove "Transformation." (15 characters)
    else:
        activity_chain = activity
    activity_chain = activity_chain.split(".")
    activity_fn = Transformation
    for part in activity_chain:
        activity_fn = getattr(activity_fn, part, None)
        if activity_fn is None:
            print(f"Error: Transformation function not found: {activity}")
            return input_data
    return activity_fn(input_data, transformation_params)
    return input_data
    # return activity_fn(input_data, transformation_params)


if __name__ == "__main__":
    print(getattr(Transformation, "JSON", None))
