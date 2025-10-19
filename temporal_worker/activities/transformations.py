import copy

from temporalio import activity

from ..utils.rule_engine import RuleEngine
from ..utils.transformations import apply_transformation


@activity.defn
async def transform(node_config, input_data):
    transform_fn_key = node_config.get("transformation_fn")
    transformation_params = node_config.get("transformation_params", {})

    output_data = apply_transformation(
        transform_fn_key, transformation_params, copy.deepcopy(input_data)
    )

    return node_config.get("next", []), output_data


@activity.defn
async def filters(node_config, input_data):
    next = []
    filters = node_config.get("filters", {})

    # Evaluate each filter independently (non-exclusive)
    for _, filter_config in filters.items():
        if "filter" not in filter_config:
            if filter_config.get("next") is not None:
                for nd in filter_config.get("next", []):
                    next.append(nd)
        else:
            # Evaluate specific filter conditions
            filter_condition = filter_config.get("filter")
            if filter_condition and RuleEngine(input_data).evaluate_filter(
                filter_condition
            ):
                # Add next nodes for this filter to queue
                if filter_config.get("next"):
                    for nd in filter_config.get("next", []):
                        next.append(nd)

    return next, copy.deepcopy(input_data)


@activity.defn
async def end(node_config, input_data):
    print(
        "Ending workflow: Message: ",
        node_config.get("message", "No message"),
        input_data,
    )
    return [], copy.deepcopy(input_data)
