import copy

from temporalio import activity

from utils.exceptions import FilterEvaluationError, TransformationError
from utils.logger import log_debug

from .rule_engine import RuleEngine
from .transformations import apply_transformation


@activity.defn
async def transform(node_config, input_data, ctx: dict):
    log_debug(
        "Applying transformation",
        ctx,
        {"transformation_fn": node_config.get("transformation_fn")},
    )
    try:
        transform_fn_key = node_config.get("transformation_fn")
        transformation_params = node_config.get("transformation_params", {})

        output_data = apply_transformation(
            transform_fn_key, transformation_params, copy.deepcopy(input_data)
        )

        return node_config.get("next", []), output_data
    except (ValueError, KeyError, TypeError) as exc:
        # These are data/config errors - wrap in our custom exception
        raise TransformationError(f"Transformation failed: {str(exc)}") from exc
    except Exception:
        # Other exceptions could be retryable (e.g., network issues)
        # Let them propagate for retry
        raise


@activity.defn
async def filters(node_config, input_data, ctx: dict):
    log_debug("Evaluating filters", ctx)
    try:
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
                filter_condition = filter_config["filter"]
                if filter_condition and RuleEngine(input_data).evaluate_filter(
                    filter_condition
                ):
                    # Add next nodes for this filter to queue
                    if filter_config.get("next"):
                        for nd in filter_config.get("next", []):
                            next.append(nd)

        return next, copy.deepcopy(input_data)
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        # These are data/config errors - wrap in our custom exception
        raise FilterEvaluationError(f"Filter evaluation failed: {str(exc)}") from exc
    except Exception:
        # Other exceptions could be retryable
        raise
