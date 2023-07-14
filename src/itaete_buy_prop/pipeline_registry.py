"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline
from itaete_buy_prop.pipelines import spine_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {"__default__": pipeline([spine_pipeline()])}
