# -*- coding: utf-8 -*-
"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from itaete_buy_prop.pipelines import (
    analise_fin_pipeline,
    clientes_pipeline,
    logreg_pipeline,
    master_table_pipeline,
    quali_clientes_crm_pipeline,
    spine_pipeline,
)


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {"__default__": pipeline([spine_pipeline() +
                                     quali_clientes_crm_pipeline() +
                                     clientes_pipeline() +
                                     analise_fin_pipeline() +
                                     master_table_pipeline() +
                                     logreg_pipeline()])}
