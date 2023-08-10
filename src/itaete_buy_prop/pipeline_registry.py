# -*- coding: utf-8 -*-
"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from itaete_buy_prop.pipelines import (
    analise_fin_pipeline,
    cen_visitas_pipeline,
    clientes_pipeline,
    funil_vendas_pipeline,
    logreg_pipeline,
    master_table_pipeline,
    precos_diesel_pipeline,
    spine_pipeline,
    yfinance_pipeline,
)


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    return {"__default__": pipeline([clientes_pipeline() +
                                     analise_fin_pipeline() +
                                     yfinance_pipeline() +
                                     funil_vendas_pipeline() +
                                     cen_visitas_pipeline() +
                                     precos_diesel_pipeline() +
                                     spine_pipeline() +
                                     master_table_pipeline() +
                                     logreg_pipeline()])}
