# -*- coding: utf-8 -*-
"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from itaete_buy_prop.pipelines import (
    analise_fin_pipeline,
    area_prod_cliente_pipeline,
    cen_visitas_pipeline,
    clientes_pipeline,
    features_custo_prod_pipeline,
    frota_clientes_pipeline,
    funil_vendas_pipeline,
    ipca_pipeline,
    logreg_pipeline,
    master_table_pipeline,
    precos_diesel_pipeline,
    precos_laranja_pipeline,
    precos_trator_cxlaranja_pipeline,
    precos_trator_potencia_pipeline,
    prod_laranja_sp_pipeline,
    selic_pipeline,
    spine_pipeline,
    ultimos_dados_pipeline,
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
                                     precos_laranja_pipeline() +
                                     ipca_pipeline() +
                                     selic_pipeline() +
                                     precos_trator_potencia_pipeline() +
                                     precos_trator_cxlaranja_pipeline() +
                                     prod_laranja_sp_pipeline() +
                                     area_prod_cliente_pipeline() +
                                     frota_clientes_pipeline() +
                                     features_custo_prod_pipeline() +
                                     ultimos_dados_pipeline() +
                                     spine_pipeline() +
                                     master_table_pipeline() +
                                     logreg_pipeline()])}
