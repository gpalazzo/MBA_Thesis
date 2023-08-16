# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    precos_trator_cxlaranja_fte,
    precos_trator_cxlaranja_prm,
    precos_trator_potencia_fte,
    precos_trator_potencia_prm,
)


def precos_trator_potencia_pipeline() -> pipeline:

    _precos_trator_potencia_pipeline = pipeline(
        Pipeline([
            node(func=precos_trator_potencia_prm,
                inputs=["raw_preco_trator_potencia",
                        "params:trator_potencia_params"],
                outputs="prm_preco_trator_potencia",
                name="run_precos_trator_potencia_prm"),

            node(func=precos_trator_potencia_fte,
                inputs=["prm_preco_trator_potencia",
                        "label_spine",
                        "params:precos_trator_potencia_params",
                        "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_preco_trator_potencia",
                name="run_precos_trator_potencia_fte")
        ],
        tags=["precos_trator_potencia_pipeline"]))

    return _precos_trator_potencia_pipeline


def precos_trator_cxlaranja_pipeline() -> pipeline:

    _precos_trator_cxlaranja_pipeline = pipeline(
        Pipeline([
            node(func=precos_trator_cxlaranja_prm,
                inputs=["raw_preco_trator_cxlaranja",
                        "params:trator_potencia_params"],
                outputs="prm_preco_trator_cxlaranja",
                name="run_precos_trator_cxlaranja_prm"),

            node(func=precos_trator_cxlaranja_fte,
                inputs=["prm_preco_trator_cxlaranja",
                        "label_spine",
                        "params:precos_trator_cxlaranja_params",
                        "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_preco_trator_cxlaranja",
                name="run_precos_trator_cxlaranja_fte")
        ],
        tags=["precos_trator_cxlaranja_pipeline"]))

    return _precos_trator_cxlaranja_pipeline
