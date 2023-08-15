# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    precos_diesel_fte,
    precos_diesel_prm,
    precos_laranja_fte,
    precos_laranja_prm,
)


def precos_diesel_pipeline() -> pipeline:

    _precos_diesel_pipeline = pipeline(
        Pipeline([
            node(func=precos_diesel_prm,
                inputs="raw_precos_diesel",
                outputs="prm_precos_diesel",
                name="run_precos_diesel_prm"),

            node(func=precos_diesel_fte,
                inputs=["prm_precos_diesel",
                        "label_spine",
                        "params:precos_diesel_params",
                        "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_precos_diesel",
                name="run_precos_diesel_fte")
        ],
        tags=["precos_diesel_pipeline"]))

    return _precos_diesel_pipeline


def precos_laranja_pipeline() -> pipeline:

    _precos_laranja_pipeline = pipeline(
        Pipeline([
            node(func=precos_laranja_prm,
                inputs="raw_precos_laranja",
                outputs="prm_precos_laranja",
                name="run_precos_laranja_prm"),

            node(func=precos_laranja_fte,
                inputs=["prm_precos_laranja",
                        "label_spine",
                        "params:precos_laranja_params",
                        "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_precos_laranja",
                name="run_precos_laranja_fte")
        ],
        tags=["precos_laranja_pipeline"]))

    return _precos_laranja_pipeline
