# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import precos_laranja_fte, precos_laranja_prm


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
