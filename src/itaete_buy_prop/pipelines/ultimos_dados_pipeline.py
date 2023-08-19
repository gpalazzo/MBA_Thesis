# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import ultimos_dados_fte, ultimos_dados_prm


def ultimos_dados_pipeline() -> pipeline:

    _ultimos_dados_pipeline = pipeline(
        Pipeline([
            node(func=ultimos_dados_prm,
                inputs="raw_ultimos_dados",
                outputs="prm_ultimos_dados",
                name="run_ultimos_dados_prm"),

            node(func=ultimos_dados_fte,
                inputs=["prm_ultimos_dados",
                        "label_spine",
                        "params:ultimos_dados_params",
                        "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_ultimos_dados",
                name="run_ultimos_dados_fte")
        ],
        tags=["ultimos_dados_pipeline"]))

    return _ultimos_dados_pipeline
