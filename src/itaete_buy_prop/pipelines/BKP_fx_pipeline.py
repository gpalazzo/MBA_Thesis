# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import fx_fte, fx_prm


def fx_pipeline() -> pipeline:

    _fx_pipeline = pipeline(
        Pipeline([
            node(func=fx_prm,
                inputs="raw_usdbrl_fx",
                outputs="prm_usdbrl_fx",
                name="run_fx_prm"),

            node(func=fx_fte,
                inputs=["prm_usdbrl_fx", "label_spine",
                        "params:fx_params", "params:spine_params.dt_fat_lookback_window"],
                outputs="fte_usdbrl_fx",
                name="run_fx_fte")
        ],
        tags=["fx_pipeline"]))

    return _fx_pipeline
