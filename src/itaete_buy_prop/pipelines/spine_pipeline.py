# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import spine_labeling, spine_preprocessing


def spine_pipeline() -> pipeline:

    _spine_pipeline = pipeline(
        Pipeline([
            node(func=spine_preprocessing,
                inputs=["prm_cen_visitas",
                        "params:spine_params"],
                outputs="preprocessing_spine",
                name="run_spine_preprocessing"),

            node(func=spine_labeling,
                inputs=["preprocessing_spine",
                        "params:spine_params.max_diff_dias_visita"],
                outputs="label_spine",
                name="run_spine_label")
        ],
        tags=["spine_pipeline"]))

    return _spine_pipeline
