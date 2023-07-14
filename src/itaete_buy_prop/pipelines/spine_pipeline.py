# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import spine_labeling, spine_preprocessing, spine_prm


def spine_pipeline() -> pipeline:

    _spine_pipeline = pipeline(
        Pipeline([
            node(func=spine_prm,
                inputs="raw_crm_bi_analise_financeira",
                outputs="prm_spine",
                name="run_spine_prm"),

            node(func=spine_preprocessing,
                inputs=["prm_spine", "prm_quali_clientes", "params:spine_params"],
                outputs="preprocessing_spine",
                name="run_spine_preprocessing"),

            node(func=spine_labeling,
                inputs="preprocessing_spine",
                outputs="label_spine",
                name="run_spine_label")
        ],
        tags=["spine_pipeline"]))

    return _spine_pipeline
