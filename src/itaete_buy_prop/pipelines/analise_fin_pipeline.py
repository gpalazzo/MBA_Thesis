# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import analise_fin_fte, analise_fin_prm


def analise_fin_pipeline() -> pipeline:

    _analise_fin_pipeline = pipeline(
        Pipeline([
            node(func=analise_fin_prm,
                inputs=["raw_crm_bi_analise_financeira", "prm_origem_receita_frota"],
                outputs="prm_analise_fin",
                name="run_analise_fin_prm"),

            node(func=analise_fin_fte,
                inputs=["prm_analise_fin", "label_spine"],
                outputs="fte_analise_fin",
                name="run_analise_fin_fte")
        ],
        tags=["analise_fin_pipeline"]))

    return _analise_fin_pipeline
