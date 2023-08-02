# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import clientes_prm


def clientes_pipeline() -> pipeline:

    _clientes_pipeline = pipeline(
        Pipeline([
            node(func=clientes_prm,
                inputs=["raw_crm_bi_origem_receita_frota",
                        "raw_crm_bi_analise_financeira",
                        "params:clientes_params"],
                outputs="prm_clientes",
                name="run_clientes_prm")
        ],
        tags=["clientes_pipeline"]))

    return _clientes_pipeline
