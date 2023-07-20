# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import quali_clientes_prm


def quali_clientes_pipeline() -> pipeline:

    _quali_clientes_pipeline = pipeline(
        Pipeline([
            node(func=quali_clientes_prm,
                inputs=["raw_crm_bi_quali_clientes", "prm_origem_receita_frota"],
                outputs="prm_quali_clientes",
                name="run_quali_clientes_prm")
        ],
        tags=["quali_clientes_pipeline"]))

    return _quali_clientes_pipeline
