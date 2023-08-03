# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import quali_clientes_crm_fte, quali_clientes_crm_prm


def quali_clientes_crm_pipeline() -> pipeline:

    _quali_clientes_crm_pipeline = pipeline(
        Pipeline([
            node(func=quali_clientes_crm_prm,
                inputs=["raw_crm_bi_quali_clientes", "prm_origem_receita_frota"],
                outputs="prm_quali_clientes",
                name="run_quali_clientes_prm"),

            node(func=quali_clientes_crm_fte,
                inputs="prm_quali_clientes",
                outputs="fte_quali_clientes",
                name="run_quali_clientes_fte")
        ],
        tags=["quali_clientes_pipeline"]))

    return _quali_clientes_crm_pipeline
