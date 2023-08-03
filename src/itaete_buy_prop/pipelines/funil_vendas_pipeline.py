# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import funil_vendas_prm


def funil_vendas_pipeline() -> pipeline:

    _funil_vendas_pipeline = pipeline(
        Pipeline([
            node(func=funil_vendas_prm,
                inputs=["raw_crm_bi_funil_vendas",
                        "prm_clientes",
                        "params:funil_vendas_params"],
                outputs="prm_funil_vendas",
                name="run_funil_vendas_prm")
        ],
        tags=["funil_vendas_pipeline"]))

    return _funil_vendas_pipeline
