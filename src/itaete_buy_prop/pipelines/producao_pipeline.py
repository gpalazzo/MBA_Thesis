# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    area_prod_cliente_prm,
    prod_laranja_sp_prm,
    producao_fte,
)


def producao_laranja_sp_pipeline() -> pipeline:

    _producao_laranja_sp_pipeline = pipeline(
        Pipeline([
            node(func=prod_laranja_sp_prm,
                inputs=["raw_prod_laranja_sp",
                        "params:producao_laranja_sp_params"],
                outputs="prm_prod_laranja_sp",
                name="run_prod_laranja_sp_prm"),
        ],
        tags=["prod_laranja_sp_pipeline"]))

    return _producao_laranja_sp_pipeline


def area_prod_cliente_pipeline() -> pipeline:

    _area_prod_cliente_pipeline = pipeline(
        Pipeline([
            node(func=area_prod_cliente_prm,
                inputs=["raw_area_prod_cliente",
                        "prm_clientes",
                        "params:clientes_params.cultura"],
                outputs="prm_area_prod_cliente",
                name="run_area_prod_cliente_prm"),
        ],
        tags=["area_prod_cliente_pipeline"]))

    return _area_prod_cliente_pipeline


def features_producao_pipeline() -> pipeline:

    _features_producao_pipeline = pipeline(
        Pipeline([
            node(func=producao_fte,
                inputs=["prm_area_prod_cliente",
                        "prm_prod_laranja_sp",
                        "prm_precos_laranja",
                        "label_spine"],
                outputs="fte_producao",
                name="run_producao_fte"),
        ],
        tags=["features_producao_pipeline"]))

    return _features_producao_pipeline
