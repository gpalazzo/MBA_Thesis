# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    area_prod_cliente_prm,
    custo_fte,
    custo_prod_fte,
    frota_clientes_prm,
    prod_laranja_sp_prm,
    producao_fte,
)


def prod_laranja_sp_pipeline() -> pipeline:

    _prod_laranja_sp_pipeline = pipeline(
        Pipeline([
            node(func=prod_laranja_sp_prm,
                inputs=["raw_prod_laranja_sp",
                        "params:producao_laranja_sp_params"],
                outputs="prm_prod_laranja_sp",
                name="run_prod_laranja_sp_prm"),
        ],
        tags=["prod_laranja_sp_pipeline"]))

    return _prod_laranja_sp_pipeline


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


def frota_clientes_pipeline() -> pipeline:

    _frota_clientes_pipeline = pipeline(
        Pipeline([
            node(func=frota_clientes_prm,
                inputs=["raw_frota_clientes",
                        "prm_clientes",
                        "params:clientes_params.cultura"],
                outputs="prm_frota_clientes",
                name="run_frota_clientes_prm"),
        ],
        tags=["frota_clientes_pipeline"]))

    return _frota_clientes_pipeline


def features_custo_prod_pipeline() -> pipeline:

    _features_custo_prod_pipeline = pipeline(
        Pipeline([
            node(func=producao_fte,
                inputs=["prm_area_prod_cliente",
                        "prm_prod_laranja_sp",
                        "prm_precos_laranja",
                        "label_spine"],
                outputs="fte_producao",
                name="run_producao_fte"),

            node(func=custo_fte,
                inputs=["prm_frota_clientes",
                        "prm_precos_diesel",
                        "label_spine",
                        "params:custo_ftes.consumo_medio_diesel_trator"],
                outputs="fte_custo",
                name="run_custo_fte"),

            node(func=custo_prod_fte,
                inputs=["fte_custo",
                        "fte_producao"],
                outputs="fte_custo_producao",
                name="run_custo_prod_fte")
        ],
        tags=["features_custo_prod_pipeline"]))

    return _features_custo_prod_pipeline
