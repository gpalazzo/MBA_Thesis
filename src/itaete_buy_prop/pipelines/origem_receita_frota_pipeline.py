# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import origem_receita_frota_fte, origem_receita_frota_prm


def origem_receita_frota_pipeline() -> pipeline:

    _origem_receita_frota_pipeline = pipeline(
        Pipeline([
            node(func=origem_receita_frota_prm,
                inputs=["raw_crm_bi_origem_receita_frota", "params:clientes_params"],
                outputs="prm_origem_receita_frota",
                name="run_origem_receita_frota_prm"),

            node(func=origem_receita_frota_fte,
                inputs=["prm_origem_receita_frota", "label_spine"],
                outputs="fte_origem_receita_frota",
                name="run_origem_receita_frota_fte")
        ],
        tags=["origem_receita_frota_pipeline"]))

    return _origem_receita_frota_pipeline
