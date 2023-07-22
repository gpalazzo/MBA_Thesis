# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import cria_master_table


def master_table_pipeline() -> pipeline:

    _master_table_pipeline = pipeline(
        Pipeline([
            node(func=cria_master_table,
                inputs=["label_spine", "fte_analise_fin", "fte_origem_receita_frota"],
                outputs="master_table",
                name="run_cria_master_table")
        ],
        tags=["master_table_pipeline"]))

    return _master_table_pipeline
