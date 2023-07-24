# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import cria_master_table, mt_split_treino_teste


def master_table_pipeline() -> pipeline:

    _master_table_pipeline = pipeline(
        Pipeline([
            node(func=cria_master_table,
                inputs=["label_spine", "fte_analise_fin", "fte_origem_receita_frota"],
                outputs="master_table",
                name="run_cria_master_table"),

            node(func=mt_split_treino_teste,
                inputs=["master_table", "params:master_table_params"],
                outputs=["master_table_treino_ftes", "master_table_treino_tgt",
                         "master_table_teste_ftes", "master_table_teste_tgt"],
                name="run_mt_split_treino_teste")
        ],
        tags=["master_table_pipeline"]))

    return _master_table_pipeline
