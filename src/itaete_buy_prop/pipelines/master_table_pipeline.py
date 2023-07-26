# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    cria_master_table,
    mt_balanceia_classes,
    mt_remove_ftes_multic,
    mt_seleciona_features,
    mt_split_treino_teste,
)


def master_table_pipeline() -> pipeline:

    _master_table_pipeline = pipeline(
        Pipeline([
            node(func=cria_master_table,
                inputs=["label_spine", "params:master_table_params",
                        "fte_analise_fin", "fte_origem_receita_frota"],
                outputs="master_table",
                name="run_cria_master_table"),

            node(func=mt_balanceia_classes,
                inputs=["master_table", "params:master_table_params"],
                outputs="master_table_balanceada",
                name="run_mt_balanceia_classes"),

            node(func=mt_remove_ftes_multic,
                inputs=["master_table_balanceada", "params:master_table_params"],
                outputs=["master_table_remove_ftes_multic", "master_table_multic_vif_values"],
                name="run_mt_remove_ftes_multic"),

            node(func=mt_split_treino_teste,
                inputs=["master_table_remove_ftes_multic", "params:master_table_params"],
                outputs=["master_table_treino_ftes_tmp", "master_table_treino_tgt",
                         "master_table_teste_ftes_tmp", "master_table_teste_tgt"],
                name="run_mt_split_treino_teste"),

            node(func=mt_seleciona_features,
                inputs=["master_table_treino_ftes_tmp", "master_table_treino_tgt",
                        "master_table_teste_ftes_tmp",
                        "params:master_table_params"],
                outputs=["master_table_treino_ftes",
                         "master_table_teste_ftes",
                         "master_table_fte_slc_importancias"],
                name="run_mt_seleciona_features"),
        ],
        tags=["master_table_pipeline"]))

    return _master_table_pipeline
