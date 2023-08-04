# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    cria_master_table,
    mt_remove_ftes_multic,
    mt_seleciona_features,
    mt_split_treino_teste,
)
from itaete_buy_prop.settings import MASTER_TABLE_DATASETS


def master_table_pipeline() -> pipeline:

    _master_table_pipeline = pipeline(
        Pipeline([
            node(func=cria_master_table,
                inputs=["label_spine", "params:master_table_params"] + MASTER_TABLE_DATASETS,
                outputs="master_table",
                name="run_cria_master_table"),

            node(func=mt_remove_ftes_multic,
                inputs=["master_table", "params:master_table_params"],
                outputs=["master_table_remove_ftes_multic", "master_table_multic_vif_values"],
                name="run_mt_remove_ftes_multic"),

            node(func=mt_split_treino_teste,
                inputs=["master_table_remove_ftes_multic", "params:master_table_params"],
                outputs=["master_table_treino", "master_table_teste"],
                name="run_mt_split_treino_teste"),

            node(func=mt_seleciona_features,
                inputs=["master_table_treino",
                        "master_table_teste",
                        "params:master_table_params"],
                outputs=["master_table_treino_ftes",
                         "master_table_treino_tgt",
                         "master_table_teste_ftes",
                         "master_table_teste_tgt",
                         "master_table_fte_slc_importancias"],
                name="run_mt_seleciona_features"),
        ],
        tags=["master_table_pipeline"]))

    return _master_table_pipeline
