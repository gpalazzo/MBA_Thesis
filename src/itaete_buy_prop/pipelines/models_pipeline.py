# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import (
    logreg_model_fit,
    logreg_model_predict,
    logreg_model_relatorio,
)


def logreg_pipeline() -> pipeline:

    _logreg_pipeline = pipeline(
        Pipeline([

            node(func=logreg_model_fit,
                inputs=["master_table_treino_ftes", "master_table_treino_tgt",
                        "params:logreg_params"],
                outputs="logreg_fitted_model",
                name="run_logreg_model_fit"),

            node(func=logreg_model_predict,
                inputs=["logreg_fitted_model", "master_table_teste_ftes"],
                outputs="logreg_model_predict",
                name="run_logreg_model_predict"),

            node(func=logreg_model_relatorio,
                inputs=["logreg_fitted_model",
                        "master_table_teste_ftes", "master_table_teste_tgt",
                        "logreg_model_predict"],
                outputs="logreg_model_relatorio",
                name="run_logreg_model_relatorio")
        ],
        tags=["logreg_pipeline"]))

    return _logreg_pipeline
