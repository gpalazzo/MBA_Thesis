# -*- coding: utf-8 -*-

# from kedro.pipeline import Pipeline, node, pipeline

# from itaete_buy_prop.nodes import logreg_model_fit


# def logreg_pipeline() -> pipeline:

#     _logreg_pipeline = pipeline(
#         Pipeline([
#             node(func=logreg_model_fit,
#                 inputs=["master_table", "fte_analise_fin", "fte_origem_receita_frota"],
#                 outputs="master_table",
#                 name="run_logreg_model_fit")
#         ],
#         tags=["logreg_pipeline"]))

#     return _logreg_pipeline
