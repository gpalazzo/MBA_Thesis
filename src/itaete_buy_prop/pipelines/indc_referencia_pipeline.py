# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import ipca_fte, ipca_prm, selic_fte, selic_prm


def ipca_pipeline() -> pipeline:

    _ipca_pipeline = pipeline(
        Pipeline([
            node(func=ipca_prm,
                inputs="raw_ipca",
                outputs="prm_ipca",
                name="run_ipca_prm"),

            node(func=ipca_fte,
                inputs=["prm_ipca",
                        "label_spine"],
                outputs="fte_ipca",
                name="run_ipca_fte")
        ],
        tags=["ipca_pipeline"]))

    return _ipca_pipeline


def selic_pipeline() -> pipeline:

    _selic_pipeline = pipeline(
        Pipeline([
            node(func=selic_prm,
                inputs=["raw_selic",
                        "params:selic_params"],
                outputs="prm_selic",
                name="run_selic_prm"),

            node(func=selic_fte,
                inputs=["prm_selic",
                        "label_spine"],
                outputs="fte_selic",
                name="run_selic_fte")
        ],
        tags=["selic_pipeline"]))

    return _selic_pipeline
