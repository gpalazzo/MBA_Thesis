# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import yfinance_fte, yfinance_prm, yfinance_raw


def yfinance_pipeline() -> pipeline:

    _yfinance_pipeline = pipeline(
        Pipeline([
            node(func=yfinance_raw,
                inputs="params:yfinance_params",
                outputs="raw_usdbrl_yfinance",
                name="run_yfinance_raw"),

            node(func=yfinance_prm,
                inputs="raw_usdbrl_yfinance",
                outputs="prm_usdbrl_yfinance",
                name="run_yfinance_prm"),

            node(func=yfinance_fte,
                inputs=["prm_usdbrl_yfinance", "label_spine"],
                outputs="fte_usdbrl_yfinance",
                name="run_yfinance_fte")
        ],
        tags=["yfinance_pipeline"]))

    return _yfinance_pipeline
