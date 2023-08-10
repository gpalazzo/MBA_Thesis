# -*- coding: utf-8 -*-
import math
from typing import Any, Dict

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.feature_selection import VarianceThreshold
from ta import add_all_ta_features

from itaete_buy_prop.utils import (
    define_janela_datas,
    filtra_data_janelas,
    seleciona_janelas,
)

BASE_JOIN_COLS = ["data_inferior", "data_alvo"]


def yfinance_raw(params: Dict[str, Any]) -> pd.DataFrame:

    df = yf.download(tickers=params["tickers"],
                     start=params["start_date"],
                     end=params["end_date"]) \
        .reset_index()

    return df


def yfinance_prm(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = df.columns.str.lower()
    df.loc[:, "date"] = df["date"].dt.date
    df = df \
            .drop(columns=["adj close"]) \
            .rename(columns={"date": "timestamp"})

    return df


def yfinance_fte(df: pd.DataFrame,
                spine: pd.DataFrame,
                params: Dict[str, int],
                spine_lookback_days: int) -> pd.DataFrame:

    fte_df = pd.DataFrame()
    spine = spine[["data_inferior", "data_visita"]].drop_duplicates()

    # quantidade de janelas para agregar as features
    tamanho_janela_dias = params["aggregate_window_days"]
    qtd_janelas = math.floor(spine_lookback_days / tamanho_janela_dias)

    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_visita"]):

        dfaux = df[df["timestamp"].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            techn_ftes_df = _build_technical_ftes(df=dfaux)
            biz_ftes_df = _build_biz_ftes(df=dfaux)

            fteaux_df = techn_ftes_df.merge(biz_ftes_df, on="timestamp", how="inner")
            assert fteaux_df.shape[0] == techn_ftes_df.shape[0] == biz_ftes_df.shape[0], \
                "Número errado de linhas após join, revisar"

            fteaux_df = fteaux_df.set_index("timestamp").add_prefix("usdbrl_").reset_index()

            define_janelas = define_janela_datas(data_inicio=data_inferior,
                                                qtd_janelas=qtd_janelas,
                                                tamanho_janela_dias=tamanho_janela_dias)
            define_janelas = seleciona_janelas(janelas=define_janelas, slc_janelas_numero=[1, 12])

            fteaux_df = filtra_data_janelas(df=fteaux_df,
                                        date_col_name="timestamp",
                                        janelas=define_janelas,
                                        tipo_janela="right")

            fteaux_df.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fteaux_df])

    fte_df = fte_df.fillna(0)

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature yfinance duplicada, revisar"

    return fte_df


def _aplica_threshold_var(df: pd.DataFrame) -> pd.DataFrame:

    df = df.set_index("timestamp").fillna(0)
    thresholder = VarianceThreshold(threshold=5)

    selector = thresholder.fit(df)
    df = df[df.columns[selector.get_support(indices=True)]]

    df = df.reset_index()

    return df


def _build_technical_ftes(df: pd.DataFrame) -> pd.DataFrame:

    ACCEPTED_COLS = ("timestamp", "volatility", "trend", "momentum")

    fteaux = add_all_ta_features(df, open="open", high="high", low="low", close="close", volume="volume")

    fteaux = fteaux[[col for col in fteaux.columns if col.startswith(ACCEPTED_COLS)]]
    fteaux = _aplica_threshold_var(df=fteaux)

    return fteaux


def _build_biz_ftes(df: pd.DataFrame) -> pd.DataFrame:

    def _build_logreturns(col: pd.Series) -> pd.Series:
        return np.log(1 + col)

    df = df[["timestamp", "close", "low", "high"]]

    df = df.set_index("timestamp").sort_index()
    df = df.pct_change().fillna(0)
    df = df.apply(_build_logreturns, axis=1)

    df.loc[:, "minMaxDiff"] = df["high"] - df["low"]
    df = df[["close", "minMaxDiff"]].cumsum()
    df = df.reset_index().rename(columns={"close": "pxclose_cumsum",
                                          "minMaxDiff": "pxhighlow_diff_cumsum"})

    return df
