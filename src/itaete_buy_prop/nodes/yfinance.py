# -*- coding: utf-8 -*-
import math
from typing import Any, Dict

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
                     start=params["start_date"]) \
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
    ACCEPTED_COLS = ("timestamp", "volatility", "trend", "momentum")

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
            try:
                fteaux = add_all_ta_features(dfaux, open="open", high="high", low="low", close="close", volume="volume")
            except Exception as e:
                continue

            fteaux = fteaux[[col for col in fteaux.columns if col.startswith(ACCEPTED_COLS)]]
            fteaux = _aplica_threshold_var(df=fteaux)

            define_janelas = define_janela_datas(data_inicio=data_inferior,
                                                qtd_janelas=qtd_janelas,
                                                tamanho_janela_dias=tamanho_janela_dias)
            define_janelas = seleciona_janelas(janelas=define_janelas, slc_janelas_numero=[1, 12])

            fteaux = filtra_data_janelas(df=fteaux,
                                        date_col_name="timestamp",
                                        janelas=define_janelas,
                                        tipo_janela="right")

            fteaux.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fteaux])

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
