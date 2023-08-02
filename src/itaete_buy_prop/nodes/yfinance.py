# -*- coding: utf-8 -*-
from typing import Any, Dict

import pandas as pd
import yfinance as yf
from ta import add_all_ta_features


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


def yfinance_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    fte_df = pd.DataFrame()

    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_faturamento_nova"]):

        dfaux = df[df["timestamp"].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            fteaux = df = add_all_ta_features(dfaux, open="Open", high="High", low="Low", close="Close", volume="Volume")
            fteaux.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fteaux])
