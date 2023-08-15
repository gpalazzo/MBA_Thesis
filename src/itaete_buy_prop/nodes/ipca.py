# -*- coding: utf-8 -*-
import pandas as pd

from itaete_buy_prop.utils import string_normalizer

BASE_JOIN_COLS = ["data_inferior", "data_alvo"]


def ipca_prm(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = [string_normalizer(col) for col in df.columns]

    df = df.rename(columns={"mes": "data"})
    df.loc[:, "data"] = df["data"].dt.date

    df.loc[:, "data_ref"] = df["data"].shift(-1)
    df = df.drop(columns=["data"])
    df_drop = df.dropna()

    assert df_drop.shape[0] == df.shape[0] - 1, "Dropou mais de 1 linha, revisar"
    return df_drop


def ipca_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    fte_df = pd.DataFrame()
    spine = spine[["data_inferior", "data_visita"]].drop_duplicates()

    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_visita"]):

        dfaux = df[df["data_ref"].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            ipca_medio = dfaux["inflacao"].mean()
            ipca_df = pd.DataFrame({"ipca_medio": ipca_medio}, index=[0])
            ipca_df.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, ipca_df])

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature ipca duplicada, revisar"

    return fte_df
