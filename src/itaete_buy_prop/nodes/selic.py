# -*- coding: utf-8 -*-
import datetime
import math
from typing import Dict

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from itaete_buy_prop.utils import string_normalizer

BASE_JOIN_COLS = ["data_inferior", "data_alvo"]


def selic_prm(df: pd.DataFrame, params: Dict[str, int]) -> pd.DataFrame:

    df.columns = [string_normalizer(col) for col in df.columns]
    df.loc[:, "data"] = df["data"].dt.date
    df = df.rename(columns={"selic": "selic_ano"})

    df.loc[:, "selic_transformada"] = df["selic_ano"].apply(_transforma_selic_ano, params=params)
    df.loc[:, "data_ultimo_dia_mes"] = df["data"].apply(_pega_ultimo_dia_mes)
    df.loc[:, "selic_logret"] = np.log(1 + df["selic_transformada"])

    df_grp = df.groupby("data_ultimo_dia_mes")["selic_logret"].sum().reset_index()
    df_grp.loc[:, "selic_pctchg"] = np.exp(df_grp["selic_logret"]) - 1

    df_grp = df_grp.drop(columns=["selic_logret"])

    return df_grp


def selic_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    fte_df = pd.DataFrame()
    spine = spine[["data_inferior", "data_visita"]].drop_duplicates()

    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_visita"]):

        dfaux = df[df["data_ultimo_dia_mes"].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            selic_media = dfaux["selic_pctchg"].mean()
            selic_df = pd.DataFrame({"selic_media": selic_media}, index=[0])
            selic_df.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, selic_df])

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature ipca duplicada, revisar"

    return fte_df


def _transforma_selic_ano(valor_ipca: float, params: Dict[str, int]) -> float:
    return math.pow(1 + valor_ipca,
                    params["qtd_dias_uteis_target"] / params["qtd_dias_uteis_ano"]) - 1


def _pega_ultimo_dia_mes(date: datetime.date) -> datetime.date:
    return date + relativedelta(day=31)
