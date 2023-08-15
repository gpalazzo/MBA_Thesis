# -*- coding: utf-8 -*-
import math
from functools import reduce
from typing import Dict

import numpy as np
import pandas as pd

from itaete_buy_prop.utils import (
    cria_indices_oscilacao,
    define_janela_datas,
    filtra_data_janelas,
    seleciona_janelas,
    string_normalizer,
)

BASE_JOIN_COLS = ["data_inferior", "data_alvo"]
DATE_COL = "data"


def precos_laranja_prm(df: pd.DataFrame) -> pd.DataFrame:

    df = df[["Data", "Preço"]]
    df.columns = [string_normalizer(col) for col in df.columns]
    df = df.rename(columns={"preco": "preco_medio_laranja"})
    df.loc[:, DATE_COL] = df[DATE_COL].dt.date

    return df


def precos_laranja_fte(df: pd.DataFrame,
                      spine: pd.DataFrame,
                      params: Dict[str, int],
                      spine_lookback_days: int) -> pd.DataFrame:

    fte_df = pd.DataFrame()
    spine = spine[["data_inferior", "data_visita"]].drop_duplicates()

    # quantidade de janelas para agregar as features
    tamanho_janela_dias = params["aggregate_window_days"]
    qtd_janelas = math.floor(spine_lookback_days / tamanho_janela_dias)

    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_visita"]):

        dfaux = df[df[DATE_COL].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            df_biz_ftes = _build_biz_ftes(df=dfaux)

            df_oscl_idx = cria_indices_oscilacao(df=dfaux,
                                                 janela_agg_dias=params["aggregate_window_days"],
                                                 value_col="preco_medio_laranja",
                                                 date_col=DATE_COL)
            df_oscl_idx = df_oscl_idx.set_index(DATE_COL).add_prefix("laranja_").reset_index()

            fteaux_df = reduce(lambda left, right: pd.merge(left, right, on=[DATE_COL], how="inner"),
                               [df_biz_ftes, df_oscl_idx, dfaux])

            assert fteaux_df.shape[0] == df_biz_ftes.shape[0] == df_oscl_idx.shape[0], \
                "Número errado de linhas pós join, revisar"

            define_janelas = define_janela_datas(data_inicio=data_inferior,
                                                qtd_janelas=qtd_janelas,
                                                tamanho_janela_dias=tamanho_janela_dias)
            define_janelas = seleciona_janelas(janelas=define_janelas, slc_janelas_numero=[1, 12])

            fteaux_df = filtra_data_janelas(df=fteaux_df,
                                        date_col_name=DATE_COL,
                                        janelas=define_janelas,
                                        tipo_janela="right")

            fteaux_df.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fteaux_df])

    fte_df = fte_df.fillna(0)

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature yfinance duplicada, revisar"

    return fte_df


def _build_biz_ftes(df: pd.DataFrame) -> pd.DataFrame:

    def _build_logreturns(col: pd.Series) -> pd.Series:
        return np.log(1 + col)

    df = df[[DATE_COL, "preco_medio_laranja"]]

    df = df.set_index(DATE_COL).sort_index()
    df = df.pct_change().fillna(0)
    df = df.apply(_build_logreturns, axis=1)

    df = df[["preco_medio_laranja"]].cumsum()
    df = df.reset_index().rename(columns={"preco_medio_laranja": "laranja_logrets_cumsum"})

    return df
