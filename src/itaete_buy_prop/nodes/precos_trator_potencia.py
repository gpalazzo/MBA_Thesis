# -*- coding: utf-8 -*-
import math
from functools import reduce
from typing import Dict, List

import pandas as pd

from itaete_buy_prop.utils import (
    aplica_threshold_var,
    calculate_SMA,
    define_janela_datas,
    filtra_data_janelas,
    seleciona_janelas,
    string_normalizer,
)

BASE_JOIN_COLS = ["data_inferior", "data_alvo"]
DATE_COL = "data"


def precos_trator_potencia_prm(df: pd.DataFrame, params: Dict[str, List[str]]) -> pd.DataFrame:

    cols = tuple(params["potencia_trator"] + [DATE_COL])
    df = df[[col for col in df.columns if col.startswith(cols)]]

    df.columns = [string_normalizer(col) for col in df.columns]
    df.loc[:, DATE_COL] = df[DATE_COL].dt.date

    return df


def precos_trator_potencia_fte(df: pd.DataFrame,
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
            dfs_inner_loop = []
            for col in dfaux.set_index(DATE_COL).columns:
                _df = dfaux[[DATE_COL] + [col]]

                df_oscl_idx = calculate_SMA(data=_df,
                                            ndays=tamanho_janela_dias,
                                            value_col=col,
                                            date_col=DATE_COL)

                df_oscl_idx = df_oscl_idx.set_index(DATE_COL).add_prefix(f"px_trator_pot_{col}_")
                dfs_inner_loop.append(df_oscl_idx)

            fteaux_df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how="inner"),
                               dfs_inner_loop)
            fteaux_df = fteaux_df.reset_index()

            define_janelas = define_janela_datas(data_inicio=data_inferior,
                                                qtd_janelas=qtd_janelas,
                                                tamanho_janela_dias=tamanho_janela_dias)
            define_janelas = seleciona_janelas(janelas=define_janelas, slc_janelas_numero=[1, 6, 12])

            fteaux_df = filtra_data_janelas(df=fteaux_df,
                                        date_col_name=DATE_COL,
                                        janelas=define_janelas,
                                        tipo_janela="right")

            fteaux_df.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fteaux_df])

    fte_df = fte_df.fillna(0)
    fte_df = aplica_threshold_var(df=fte_df, date_col=BASE_JOIN_COLS, var_threshold=1e6)

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature yfinance duplicada, revisar"

    return fte_df
