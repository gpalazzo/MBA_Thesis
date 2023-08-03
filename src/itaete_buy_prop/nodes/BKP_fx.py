# -*- coding: utf-8 -*-
import math
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from itaete_buy_prop.utils import input_null_values

BASE_JOIN_COLS = ["data_alvo", "data_inferior"]


def fx_prm(df: pd.DataFrame) -> pd.DataFrame:

    df = df.rename(columns={"Taxa de Câmbio - R$ Dolar": "usdbrl_fx"})
    df.columns = df.columns.str.lower()
    df.loc[:, "data"] = df["data"].dt.date

    return df


def fx_fte(df: pd.DataFrame,
           spine: pd.DataFrame,
           params: Dict[str, int],
           spine_lookback_days: int) -> pd.DataFrame:

    df = input_null_values(df=df, input_strategy="most_frequent")
    # a spine está no nível de cliente e data, mas aqui só precisa da data
    spine = spine[["data_inferior", "data_faturamento_nova"]].drop_duplicates()

    # quantidade de janelas para agregar as features
    tamanho_janela_dias = params["aggregate_window_days"]
    qtd_janelas = math.floor(spine_lookback_days / tamanho_janela_dias)

    fte_df = pd.DataFrame()
    for data_inferior, data_alvo in zip(spine["data_inferior"], spine["data_faturamento_nova"]):

        dfaux = df[df["data"].between(data_inferior, data_alvo)]

        # se dataframe não tiver dado na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            define_janelas = _define_janelas(data_inicio=data_inferior,
                                             qtd_janelas=qtd_janelas,
                                             tamanho_janela_dias=tamanho_janela_dias)
            df_janelas_agg = _agrega_janelas(df=dfaux, janelas=define_janelas)

            df_janelas_agg.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]
            fte_df = pd.concat([fte_df, df_janelas_agg])

    # dropa nulos: isso pode ocorrer porque o horizonte de início e fim de dados das fontes são diferentes
    fte_df = fte_df.dropna()

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature fx duplicada, revisar"
    return fte_df


def _define_janelas(data_inicio: datetime.date,
                    qtd_janelas: int,
                    tamanho_janela_dias: int) -> Dict[str, str]:

    janelas_dict = {}
    PREFIX = "janela"

    for i in list(range(1, qtd_janelas+1)):
        data_inicio_janela = data_inicio + timedelta(days=tamanho_janela_dias * (i-1))
        data_fim_janela = data_inicio + timedelta(days=tamanho_janela_dias * i)

        janelas_dict[f"{PREFIX}_{i}"] = [data_inicio_janela, data_fim_janela]

    return janelas_dict


def _agrega_janelas(df: pd.DataFrame,
                    janelas: Dict[str, List[datetime.date]]) -> pd.DataFrame:

    final_df = pd.DataFrame()
    DATE_COL = "data"
    VALUE_COL = "usdbrl_fx"

    for janela, datas in janelas.items():
        dfaux = df[df[DATE_COL].between(datas[0], datas[1], inclusive="right")]
        dfaux = dfaux.set_index(DATE_COL)
        mean = dfaux[VALUE_COL].mean()

        _df = pd.DataFrame({f"{VALUE_COL}__{janela}": mean}, index=[0])

        final_df = pd.concat([final_df, _df], axis=1)

    return final_df
