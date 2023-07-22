# -*- coding: utf-8 -*-
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd

from itaete_buy_prop.utils import (
    col_string_normalizer,
    input_null_values,
    string_normalizer,
)


def analise_fin_prm(df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:

    df = df[["SeqPessoa",
             "Potencial do cliente",
             "Linha Credito",
             "Data pedido",
             "Dta Fat",
             "Valor Total"]].rename(columns={"SeqPessoa": "id_cliente"})

    # considera somente uma cultura específica (ver parâmetros)
    df = df.merge(clientes_df[["id_cliente"]], on="id_cliente", how="inner")
    df.columns = [string_normalizer(col) for col in df.columns]

    # remover dado quando as 2 colunas são iguais a "-"
    df = df[~((df["data_pedido"] == "-") & (df["dta_fat"] == "-"))]
    df = df.replace({"-": np.nan})

    df.loc[:, "ref_date"] = df.apply(lambda col: col["data_pedido"] \
                                     if pd.isnull(col["dta_fat"]) \
                                    else col["dta_fat"], axis=1)
    df["ref_date"] = df["ref_date"].dt.date
    df = df.drop(columns=["data_pedido", "dta_fat"])

    _cols = ["potencial_do_cliente", "linha_credito"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_cols)

    assert df[["id_cliente", "ref_date"]].isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df


def analise_fin_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    df = input_null_values(df=df, input_strategy="most_frequent")

    df_grp = df.groupby(["id_cliente", "ref_date", "potencial_do_cliente", "linha_credito"]) \
        ["valor_total"].sum() \
        .reset_index()

    fte_df = pd.DataFrame()

    for cliente, data_inferior, data_alvo in zip(spine["id_cliente"], spine["data_inferior"], spine["data_faturamento_nova"]):

        dfaux = df_grp[(df_grp["id_cliente"] == cliente) & \
                        (df_grp["ref_date"].between(data_inferior, data_alvo))]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            df_tempo_medio = _calcula_tempoMedioDias(datas=dfaux["ref_date"].tolist())
            df_vlr_ctg_contar = _conta_valorCategorico(df=dfaux, cols=["potencial_do_cliente", "linha_credito"])
            df_soma_compras = _soma_valorTotal_compras(compras=dfaux["valor_total"].tolist())
            df_dias_ultima_compra = _calcula_diasPassados_ultimaCompra(datas=dfaux["ref_date"].tolist(),
                                                                       data_maxima=data_alvo)

            fte_dfaux = pd.concat([df_tempo_medio, df_vlr_ctg_contar, df_soma_compras, df_dias_ultima_compra], axis=1)
            fte_dfaux.loc[:, ["id_cliente", "data_inferior", "data_alvo"]] = [cliente, data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fte_dfaux])

    fte_df = fte_df.fillna(0)
    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"

    return fte_df


def _calcula_tempoMedioDias(datas: List[datetime.date]) -> pd.DataFrame:

    datas = sorted(datas, reverse=False)
    min_date = datas[0]
    max_date = datas[-1]

    tempo_dias = (max_date - min_date).days
    tempo_medio = tempo_dias / len(datas)

    return pd.DataFrame({"tempo_medio_dias_compras": tempo_medio}, index=[0])


def _conta_valorCategorico(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:

    final_df = pd.DataFrame()

    for col in cols:
        dict_aux = df.groupby(col)[col].count().to_dict()
        dfaux = pd.DataFrame(dict_aux, index=[0])
        dfaux.columns = [f"{col}__{_col}" for _col in dfaux.columns]

        final_df = pd.concat([final_df, dfaux], axis=1)

    return final_df


def _soma_valorTotal_compras(compras: List[float]) -> pd.DataFrame:

    total = sum(compras)
    return pd.DataFrame({"valor_total_compras": total}, index=[0])


def _calcula_diasPassados_ultimaCompra(datas: List[datetime.date], data_maxima: datetime.date = None) -> pd.DataFrame:

    if data_maxima is None:
        data_maxima = datetime.now().date()

    datas = sorted(datas, reverse=False)
    ultima_data = datas[-1]

    dif_dias = (data_maxima - ultima_data).days
    return pd.DataFrame({"dif_dias_ultima_compra": dif_dias}, index=[0])
