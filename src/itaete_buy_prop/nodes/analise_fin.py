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

BASE_JOIN_COLS = ["id_cliente", "data_inferior", "data_alvo"]


def analise_fin_prm(df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:

    df = df[["SeqPessoa",
             "Processo",
             "Potencial do cliente",
             "Linha Credito",
             "Data pedido",
             "Dta Fat",
             "Empresa",
             "Tipo produto",
             "Valor Total"]].rename(columns={"SeqPessoa": "id_cliente",
                                             "Dta Fat": "data_faturamento"})

    df.columns = [string_normalizer(col) for col in df.columns]
    _cols = ["potencial_do_cliente", "linha_credito",
             "empresa", "tipo_produto"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_cols)

    df_merged = df.merge(clientes_df[["id_cliente", "empresa", "tipo_produto"]], \
                        on=["id_cliente", "empresa", "tipo_produto"],\
                        how="inner")
    df_merged = df_merged.drop(columns=["empresa", "tipo_produto"])

    df_merged = df_merged.replace({"-": np.nan})
    df_merged.loc[:, "data_pedido"] = pd.to_datetime(df_merged["data_pedido"], errors="ignore").dt.date
    df_merged.loc[:, "data_faturamento"] = pd.to_datetime(df_merged["data_faturamento"], errors="ignore").dt.date

    return df_merged


def analise_fin_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    df = df[["id_cliente", "data_pedido", "potencial_do_cliente", "linha_credito", "valor_total"]] \
            .drop_duplicates() \
            .dropna(subset=["data_pedido"])

    df = input_null_values(df=df, input_strategy="most_frequent", date_col_name="data_pedido")

    df_grp = df.groupby(["id_cliente", "data_pedido", "potencial_do_cliente", "linha_credito"]) \
        ["valor_total"].sum() \
        .reset_index()

    fte_df = pd.DataFrame()

    for cliente, data_inferior, data_alvo in zip(spine["id_cliente"], spine["data_inferior"], spine["data_visita"]):

        dfaux = df_grp[(df_grp["id_cliente"] == cliente) & \
                        (df_grp["data_pedido"].between(data_inferior, data_alvo))]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            df_tempo_medio = _calcula_tempoMedioDias(datas=dfaux["data_pedido"].tolist())
            df_vlr_ctg_contar = _conta_valorCategorico(df=dfaux, cols=["potencial_do_cliente", "linha_credito"])
            df_soma_compras = _soma_valorTotal_compras(compras=dfaux["valor_total"].tolist())
            df_dias_ultima_compra = _calcula_diasPassados_ultimaCompra(datas=dfaux["data_pedido"].tolist(),
                                                                       data_maxima=data_alvo)

            fte_dfaux = pd.concat([df_tempo_medio, df_vlr_ctg_contar, df_soma_compras, df_dias_ultima_compra], axis=1)
            fte_dfaux.loc[:, ["id_cliente", "data_inferior", "data_alvo"]] = [cliente, data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fte_dfaux])

    fte_df = fte_df.fillna(0)

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature analise_fin duplicada, revisar"

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
