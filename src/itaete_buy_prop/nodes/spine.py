# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd


def spine_prm(df: pd.DataFrame) -> pd.DataFrame:

    df = df[["SeqPessoa", "Data pedido", "Dta Fat"]].rename(columns={
                                                        "SeqPessoa": "id_cliente",
                                                        "Data pedido": "data_pedido",
                                                        "Dta Fat": "data_faturamento"})

    # remover dado quando as 2 colunas são iguais a "-"
    df = df[~((df["data_pedido"] == "-") & (df["data_faturamento"] == "-"))]
    df = df.replace({"-": np.nan})

    df.loc[:, "data_pedido"] = pd.to_datetime(df["data_pedido"], errors="ignore").dt.date
    df.loc[:, "data_faturamento"] = pd.to_datetime(df["data_faturamento"], errors="ignore").dt.date

    return df


def spine_preprocessing(df: pd.DataFrame, clientes_df: pd.DataFrame, params: Dict[str, int]) -> pd.DataFrame:

    df = df.merge(clientes_df[["id_cliente"]].drop_duplicates(), on="id_cliente", how="inner")
    df = df.reset_index(drop=True)

    # adicionar uma data de faturamento fake para pedidos sem faturamento
    lower_bound_date_pedido = datetime.now().date() - timedelta(days=params["qtd_dias_lookback_pedidos"])
    dfaux = df[df["data_pedido"] >= lower_bound_date_pedido]
    diff_days = (dfaux["data_faturamento"] - dfaux["data_pedido"]).dt.days
    diff_days = diff_days.dropna()
    median_days = np.median(diff_days)

    df1 = df[(df["data_pedido"].notnull()) & (df["data_faturamento"].isnull())]
    df1.loc[:, "data_faturamento_nova"] = df["data_pedido"] + timedelta(days=median_days)

    df = df.drop(df1.index)
    df.loc[:, "data_faturamento_nova"] = df["data_faturamento"].copy()
    df = pd.concat([df, df1])
    # fim

    df.loc[:, "data_inferior"] = df["data_faturamento_nova"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row - timedelta(days=params["dt_fat_lookback_window"]))
    df.loc[:, "data_superior"] = df["data_faturamento_nova"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row + timedelta(days=params["dt_fat_lookforward_window"]))

    # ajuste para considerar apenas os itens em que a data do pedido é maior em, no máximo, 30 dias da data do faturamento
    df = df.reset_index(drop=True)
    df1 = df[df["data_pedido"].isnull()]
    df2 = df.drop(df1.index)
    df2 = df2[df2["data_faturamento"].isnull()]

    df_nulls = pd.concat([df1, df2])

    df3 = df.drop(df_nulls.index)
    df3.loc[:, "data_faturamento_aux"] = df3["data_faturamento"] + timedelta(days=params["dt_fat_pedido_diffdays"])
    df3 = df3[df3["data_faturamento_aux"] >= df3["data_pedido"]]

    df_final = pd.concat([df_nulls, df3])
    # fim do ajuste de data do pedido

    df_final = df_final.drop(columns=["data_faturamento_aux"])

    return df_final


def spine_labeling(df: pd.DataFrame) -> pd.DataFrame:

    df = df.reset_index(drop=True)

    df1 = df[df["data_faturamento"].notnull()]
    df1.loc[:, "label"] = "compra"

    df2 = df.drop(df1.index)
    df2.loc[:, "label"] = "nao_compra"

    df = pd.concat([df1, df2])

    df = df.drop(columns=["data_pedido", "data_faturamento"])
    df = df.drop_duplicates()
    assert df.isnull().sum().sum() == 0, "Spine tem nulo, revisar"

    return df
