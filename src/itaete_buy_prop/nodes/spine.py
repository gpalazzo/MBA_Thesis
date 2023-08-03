# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Dict, List

import pandas as pd

BASE_JOIN_COLS = ["id_cliente", "data_pedido", "data_inferior"]


def spine_prm(df_anls_fin: pd.DataFrame,
              df_fnl_vendas: pd.DataFrame,
              clientes_df: pd.DataFrame) -> pd.DataFrame:

    df_anls_fin = df_anls_fin[["id_cliente", "processo", "data_pedido", "data_faturamento"]] \
                    .rename(columns={"data_pedido": "data_pedido2",
                                    "data_faturamento": "data_faturamento2"})

    df_fnl_vendas = df_fnl_vendas.rename(columns={"data_pedido": "data_pedido1",
                                                "data_faturamento": "data_faturamento1"})

    df = df_anls_fin.merge(df_fnl_vendas, on=["id_cliente", "processo"])
    _validate_join(df, df_anls_fin, df_fnl_vendas)

    df.loc[:, "data_pedido"] = df.apply(lambda col: col["data_pedido2"] \
                                        if pd.isnull(col["data_pedido1"]) \
                                            else col["data_pedido1"], axis=1)

    df.loc[:, "data_faturamento"] = df.apply(lambda col: col["data_faturamento2"] \
                                        if pd.isnull(col["data_faturamento1"]) \
                                            else col["data_faturamento1"], axis=1)

    df = df.drop(columns=["processo", "data_pedido1", "data_pedido2", "data_faturamento1", "data_faturamento2"])

    # remover dado quando as 2 colunas são iguais a "-"
    df = df[~((df["data_pedido"].isnull()) & (df["data_faturamento"].isnull()))]

    return df


def spine_preprocessing(df: pd.DataFrame, params: Dict[str, int]) -> pd.DataFrame:

    df = df.reset_index(drop=True)
    df = df.drop_duplicates()

    df.loc[:, "data_inferior"] = df["data_pedido"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row - timedelta(days=params["dt_fat_lookback_window"]))
    df.loc[:, "data_superior"] = df["data_pedido"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row + timedelta(days=params["dt_fat_lookforward_window"]))

    # ajuste para considerar apenas os itens em que a data do pedido é maior em, no máximo, N dias da data do faturamento
    # consultar parâmetros para saber o valor de N
    df_final = _ajusta_data_fatrm_menor_pedido(df=df, params=params)

    return df_final


def spine_labeling(df: pd.DataFrame) -> pd.DataFrame:

    df = df.reset_index(drop=True)

    df1 = df[df["data_faturamento"].notnull()]
    df1.loc[:, "label"] = "compra"

    df2 = df.drop(df1.index)
    df2.loc[:, "label"] = "nao_compra"

    df = pd.concat([df1, df2])

    df = df.drop(columns=["data_faturamento"])
    df = df.drop_duplicates(subset=BASE_JOIN_COLS + ["label"])

    assert df.shape[0] == df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
        "Spine duplicada no labeling, revisar"
    assert df.isnull().sum().sum() == 0, "Spine tem nulo, revisar"

    return df


def _ajusta_data_fatrm_menor_pedido(df: pd.DataFrame, params: Dict[str, int]) -> pd.DataFrame:

    df = df.reset_index(drop=True)
    df_maior = df[df["data_pedido"] > df["data_faturamento"]]

    if df_maior.empty:
        return df

    else:
        df_menor = df.drop(df_maior.index)

        df_maior.loc[:, "diff_dias"] = (df["data_pedido"] - df["data_faturamento"]).dt.days
        df_maior = df_maior[df_maior["diff_dias"] <= params["dt_fat_pedido_diffdays"]]

        df_maior = df_maior.drop(columns=["diff_dias"])
        df_maior.loc[:, "data_pedido"] = df["data_faturamento"].copy()

        df = pd.concat([df_maior, df_menor])
        return df


def _validate_join(res_df: pd.DataFrame, *dfs: List[pd.DataFrame]) -> pd.DataFrame:

    min_rows = min([df.shape[0] for df in dfs])
    assert res_df.shape[0] <= min_rows, "Join duplicou os dados, revisar"
