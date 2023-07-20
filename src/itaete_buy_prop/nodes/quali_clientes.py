# -*- coding: utf-8 -*-
from typing import List

import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer


def quali_clientes_prm(df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:

    df = df[["SEQPESSOA",
             "POTENCIAL",
             "FJ",
             "STS",
             "FROTA"]].rename(columns={"SEQPESSOA": "id_cliente",
                                        "FROTA": "qtd_maquinas"})

    # considera somente uma cultura específica (ver parâmetros)
    df = df.merge(clientes_df[["id_cliente"]], on="id_cliente", how="inner")

    df.columns = df.columns.str.lower()
    _cols = ["potencial", "fj", "sts"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_cols)

    df = _build_dummies(df=df, categ_cols=_cols)

    return df


def _build_dummies(df: pd.DataFrame, categ_cols: List[str]) -> pd.DataFrame:

    final_df = df.copy()

    for col in categ_cols:
        dfaux = pd.get_dummies(df[col])

        dfaux = dfaux.add_prefix(f"bool_{col}_")
        final_df = final_df.rename(columns={col: f"ctg_{col}"})

        final_df = pd.concat([final_df, dfaux], axis=1)

    return final_df
