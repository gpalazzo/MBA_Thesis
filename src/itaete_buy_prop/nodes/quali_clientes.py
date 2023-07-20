# -*- coding: utf-8 -*-
import pandas as pd

from itaete_buy_prop.utils import (
    build_dummies,
    col_string_normalizer,
    string_normalizer,
)


def quali_clientes_prm(df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:

    df = df[["SEQPESSOA",
             "FJ",
             "STS",
             "FROTA"]].rename(columns={"SEQPESSOA": "id_cliente",
                                        "FROTA": "qtd_maquinas"})

    # considera somente uma cultura específica (ver parâmetros)
    df = df.merge(clientes_df[["id_cliente", "ref_date"]], on="id_cliente", how="inner")

    df.columns = [string_normalizer(col) for col in df.columns]
    _cols = ["fj", "sts"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_cols)

    return df


def quali_clientes_fte(df: pd.DataFrame) -> pd.DataFrame:

    _cols = ["fj", "sts"]
    df = build_dummies(df=df, categ_cols=_cols)

    assert df.isnull().sum().sum() == 0, "Nulos na feature, revisar"

    return df
