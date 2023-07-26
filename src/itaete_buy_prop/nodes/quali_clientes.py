# -*- coding: utf-8 -*-
import pandas as pd

from itaete_buy_prop.utils import (
    build_dummies,
    col_string_normalizer,
    string_normalizer,
)

BASE_JOIN_COLS = ["id_cliente", "ref_date"]


def quali_clientes_prm(df: pd.DataFrame, clientes_df: pd.DataFrame) -> pd.DataFrame:
    """essa função está pegando a data de outra aba do excel. isso é uma proxy, validar se faz
    sentido (apesar de não ser um processo ideal)
    """

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

    assert df[["id_cliente", "ref_date"]].isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df


def quali_clientes_fte(df: pd.DataFrame) -> pd.DataFrame:

    _cols = ["fj", "sts"]
    df = build_dummies(df=df, categ_cols=_cols)
    df = df.drop_duplicates()

    assert df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert df.shape[0] == df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
            "Feature quali_clientes duplicada, revisar"

    return df
