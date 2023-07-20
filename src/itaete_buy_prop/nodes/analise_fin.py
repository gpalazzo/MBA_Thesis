# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer

from itaete_buy_prop.utils import (
    build_dummies,
    col_string_normalizer,
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
    df = df.drop(columns=["data_pedido", "dta_fat"])

    _cols = ["potencial_do_cliente", "linha_credito"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_cols)

    assert df[["id_cliente", "ref_date"]].isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df


def analise_fin_fte(df: pd.DataFrame) -> pd.DataFrame:

    df = df.reset_index(drop=True)
    null_cols = df.columns[df.isna().any()].tolist()

    dfaux = df[null_cols].copy()
    df = df.drop(columns=null_cols)

    imp = SimpleImputer(missing_values=None, strategy="most_frequent")
    df_filled = pd.DataFrame(imp.fit_transform(dfaux))
    df_filled.columns = null_cols

    final_df = df.merge(df_filled, left_index=True, right_index=True, how="inner")
    assert final_df.shape[0] == df.shape[0], "Dados perdidos, revisar"

    df_dummy = build_dummies(df=final_df, categ_cols=["potencial_do_cliente", "linha_credito"])

    assert df_dummy.isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df_dummy
