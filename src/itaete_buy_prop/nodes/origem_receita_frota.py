# -*- coding: utf-8 -*-
from typing import Dict

import numpy as np
import pandas as pd

from itaete_buy_prop.utils import (
    col_string_normalizer,
    input_null_values,
    string_normalizer,
)

BASE_JOIN_COLS = ["id_cliente", "data_alvo", "data_inferior"]


def origem_receita_frota_prm(df: pd.DataFrame, params: Dict[str, str]) -> pd.DataFrame:

    df = df[["SEQPESSOA",
             "TIPO",
             "NUMERO4",
             "NUMERO4DESC",
             "DTALTERADO"]].rename(columns={"SEQPESSOA": "id_cliente",
                                            "DTALTERADO": "ref_date"})

    df.columns = [string_normalizer(col) for col in df.columns]
    _col = ["tipo"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_col)

    df = df[df["tipo"] == params["cultura"]]

    df["ref_date"] = df["ref_date"].dt.date

    assert df["numero4desc"].unique()[0] == "Área(ha)*", "Métrica diferente de área, revisar."

    df = df \
            .rename(columns={"numero4": "area_ha"}) \
            .drop(columns=["numero4desc", "tipo"])

    assert df[["id_cliente", "ref_date"]].isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df


def origem_receita_frota_fte(df: pd.DataFrame, spine: pd.DataFrame) -> pd.DataFrame:

    # ajuste para a função de input de nulos funcionar
    df = df.replace({np.nan: None})
    df = input_null_values(df=df, input_strategy="most_frequent")

    fte_df = pd.DataFrame()

    for cliente, data_inferior, data_alvo in zip(spine["id_cliente"], spine["data_inferior"], spine["data_faturamento_nova"]):

        dfaux = df[(df["id_cliente"] == cliente) & \
                    (df["ref_date"].between(data_inferior, data_alvo))]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            df_grp = dfaux.groupby(["id_cliente", "ref_date"])["area_ha"] \
                        .sum() \
                        .reset_index() \
                        .rename(columns={"area_ha": "area_ha_somadas"})

            df_grp = df_grp.drop(columns=["ref_date"])
            df_grp.loc[:, ["data_inferior", "data_alvo"]] = [data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, df_grp])

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
        "Feature origem_receita_frota duplicada, revisar"

    return fte_df
