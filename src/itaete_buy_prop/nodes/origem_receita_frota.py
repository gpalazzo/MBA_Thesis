# -*- coding: utf-8 -*-
from typing import Dict

import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer

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
