# -*- coding: utf-8 -*-
from typing import Dict

import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer


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

    df = df.rename(columns={"numero4": "area_ha"})
    df = df.groupby(["id_cliente", "ref_date"])["area_ha"] \
        .sum() \
        .reset_index() \
        .rename(columns={"area_ha": "area_ha_somadas"})

    assert df.isnull().sum().sum() == 0, "Nulos no primary, revisar"

    return df
