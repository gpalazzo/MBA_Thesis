# -*- coding: utf-8 -*-
from typing import Dict

import pandas as pd


def origem_receita_frota_prm(df: pd.DataFrame, params: Dict[str, str]) -> pd.DataFrame:

    df = df[["SEQPESSOA",
             "TIPO",
             "NUMERO4",
             "NUMERO4DESC"]].rename(columns={"SEQPESSOA": "id_cliente"})

    df.columns = df.columns.str.lower()
    df["tipo"] = df["tipo"].str.lower()
    df = df[df["tipo"] == params["cultura"]]

    assert df["numero4desc"].unique()[0] == "Área(ha)*", "Métrica diferente de área, revisar."

    df = df.rename(columns={"numero4": "area_ha"})
    df = df[["id_cliente", "area_ha"]].drop_duplicates()

    return df
