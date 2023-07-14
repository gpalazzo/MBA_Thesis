# -*- coding: utf-8 -*-
from typing import Dict

import pandas as pd


def clientes_prm(df: pd.DataFrame, params: Dict[str, str]) -> pd.DataFrame:

    df = df[["SEQPESSOA", "ORIGEM RECEITA"]].rename(columns={"SEQPESSOA": "id_cliente",
                                                             "ORIGEM RECEITA": "origem_receita"})

    df.loc[:, "origem_receita"] = df["origem_receita"].str.lower().apply(str)
    df.loc[:, "cultura_flag"] = df["origem_receita"].apply(lambda row: params["cultura"] in row)

    df = df[df["cultura_flag"]][["id_cliente"]].drop_duplicates()
    return df
