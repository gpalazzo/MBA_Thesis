# -*- coding: utf-8 -*-
import pandas as pd

from itaete_buy_prop.utils import string_normalizer


def precos_laranja_prm(df: pd.DataFrame) -> pd.DataFrame:

    df = df[["Data", "Preço"]]
    df.columns = [string_normalizer(col) for col in df.columns]
    df = df.rename(columns={"preco": "preco_medio_laranja"})
    df.loc[:, "data"] = df["data"].dt.date

    return df


def precos_laranja_fte(df: pd.DataFrame) -> pd.DataFrame:
    pass
