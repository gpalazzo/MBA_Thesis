# -*- coding: utf-8 -*-
from typing import Dict, List

import numpy as np
import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer


def funil_vendas_prm(df: pd.DataFrame,
                     clientes_df: pd.DataFrame,
                     params: Dict[str, List[str]]) -> pd.DataFrame:

    df = df[["SeqPessoa", "Processo", "Data Pedido", "Data Faturamento", "Status Processo"]] \
                    .rename(columns={"SeqPessoa": "id_cliente"})

    df.columns = [string_normalizer(col) for col in df.columns]
    df = col_string_normalizer(df=df, _cols_to_normalize=["status_processo"])

    df = df[~df["status_processo"].isin(params["status_pedidos"])]

    df_merged = df.merge(clientes_df[["id_cliente"]], \
                        on=["id_cliente"],\
                        how="inner")

    df_merged = df_merged.replace({"-": np.nan})
    df_merged.loc[:, "data_pedido"] = pd.to_datetime(df_merged["data_pedido"], errors="ignore").dt.date
    df_merged.loc[:, "data_faturamento"] = pd.to_datetime(df_merged["data_faturamento"], errors="ignore").dt.date

    return df_merged
