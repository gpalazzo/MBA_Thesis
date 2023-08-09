# -*- coding: utf-8 -*-
import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer


def cen_visitas_prm(df: pd.DataFrame,
                    clientes_df: pd.DataFrame) -> pd.DataFrame:

    df = df[["Data.1", "SeqPessoa", "Ação", "Resultado"]] \
            .rename(columns={"Data.1": "data_visita",
                             "SeqPessoa": "id_cliente"})

    df = df.drop_duplicates()
    df = df.merge(clientes_df[["id_cliente"]], on="id_cliente", how="inner")

    df.columns = [string_normalizer(col) for col in df.columns]
    _col = ["acao", "resultado"]
    df = col_string_normalizer(df=df, _cols_to_normalize=_col)

    return df
