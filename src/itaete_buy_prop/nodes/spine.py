# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Dict, Union

import numpy as np
import pandas as pd

BASE_JOIN_COLS = ["id_cliente", "data_visita", "data_inferior"]


def spine_preprocessing(df: pd.DataFrame,
                        params: Dict[str, Union[str, int]]) -> pd.DataFrame:

    df = df[df["acao"] == params["acao"]]

    # crucial que o sort seja primeiro no cliente e depois na data
    df = df.sort_values(by=["id_cliente", "data_visita"])
    df[["data_visita_anterior", "resultado_anterior"]] = df.groupby(["id_cliente", "acao"]) \
                                                                ["data_visita", "resultado"].shift()

    df.loc[:, "diff_dias_visita"] = df.apply(lambda col: (col["data_visita"] - col["data_visita_anterior"]).days
                                                if col["resultado"] == "contato_em_andamento" and \
                                                    col["resultado_anterior"] == "com_interesse_compra"
                                                else np.nan,
                                                axis=1)

    df.loc[:, "data_inferior"] = df["data_visita"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row - timedelta(days=params["dt_fat_lookback_window"]))
    df.loc[:, "data_superior"] = df["data_visita"].apply(lambda row: row \
                                                            if pd.isnull(row) \
                                                            else row + timedelta(days=params["dt_fat_lookforward_window"]))

    return df


def spine_labeling(df: pd.DataFrame,
                   max_diff_dias: int) -> pd.DataFrame:

    TARGET_COLS = ["id_cliente", "data_visita", "data_inferior", "data_superior", "label"]

    df = df.reset_index(drop=True)
    df.loc[:, "diff_dias_visita"] = df["diff_dias_visita"].fillna(max_diff_dias + 1)

    df_compra = df[df["resultado"] == "com_interesse_compra"]
    df_compra.loc[:, "label"] = "compra"
    df_compra = df_compra[TARGET_COLS]

    df_naocompra = df.drop(df_compra.index)
    df_naocompra = df_naocompra[~(
                                (df_naocompra["id_cliente"].isin(df_compra["id_cliente"])) & \
                                (df_naocompra["data_visita"].isin(df_compra["data_visita"]))
                                )]

    df_naocompra.loc[:, "label"] = df_naocompra.apply(lambda col: "nao_compra" \
                                                        if (col["resultado"] == "contato_em_andamento" and \
                                                            col["resultado_anterior"] != "com_interesse_compra") \
                                                            or \
                                                            (col["resultado"] == "contato_em_andamento" and \
                                                            col["resultado_anterior"] == "com_interesse_compra" and
                                                            col["diff_dias_visita"] > max_diff_dias)
                                                        else "indefinido", axis=1)

    df_naocompra = df_naocompra[df_naocompra["label"] == "nao_compra"]
    df_naocompra = df_naocompra[TARGET_COLS]

    final_df = pd.concat([df_compra, df_naocompra])
    final_df = final_df.drop_duplicates(subset=TARGET_COLS)

    DATES = ["data_inferior", "data_superior", "data_visita"]
    for date in DATES:
        final_df.loc[:, date] = final_df[date].dt.date

    assert final_df.shape[0] == final_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
        "Spine duplicada no labeling, revisar"
    assert final_df.isnull().sum().sum() == 0, "Spine tem nulo, revisar"

    return final_df
