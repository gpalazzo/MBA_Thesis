# -*- coding: utf-8 -*-
from functools import reduce
from typing import Any, Dict, Tuple

import pandas as pd
from sklearn.model_selection import train_test_split


def cria_master_table(spine: pd.DataFrame, *args: Tuple[pd.DataFrame]) -> pd.DataFrame:

    spine = spine[["id_cliente", "data_faturamento_nova", "data_inferior", "label"]] \
                .rename(columns={"data_faturamento_nova": "data_alvo"})

    JOIN_COLS = ["id_cliente", "data_alvo", "data_inferior"]
    ALL_DFS = [spine] + list(args)

    mt_df = reduce(lambda left, right: pd.merge(left, right, on=JOIN_COLS, how="inner"), ALL_DFS)

    mt_df = mt_df.drop(columns=["id_cliente"])

    min_rows = min([df.shape[0] for df in ALL_DFS])
    assert mt_df.shape[0] == min_rows, "Número de linhas errado na master table, revisar"

    return mt_df


def mt_split_treino_teste(master_table: pd.DataFrame,
                          params: Dict[str, Any]) -> \
                            Tuple[pd.DataFrame,
                                pd.DataFrame,
                                pd.DataFrame,
                                pd.DataFrame]:

    treino_teste_params = params["treino_teste_split"].copy()
    target_col = params["target_col"]

    if treino_teste_params["stratify"] is True:
        treino_teste_params.update({"stratify": master_table[target_col].to_numpy()})
    else:
        del treino_teste_params["stratify"]

    train_df, test_df = train_test_split(master_table, **treino_teste_params)
    train_df, test_df = train_df.reset_index(drop=True), test_df.reset_index(drop=True)

    X_train, y_train = train_df.drop(columns=params["target_col"]), train_df[[target_col]]
    X_test, y_test = test_df.drop(columns=params["target_col"]), test_df[[target_col]]

    return X_train, y_train, X_test, y_test
