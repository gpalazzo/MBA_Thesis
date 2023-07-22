# -*- coding: utf-8 -*-
from functools import reduce
from typing import Tuple

import pandas as pd


def cria_master_table(spine: pd.DataFrame, *args: Tuple[pd.DataFrame]) -> pd.DataFrame:

    spine = spine[["id_cliente", "data_faturamento_nova", "data_inferior", "label"]] \
                .rename(columns={"data_faturamento_nova": "data_alvo"})

    JOIN_COLS = ["id_cliente", "data_alvo", "data_inferior"]
    ALL_DFS = [spine] + list(args)

    mt_df = reduce(lambda left, right: pd.merge(left, right, on=JOIN_COLS, how="inner"), ALL_DFS)

    min_rows = min([df.shape[0] for df in ALL_DFS])
    assert mt_df.shape[0] == min_rows, "Número de linhas errado na master table, revisar"

    return mt_df
