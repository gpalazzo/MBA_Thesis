# -*- coding: utf-8 -*-
import re
import unicodedata
from typing import List

import pandas as pd


def string_normalizer(_str) -> str:

    if _str is None:
        return _str

    column_new = (
        unicodedata.normalize("NFKD", _str).encode("ascii", "ignore").decode("utf-8")
    )
    column_new = re.sub("[ :_\\.,;{}()\n\t=]+", "_", column_new)
    column_new = re.sub("[/]+", "by", column_new)
    column_new = re.sub("#", "number", column_new)
    column_new = re.sub("%", "percent", column_new)
    column_new = re.sub("[&+]+", "and", column_new)
    column_new = re.sub("[|,;]+", "or", column_new)
    column_new = re.sub("[\^]+", "", column_new)

    column_new = column_new.lower()

    return column_new


def col_string_normalizer(
    df: pd.DataFrame, _cols_to_normalize: List[str] = None
) -> pd.DataFrame:

    cols_to_normalize = (
        _cols_to_normalize or df.select_dtypes(include=[object]).columns.tolist()
    )

    for col_to_normalize in cols_to_normalize:
        df.loc[:, col_to_normalize] = df[col_to_normalize].apply(string_normalizer)

    return df
