# -*- coding: utf-8 -*-
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, RepeatedStratifiedKFold


def string_normalizer(_str) -> str:

    if _str in [None, np.nan]:
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


def build_dummies(df: pd.DataFrame, categ_cols: List[str]) -> pd.DataFrame:

    final_df = df.copy()

    for col in categ_cols:
        dfaux = pd.get_dummies(df[col])

        dfaux = dfaux.add_prefix(f"bool_{col}_")
        final_df = final_df.rename(columns={col: f"ctg_{col}"})

        final_df = pd.concat([final_df, dfaux], axis=1)

    return final_df


def input_null_values(df: pd.DataFrame,
                    input_strategy: str,
                    date_col_name: str = "ref_date") -> pd.DataFrame:

    df = df.reset_index(drop=True)
    null_cols = df.columns[df.isna().any()].tolist()

    if null_cols != []:
        null_dates = _get_date_null_cols(df=df, null_cols=null_cols, date_col_name=date_col_name)
        null_dates = sorted(null_dates, reverse=True)
        max_date = null_dates[0]

        dfaux = df[df[date_col_name] <= max_date]
        df = df.drop(dfaux.index)

        df_filled = null_handler(df=dfaux,
                                null_cols=null_cols,
                                input_strategy=input_strategy)

        df = pd.concat([df, df_filled])

    return df


def _get_date_null_cols(df: pd.DataFrame,
                        null_cols: List[str],
                        date_col_name: str = "ref_date") -> List[str]:

    dates = []

    for col in null_cols:
        dfaux = df[df[col].isnull()]
        dateaux = dfaux[date_col_name].unique().tolist()
        dates += dateaux

    return list(set(dates))


def null_handler(df: pd.DataFrame,
                null_cols: List[str],
                input_strategy: str
                ) -> pd.DataFrame:

    NULL_VALUES = None

    df = df.reset_index(drop=True)

    dfaux = df[null_cols].copy()
    df = df.drop(columns=null_cols)

    imp = SimpleImputer(missing_values=NULL_VALUES, strategy=input_strategy)
    df_filled = pd.DataFrame(imp.fit_transform(dfaux))

    df_filled.columns = null_cols
    df_filled = df_filled.merge(df, left_index=True, right_index=True, how="inner")

    assert df_filled.shape[0] == df.shape[0], "Dados perdidos no null handler, revisar"
    assert df_filled.isnull().sum().sum() == 0, "Nulos mesmo após aplicar null handler, revisar"

    return df_filled


def optimize_params(model: LogisticRegression,
                    grid: Dict[str, Any],
                    X_train: pd.DataFrame,
                    y_train: pd.DataFrame,
                    n_splits: int,
                    n_repeats: int,
                    random_state: int = 1,
                    grid_search_scoring: str = "accuracy") -> Dict[str, str]:

    cv = RepeatedStratifiedKFold(n_splits=n_splits, n_repeats=n_repeats, random_state=random_state)
    grid_search = GridSearchCV(estimator=model,
                                param_grid=grid,
                                n_jobs=-1,
                                cv=cv,
                                scoring=grid_search_scoring,
                                error_score=0)

    grid_result = grid_search.fit(X_train, y_train)

    return grid_result


def define_janela_datas(data_inicio: datetime.date,
                        qtd_janelas: int,
                        tamanho_janela_dias: int) -> Dict[str, str]:

    janelas_dict = {}
    PREFIX = "janela"

    for i in list(range(1, qtd_janelas+1)):
        data_inicio_janela = data_inicio + timedelta(days=tamanho_janela_dias * (i-1))
        data_fim_janela = data_inicio + timedelta(days=tamanho_janela_dias * i)

        janelas_dict[f"{PREFIX}{i}"] = [data_inicio_janela, data_fim_janela]

    return janelas_dict


def filtra_data_janelas(df: pd.DataFrame,
                        date_col_name: str,
                        janelas: Dict[str, List[datetime.date]],
                        tipo_janela: str) -> pd.DataFrame:

    final_df = pd.DataFrame()

    for janela, datas in janelas.items():
        if tipo_janela == "right":
            _data = datas[1]
        elif tipo_janela == "left":
            _data = datas[0]
        else:
            raise RuntimeError("tipo_janela desconhecida, revisar")

        dfaux = df[df[date_col_name] == _data]
        dfaux = dfaux \
                    .set_index(date_col_name) \
                    .add_prefix(f"{janela}__") \
                    .reset_index(drop=True)

        final_df = pd.concat([final_df, dfaux], axis=1)

    return final_df


def seleciona_janelas(janelas: Dict[str, List[datetime.date]],
                      slc_janelas_numero: List[int]) -> Dict[str, List[datetime.date]]:

    _dict = {}

    janelas_alvo = [f"janela{i}" for i in slc_janelas_numero]
    keys = list(janelas)

    janelas_alvo = set(janelas_alvo).intersection(keys)

    for janela in janelas_alvo:
        _dict[janela] = janelas[janela]

    return _dict
