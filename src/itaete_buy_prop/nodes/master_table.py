# -*- coding: utf-8 -*-
import logging
from functools import reduce
from typing import Any, Dict, List, Tuple

import pandas as pd
from imblearn.under_sampling import NearMiss
from sklearn.feature_selection import SelectKBest, mutual_info_classif
from sklearn.model_selection import train_test_split
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif

logger = logging.getLogger(__name__)
CLIENTE_COL = "id_cliente"
CLIENTE_BASE_JOIN_COLS = [CLIENTE_COL] + ["data_alvo", "data_inferior"]
DATE_BASE_JOIN_COLS = ["data_alvo", "data_inferior"]


def cria_master_table(spine: pd.DataFrame,
                      params: Dict[str, Any],
                      *args: Tuple[pd.DataFrame]) -> pd.DataFrame:

    cliente_args, date_args = _split_dataset_levels(args)

    spine = spine[["id_cliente", "data_pedido", "data_inferior", "label"]] \
                .rename(columns={"data_pedido": "data_alvo"})

    # join de todos os dados em diferentes níveis de agregação
    # o 1o join é a nível de cliente, logo é o mais granular. então o 2o join não pode ter mais linhas que esse primeiro
    ALL_CLIENTE_DFS = [spine] + cliente_args
    mt_df = reduce(lambda left, right: pd.merge(left, right, on=CLIENTE_BASE_JOIN_COLS, how="inner"), ALL_CLIENTE_DFS)
    # 2o join a nível somente de datas
    ALL_DATE_DFS = [mt_df] + date_args
    mt_df_all = reduce(lambda left, right: pd.merge(left, right, on=DATE_BASE_JOIN_COLS, how="inner"), ALL_DATE_DFS)
    assert mt_df_all.shape[0] <= mt_df.shape[0], "Duplicou linhas na master table, revisar"

    target_col = params["target_col"]
    TARGET_MAPPER = {"nao_compra": 0,
                     "compra": 1}
    mt_df_all.loc[:, target_col] = mt_df_all[target_col].map(TARGET_MAPPER)

    assert mt_df_all.shape[0] == mt_df_all[CLIENTE_BASE_JOIN_COLS].drop_duplicates().shape[0], \
        "Master table duplicada, revisar"

    mt_df_all = mt_df_all.set_index(CLIENTE_BASE_JOIN_COLS)

    return mt_df_all


def _split_dataset_levels(args: List[pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:

    cliente_dfs = []
    date_dfs = []

    for df in args:
        if CLIENTE_COL in df.columns:
            cliente_dfs.append(df)
        else:
            date_dfs.append(df)

    return cliente_dfs, date_dfs


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

    X_train, y_train = train_df.drop(columns=params["target_col"]), train_df[[target_col]]
    X_test, y_test = test_df.drop(columns=params["target_col"]), test_df[[target_col]]

    return X_train, y_train, X_test, y_test


def mt_balanceia_classes(df: pd.DataFrame,
                        params: Dict[str, Any]) -> pd.DataFrame:

    target_col = params["target_col"]
    limite_classes = params["class_bounds"]

    logger.info("Checking for class balance")
    label0_count, _ = df[target_col].value_counts()
    label0_pct = label0_count / df.shape[0]

    # check if any label is outside the desired range
    # there's no need to check both labels, if 1 is outside the range, the other will also be
    if not pd.Series(label0_pct). \
            between(limite_classes["lower"],
                    limite_classes["upper"]) \
                    [0]: #pull index [0] always works because it's only 1 label

        logger.info("Balanceando classes")
        X, y = df.drop(columns=[target_col]), df[[target_col]]
        df = _balance_classes(X=X, y=y)

    else:
        logger.info("Class are balanced, skipping balancing method")

    df = df.set_index(CLIENTE_BASE_JOIN_COLS)

    return df


def mt_remove_ftes_multic(df: pd.DataFrame,
                        params: Dict[str, Any]) -> pd.DataFrame:

    target_col = params["target_col"]
    valor_vif = params["vif_limite"]

    df_ftes_aux = df.drop(columns=[target_col]).copy()
    df_ftes_aux = df_ftes_aux.dropna()

    df_ftes_aux.loc[:, "const"] = 1 #VIF requires a constant col

    _vif = pd.DataFrame()
    _vif.loc[:, "features"] = df_ftes_aux.columns.copy()
    _vif.loc[:, "vif"] = [vif(df_ftes_aux.values, i) for i in range(df_ftes_aux.shape[1])]

    _vif = _vif[_vif["features"] != "const"] #drop constant col

    # get symbols below threshold
    slct_symbols = _vif[_vif["vif"] <= valor_vif]["features"].tolist()
    df = df[slct_symbols + [target_col]]

    return df, _vif


def mt_seleciona_features(X_train: pd.DataFrame,
                        y_train: pd.DataFrame,
                        X_test: pd.DataFrame,
                        params: Dict[str, Any]) -> Tuple[pd.DataFrame,
                                                         pd.DataFrame,
                                                         pd.DataFrame]:

    topN_features = params["slc_topN_features"]

    if X_train.shape[1] < topN_features:
        k = "all"
    else:
        k = topN_features

    selector = SelectKBest(mutual_info_classif, k=k)
    selector.fit_transform(X_train, y_train)
    slct_cols_idx = selector.get_support(indices=True)

    # cria dataframe com as importâncias das features
    fte_imps = {}
    for feature, score in zip(selector.feature_names_in_, selector.scores_):
        fte_imps[feature] = score
    df_fte_imps = pd.DataFrame({"features": fte_imps.keys(), "score": fte_imps.values()})

    slct_cols = X_train.iloc[:, slct_cols_idx].columns.tolist()
    X_train, X_test = X_train[slct_cols], X_test[slct_cols]

    return X_train, X_test, df_fte_imps


def _balance_classes(X: pd.DataFrame, y: pd.DataFrame) -> pd.DataFrame:

    X, y, lookup_idx_times = _cria_fake_index(X=X, y=y)

    nm = NearMiss(version=3)
    X_res, y_res = nm.fit_resample(X, y)

    idxs = nm.sample_indices_
    X_res.index, y_res.index = idxs, idxs

    mt = X_res.merge(y_res, left_index=True, right_index=True, how="inner")
    mt = mt.merge(lookup_idx_times, left_index=True, right_index=True, how="inner")

    return mt


def _cria_fake_index(X: pd.DataFrame, y: pd.DataFrame) -> Tuple[pd.DataFrame,
                                                                pd.DataFrame,
                                                                pd.DataFrame]:

    FAKE_IDX_NAME = "fake_idx"
    idxs = list(range(0, X.shape[0]))

    X.loc[:, FAKE_IDX_NAME] = idxs
    y.loc[:, FAKE_IDX_NAME] = idxs

    X = X.set_index(FAKE_IDX_NAME, append=True)
    y = y.set_index(FAKE_IDX_NAME, append=True)

    lookup_idx_times = y.reset_index() \
                        [CLIENTE_BASE_JOIN_COLS + [FAKE_IDX_NAME]] \
                        .set_index(FAKE_IDX_NAME)

    return X, y, lookup_idx_times
