# -*- coding: utf-8 -*-
import logging
import math
import warnings
from typing import Any, Dict

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix

from itaete_buy_prop.utils import optimize_params

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


def logreg_model_fit(X_train: pd.DataFrame,
                    y_train: pd.DataFrame,
                    model_params: Dict[str, Any]) -> LogisticRegression:

    logreg_optimize_params = model_params["model_params_otimizar"]
    logreg_default_params = model_params["model_params_padrao"]
    logreg_extra_params = model_params["model_params_extra"]

    model = LogisticRegression(**logreg_default_params)

    if logreg_optimize_params:
        # params opt
        logger.info("Optimzing parameters")
        params_opt = optimize_params(model=model,
                                    grid=model_params,
                                    X_train=X_train,
                                    y_train=y_train,
                                    n_splits=10,
                                    n_repeats=3)
        params_opt = params_opt.best_params_

    else:
        params_opt = logreg_extra_params.copy()

    params_opt.update(logreg_default_params)
    model.set_params(**params_opt)
    model.fit(X_train, y_train)

    # df_params_opt = pd.DataFrame(params_opt, index=[0])

    return model


def logreg_model_predict(model: LogisticRegression,
                      X_test: pd.DataFrame) -> pd.DataFrame:

    idxs_nome = list(X_test.index.names)
    idxs_valor = X_test.index.tolist()

    y_pred = model.predict(X_test)
    df = pd.DataFrame(data={"y_pred": y_pred})

    df.loc[:, idxs_nome] = idxs_valor
    df = df.set_index(idxs_nome)

    return df


def logreg_model_relatorio(model: LogisticRegression,
                           X_test: pd.DataFrame,
                           y_test: pd.DataFrame,
                           y_pred: pd.DataFrame) -> pd.DataFrame:

    # acurária do modelo
    acc = accuracy_score(y_true=y_test, y_pred=y_pred)
    # parâmetros do modelo
    params = model.get_params()
    # matriz de confusão
    cm = confusion_matrix(y_true=y_test, y_pred=y_pred, normalize="all")

    # calcula probabilidade de pertencer a cada uma das classes
    idxs_nome = list(X_test.index.names)
    idxs_valor = X_test.index.tolist()
    probas = model.predict_proba(X_test)
    probas_df = pd.DataFrame(data=probas, columns=["proba_label_0", "proba_label_1"])
    probas_df.loc[:, idxs_nome] = idxs_valor
    probas_df = probas_df.set_index(idxs_nome)

    # calcula importância das features para o modelo
    weights = model.coef_[0]
    ftes = X_test.columns.tolist()
    fte_imps = pd.DataFrame({"ftes": ftes})
    fte_imps.loc[:, "importance"] = pow(math.e, weights)
    fte_imps = fte_imps.set_index("ftes").to_dict()

    reporting_df = pd.DataFrame({"model_accuracy": acc,
                                "model_params": str(params),
                                "test_probas": str(probas_df.to_dict(orient="index")),
                                "fte_importance": str(fte_imps),
                                "confusion_matrix": repr(cm)
                                }, index=[0])

    return reporting_df
