# -*- coding: utf-8 -*-
import logging
import warnings
from typing import Any, Dict, Tuple

import pandas as pd
from sklearn.linear_model import LogisticRegression

from itaete_buy_prop.utils import optimize_params

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

TARGET_COL = ["label"]
# these cols were useful so far, but not anymore
INDEX_COL = "window_nbr"


def logreg_model_fit(master_table: pd.DataFrame,
                    train_test_cutoff_date: str,
                    model_params: Dict[str, Any],
                    logreg_optimize_params: bool,
                    logreg_default_params: Dict[str, Any]
                    ) -> Tuple[LogisticRegression,
                                pd.DataFrame, pd.DataFrame,
                                pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # X_train, y_train, X_test, y_test = mt_split_train_test(master_table=master_table,
    #                                                         index_col=INDEX_COL,
    #                                                         train_test_cutoff_date=train_test_cutoff_date,
    #                                                         target_col=TARGET_COL)
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
        params_opt = model_params.copy()

    params_opt.update(logreg_default_params)
    model.set_params(**params_opt)
    model.fit(X_train, y_train)

    df_params_opt = pd.DataFrame(params_opt, index=[0])

    return model, df_params_opt, X_train, y_train, X_test, y_test
