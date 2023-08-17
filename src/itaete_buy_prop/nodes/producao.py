# -*- coding: utf-8 -*-
from typing import Any, Dict, List

import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer

BASE_JOIN_COLS = ["id_cliente", "data_inferior", "data_alvo"]


def prod_laranja_sp_prm(df: pd.DataFrame, params: Dict[str, str]) -> pd.DataFrame:

    df.columns = [string_normalizer(col) for col in df.columns]
    _cols = "local"
    df = col_string_normalizer(df=df, _cols_to_normalize=[_cols])

    df = df[df[_cols] == params["local"]]
    df = df[["data_inicio", "data_fim", "produtividade_por_hectare"]] \
            .drop_duplicates() \
            .set_index(["data_inicio", "data_fim"])

    VALUE_COL = "produtividade_por_hectare"
    final_df = pd.DataFrame()

    for dt_inicio, dt_fim in df.index:
        datas_df = _cria_datas(data_inicio=dt_inicio, data_fim=dt_fim)
        value = _pega_valor_multiindex(df=df,
                                       filter_values=[dt_inicio, dt_fim],
                                       level=1,
                                       col=VALUE_COL)

        datas_df.loc[:, VALUE_COL] = value

        final_df = pd.concat([final_df, datas_df])

    final_df.loc[:, "data"] = final_df["data"].dt.date

    return final_df


def area_prod_cliente_prm(df: pd.DataFrame,
                          clientes_df: pd.DataFrame,
                          cultura: str) -> pd.DataFrame:

    df.columns = [string_normalizer(col) for col in df.columns]
    df = df[["sequencial", "cultura_principal", "area_2023", "area_2022", "area_2021"]] \
            .rename(columns={"sequencial": "id_cliente"}) \
            .drop_duplicates()

    df = df.merge(clientes_df[["id_cliente"]], on="id_cliente", how="inner")

    _cols = "cultura_principal"
    df = col_string_normalizer(df=df, _cols_to_normalize=[_cols])

    df = df[df[_cols] == cultura] \
            .drop(columns=[_cols])

    INDEX_COL = "id_cliente"
    final_df = pd.DataFrame()
    for col in df.set_index(INDEX_COL).columns:

        dfaux = df[[INDEX_COL, col]] \
                    .rename(columns={col: "area"})

        ano = col.split("_")[1]
        dt_inicio = pd.to_datetime(f"{ano}-01-01")
        dt_fim = pd.to_datetime(f"{ano}-12-31")

        datas_df = _cria_datas(data_inicio=dt_inicio, data_fim=dt_fim)
        nrows = datas_df.shape[0]

        for cliente, area in zip(dfaux["id_cliente"], dfaux["area"]):
            datas = datas_df["data"].copy()
            clientes = [cliente] * nrows
            areas = [area] * nrows

            _df = pd.DataFrame({"data": datas,
                                "id_cliente": clientes,
                                "area": areas})

            final_df = pd.concat([_df, final_df])

    final_df.loc[:, "data"] = final_df["data"].dt.date

    return final_df


def producao_fte(area_prod: pd.DataFrame,
                 prod_laranja: pd.DataFrame,
                 px_laranja: pd.DataFrame,
                 spine: pd.DataFrame) -> pd.DataFrame:

    df_laranja = prod_laranja.merge(px_laranja, on="data", how="inner")
    df = df_laranja.merge(area_prod, on="data", how="inner")

    df.loc[:, "receita_diaria"] = df["produtividade_por_hectare"] * df["area"] * df["preco_medio_laranja"]
    df = df.drop(columns=["produtividade_por_hectare", "preco_medio_laranja", "area"])

    fte_df = pd.DataFrame()
    for cliente, data_inferior, data_alvo in zip(spine["id_cliente"], spine["data_inferior"], spine["data_visita"]):

        dfaux = df[(df["id_cliente"] == cliente) & \
                (df["data"].between(data_inferior, data_alvo))]

        # se dataframe não tiver dado para o cliente na janela, então ignora o código abaixo
        if dfaux.empty:
            continue

        else:
            fte_dfaux = pd.DataFrame({"receita_media": dfaux["receita_diaria"].mean()}, index=[0])
            fte_dfaux.loc[:, ["id_cliente", "data_inferior", "data_alvo"]] = [cliente, data_inferior, data_alvo]

            fte_df = pd.concat([fte_df, fte_dfaux])

    assert fte_df.isnull().sum().sum() == 0, "Nulos na feature, revisar"
    assert fte_df.shape[0] == fte_df[BASE_JOIN_COLS].drop_duplicates().shape[0], \
                "Feature analise_fin duplicada, revisar"

    return fte_df


def _cria_datas(data_inicio: pd.Timestamp, data_fim: pd.Timestamp, freq: str = "D") -> pd.DataFrame:

    datas = pd.date_range(start=data_inicio, end=data_fim, freq=freq)
    df = pd.DataFrame({"data": datas})
    return df


def _pega_valor_multiindex(df: pd.DataFrame,
                           filter_values: List[Any],
                           level: int,
                           col: str) -> Any:

    aux = df[df.index.isin(filter_values, level=level)]
    return aux[col].values[0]
