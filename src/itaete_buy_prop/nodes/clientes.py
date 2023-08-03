# -*- coding: utf-8 -*-
from typing import Dict

import pandas as pd

from itaete_buy_prop.utils import col_string_normalizer, string_normalizer


def clientes_prm(df_org_rec_frota: pd.DataFrame,
                 df_anls_fin: pd.DataFrame,
                 params: Dict[str, str]) -> pd.DataFrame:

	df_org_rec_frota = df_org_rec_frota[["SEQPESSOA","TIPO"]] \
						.drop_duplicates() \
						.rename(columns={"SEQPESSOA": "id_cliente"})

	df_anls_fin = df_anls_fin[["SeqPessoa", "Empresa", "Tipo produto"]] \
						.drop_duplicates() \
						.rename(columns={"SeqPessoa": "id_cliente"})

	df = df_org_rec_frota.merge(df_anls_fin, on="id_cliente", how="inner")

	df.columns = [string_normalizer(col) for col in df.columns]
	_col = ["tipo", "empresa", "tipo_produto"]
	df = col_string_normalizer(df=df, _cols_to_normalize=_col)

	df = df[(df["tipo"] == params["cultura"]) & \
	 		(df["empresa"] == params["empresa"]) & \
			(df["tipo_produto"] == params["produto"])
			]

	assert df.shape[0] == df[["id_cliente"]].drop_duplicates().shape[0], "id_cliente não é único, revisar"
	assert df.isnull().sum().sum() == 0, "Nulos no primary, revisar"

	return df
