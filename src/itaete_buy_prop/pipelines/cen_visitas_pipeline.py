# -*- coding: utf-8 -*-

from kedro.pipeline import Pipeline, node, pipeline

from itaete_buy_prop.nodes import cen_visitas_prm


def cen_visitas_pipeline() -> pipeline:

    _cen_visutas_pipeline = pipeline(
        Pipeline([
            node(func=cen_visitas_prm,
                inputs="raw_cen_visitas",
                outputs="prm_cen_visitas",
                name="run_cen_visitas_prm")
        ],
        tags=["cen_visitas_pipeline"]))

    return _cen_visutas_pipeline
