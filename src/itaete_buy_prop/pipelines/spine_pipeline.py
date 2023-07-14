# -*- coding: utf-8 -*-
"""Pipeline object having mainly func, inputs and outputs
`func` is the python function to be executed
`inputs` are either datasets or parameters defined in the conf/base directory
`outputs` are datasets defined in the catalog
- if the output is not defined in the catalog, then it becomes a MemoryDataSet
- MemoryDataSet persists as long as the Session is active
"""

from kedro.pipeline import Pipeline, node, pipeline
from itaete_buy_prop.nodes import spine_prm, spine_labeling, spine_preprocessing


def spine_pipeline() -> pipeline:

    _spine_pipeline = pipeline(
        Pipeline([
            node(func=spine_prm,
                inputs="raw_crm_bi",
                outputs="prm_spine",
                name="run_spine_prm"),

            node(func=spine_preprocessing,
                inputs=["prm_spine", "params:spine_params"],
                outputs="preprocessing_spine",
                name="run_spine_preprocessing"),

            node(func=spine_labeling,
                inputs="preprocessing_spine",
                outputs="label_spine",
                name="run_spine_label")
        ],
        tags=["spine_pipeline"]))

    return _spine_pipeline
