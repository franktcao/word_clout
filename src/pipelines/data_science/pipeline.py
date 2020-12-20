from kedro.pipeline import Pipeline, node

from .nodes import clean_data


def create_pipeline(**kwargs):  # pragma: no cover
    return Pipeline(
        [node(clean_data, ["indeed_data_scientist_postings"], "ds_postings_cleaned",),]
    )
