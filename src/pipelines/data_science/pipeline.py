from kedro.pipeline import Pipeline, node

from .nodes import clean_data
from .parse_description import append_idf, convert_descriptions_to_term_counts


# Not covered since just returning pipeline
def create_pipeline(**kwargs):  # pragma: no cover
    return Pipeline(
        [
            node(
                func=clean_data,
                inputs=["indeed_data_scientist_postings"],
                outputs="ds_postings_cleaned",
            ),
            node(
                func=convert_descriptions_to_term_counts,
                inputs=["ds_postings_cleaned"],
                outputs="description_stats",
            ),
            node(func=append_idf, inputs="description_stats", outputs="tfidf_base",),
        ]
    )
