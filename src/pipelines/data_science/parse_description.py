from collections import Counter

import pandas as pd
from tqdm import tqdm

from ...definitions import TERM_FREQ_DIR


def convert_description_stats(data: pd.DataFrame) -> None:
    for row in tqdm(data.to_dict(orient="records"), desc="Row"):
        extract_description_stats(row)


def clean_description(text: str) -> str:
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")

    return text


def extract_description_stats(row: pd.Series) -> None:
    uid = row["link"]
    description = row["description"]

    description = clean_description(description)
    description = description.lower()

    term_counts = Counter(description.split())

    df = pd.DataFrame(term_counts.items(), columns=["term", "frequency"])
    df["corpus_id"] = uid

    df.to_parquet(TERM_FREQ_DIR / f"{uid}.parquet")
