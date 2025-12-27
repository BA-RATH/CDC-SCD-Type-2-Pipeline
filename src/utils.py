import yaml
from pyspark.sql.functions import sha2, concat_ws, col

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def add_hash_column(df, columns, hash_col="row_hash"):
    return df.withColumn(
        hash_col,
        sha2(concat_ws("||", *[col(c) for c in columns]), 256)
    )
