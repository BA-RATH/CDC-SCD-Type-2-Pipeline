from delta.tables import DeltaTable
from pyspark.sql.functions import (
    to_timestamp, col, lit, row_number
)
from pyspark.sql.window import Window
from src.utils import add_hash_column

def scd_type2_merge(
    spark,
    microBatchDF,
    batchId,
    target_path,
    hash_columns
):
    df = microBatchDF.withColumn(
        "event_time", to_timestamp("event_time")
    )

    df = add_hash_column(df, hash_columns)

    w = Window.partitionBy("customer_id").orderBy(col("event_time").desc())
    df = (
        df.withColumn("rn", row_number().over(w))
          .filter(col("rn") == 1)
          .drop("rn")
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df.withColumn("is_current", lit(True))
              .withColumn("effective_from", col("event_time"))
              .withColumn("effective_to", lit(None).cast("timestamp"))
              .write.format("delta")
              .save(target_path)
        )
        return

    target = DeltaTable.forPath(spark, target_path)

    (
        target.alias("t")
        .merge(
            df.alias("s"),
            "t.customer_id = s.customer_id AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={
                "is_current": "false",
                "effective_to": "s.event_time"
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "s.customer_id",
                "name": "s.name",
                "email": "s.email",
                "address": "s.address",
                "row_hash": "s.row_hash",
                "is_current": "true",
                "effective_from": "s.event_time",
                "effective_to": "null"
            }
        )
        .execute()
    )
