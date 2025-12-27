from pyspark.sql import SparkSession
from src.utils import load_config
from src.bronze_ingestion import autoloader_bronze
from src.silver_transformation import scd_type2_merge

def run_pipeline(config_path: str):
    spark = SparkSession.builder.appName("cdc-scd-pipeline").getOrCreate()
    config = load_config(config_path)

    autoloader_bronze(
        spark,
        config["source_path"],
        config["bronze_table_path"],
        config["bronze_checkpoint"],
        config["schema_location"],
        config["trigger_interval"]
    )

    (
        spark.readStream
        .format("delta")
        .load(config["bronze_table_path"])
        .writeStream
        .foreachBatch(
            lambda df, batchId: scd_type2_merge(
                spark,
                df,
                batchId,
                config["silver_table_path"],
                config["hash_columns"]
            )
        )
        .trigger(processingTime=config["trigger_interval"])
        .option("checkpointLocation", config["silver_checkpoint"])
        .start()
        .awaitTermination()
    )
