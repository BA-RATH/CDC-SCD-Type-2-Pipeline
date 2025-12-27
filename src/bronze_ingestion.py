from pyspark.sql.functions import input_file_name

def autoloader_bronze(
    spark,
    source_path,
    target_path,
    checkpoint_path,
    schema_location,
    trigger_interval
):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .withColumn("FileName", input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_path)
        .start(target_path)
    )
