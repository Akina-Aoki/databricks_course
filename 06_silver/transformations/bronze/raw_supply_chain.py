from pyspark import pipelines as dp


BASE_DIR = "/Volumes/supply_chain_demo/default/raw"
# raw_file_path = "/Volumes/data/preparations/raw/DataCoSupplyChainDataset.csv"

# parse schema from csv file and use it for the streaming table
# as readStream processes data continously and can't look ahead of time to infer schema

schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_DIR}/data/DataCoSupplyChainDataset.csv")
    .schema
)

# https://docs.delta.io/delta-column-mapping/


@dp.table(
    name="supply_chain_demo.bronze.raw_supply_chain",
    # enable column mapping to handle invalid characters in original column names
    comment="Raw orders data as the bronze layer in medallion architecture",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_supply_chain():
    return (
        spark.readStream.format("csv")
        .options(header="true", inferSchema="true", encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}/data")
    )