from pyspark import pipelines as dp

BASE_DIR = "/Volumes/supply_chain_demo/default/raw"

schema = spark.read.format("csv").options(header=True, inferSchema=True).load(f"{BASE_DIR}/data/DataCoSupplyChainDataset.csv").schema

@dp.table(name = "supply_chain_demo.bronze.raw_supply_chain", comment = "Raw Supply Chain Data X")

def raw_supply_chain():
    return spark.readStream.format("csv").options(header="true", encoding="UTF-8").schema(schema).load(f"(BASE_DIR)/data")