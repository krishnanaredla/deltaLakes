from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import argparse
from delta.tables import *

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", help="data file")

args = parser.parse_args()


spark = (
    SparkSession.builder.appName("deltaSCD")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)




pkey = "customerId"
definingCol = "createdOn"
dataKeys = ["address", "city"]
path = "/data/DemoData/"
hashKeyCols = ["customerId"]
hashDataCols = dataKeys

def performFirstLoad(df: DataFrame) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def performDeltaLoad(df: DataFrame) -> None:
    oldData = DeltaTable.forPath(spark, path)
    dataToInsert = (
        df.alias("updates")
        .join(oldData.toDF().alias("oldData"), pkey)
        .where("oldData.endDate is null  AND updates.hashData <> oldData.hashData")
    )
    stagedUpdates = dataToInsert.selectExpr("NULL as mergeKey", "updates.*").union(
        df.alias("updates").selectExpr("updates.{0} as mergeKey".format(pkey), "*")
    )
    oldData.alias("oldData").merge(
        stagedUpdates.alias("staged_updates"),
        "oldData.endDate is null and oldData.{0} = mergeKey".format(pkey),
    ).whenMatchedUpdate(
        condition=" oldData.hashData <> staged_updates.hashData",
        set={"endDate": "staged_updates.startDate"},
    ).whenNotMatchedInsert(
        values={
            "{0}".format(str(col_name)): "staged_updates.{0}".format(str(col_name))
            for col_name in stagedUpdates.columns
            if col_name not in "mergeKey"
        }
    ).execute()


def processData(filePath: str) -> None:
    df = spark.read.parquet(filePath)
    windowBy = Window.partitionBy(col(pkey)).orderBy(col(definingCol))
    df = (
        df.withColumn(
            "hashKey",
            sha2(
                concat_ws("|", *map(lambda key_cols: col(key_cols), hashKeyCols)), 256
            ),
        )
        .withColumn(
            "hashData",
            sha2(
                concat_ws("|", *map(lambda key_cols: col(key_cols), hashDataCols)), 256
            ),
        )
        .withColumn("startDate", col(definingCol))
        .withColumn("endDate", lead(col(definingCol)).over(windowBy))
    )
    if not DeltaTable.isDeltaTable(spark, path):
        performFirstLoad(df)
    else:
        performDeltaLoad(df)


if __name__ == "__main__":
    processData(args.file)
