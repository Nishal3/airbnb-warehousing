import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
GlueContext = GlueContext(sc)

spark = GlueContext.spark_session
job = Job(glueContext)

cataloginput = glueContext.getCatalogSource(
    database="airbnb", table="listings", transformationContext="cataloginput"
)

spark = SparkSession.builder.appnName("Airbnb_Transform").getOrCreate()


listing_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .option("escape", '"')
    .option("quote", '"')
    .option("escape", "\\")
    .option("multiLine", "true")
    .load("s3://airbnb-data-bucket/listings.csv")
)
