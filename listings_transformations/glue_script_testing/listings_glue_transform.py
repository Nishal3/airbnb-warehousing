# Imporing libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, split, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType
from pyspark.errors import AnalysisException
from pyspark.sql.types import StructField, StructType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


# UDF to transform host_verifications to a 3 character string
# index 0 = email, index 1 = phone, index 2 = work_email. 1 = verified, 0 = not verified
def hostQualifications_df_host_verifications_transform(verifs_list):
    res = ""

    verifs = ["email", "phone", "work_email"]

    for verif in verifs:
        if verif in verifs_list:
            res += "1"
        else:
            res += "0"

    return res


# UDF To convert t/f to 1/0 respectively
def t_f_to_1_0(t_f):
    if t_f == "t":
        return 1
    else:
        return 0


# Argument parsing
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Creating SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

# Creating SparkSession and Job
spark = GlueContext.spark_session
job = Job(glueContext)

# Initializing Job
job.init(args["JOB_NAME"], args)

logger.info("Starting Job...")

logger.info("Creating DynamicFrame from RDS...")

# Loading in RDS data from AWS Glue
RDSdata = glueContext.create_dynamic_frame.from_catalog(
    database="airbnb_untransformed_data",
    table_name="columbus_oh_listings_listings",
    transformation_ctx="RDSdata",
)
logger.info("Created DynamicFrame from RDS")

logger.info("Converting DynamicFrame to Spark DataFrame...")

# Converting to Spark DataFrame
listingsDf = RDSdata.toDF()

logger.info("Created Spark DataFrame")

logger.info("Transforming Spark DataFrame...")
# Transforming Listings Table

# Dropping Redundant/Empty Columns
columns_to_remove = [
    "calendar_last_scraped",
    "description",
    "calendar_updated",
    "bedrooms",
    "bathrooms",
    "neighbourhood_group_cleansed",
    "amenities",
]

logger.info("Dropping Redundant/Empty Columns...")

# Dropping Columns
listingsDf = listingsDf.drop(*columns_to_remove)

logger.info("Dropped Columns")

logger.info("Renaming Column...")
# Correcting Typo
listingsDf = listingsDf.withColumnRenamed(
    "neighborhood_overview", "neighbourhood_overview"
)

logger.info("Renamed Column")

# Actual Transformation
listingsDf.createOrReplaceTempView("listings_df_view")

hostQualifications_array_transform = udf(
    hostQualifications_df_host_verifications_transform, StringType()
)

truefalse_to_10 = udf(t_f_to_1_0, IntegerType())

listingDfQuery = """
                 SELECT id as listing_id,
                        host_id,
                        host_url,
                        host_name,
                        host_since,
                        host_location,
                        host_about,
                        host_thumbnail_url,
                        host_picture_url,
                        host_neighbourhood,
                        CASE host_response_time
                            WHEN 'within an hour' THEN 'H'
                            WHEN 'within a few hours' THEN 'FH'
                            WHEN 'within a dat' THEN 'D'
                            WHEN 'a few days or more' THEN 'D+'
                            ELSE NULL
                        END AS host_response_time, 
                        CAST(REPLACE(host_response_rate, '%', '') / 100 AS DECIMAL(3,2)) AS host_response_rate,
                        CAST(REPLACE(host_acceptance_rate, '%', '') / 100 AS DECIMAL(3,2)) AS host_acceptance_rate,
                        host_is_superhost,
                        host_listings_count, 
                        host_total_listings_count, 
                        host_verifications, 
                        host_has_profile_pic,
                        host_identity_verified,
                        calculated_host_listings_count,
                        calculated_host_listings_count_entire_homes,
                        calculated_host_listings_count_private_rooms,
                        calculated_host_listings_count_shared_rooms,
                        latitude,
                        longitude,
                        property_type,
                        room_type,
                        accommodates,
                        CASE
                            WHEN INSTR(bathrooms_text, 'Private half-bath') > 0 THEN '0.5 private bath'
                            WHEN INSTR(bathrooms_text, 'Half-bath') > 0 THEN '0.5 bath'
                            ELSE bathrooms_text
                        END as bathrooms,
                        beds,
                        cast(replace(price, '$', '') as decimal(10,2)) as daily_price,
                        number_of_reviews,
                        number_of_reviews_ltm,
                        number_of_reviews_l30d,
                        first_review,
                        last_review,
                        review_scores_rating,
                        review_scores_accuracy,
                        review_scores_cleanliness,
                        review_scores_checkin,
                        review_scores_communication,
                        review_scores_location,
                        review_scores_value,
                        reviews_per_month,
                        scrape_id,
                        last_scraped,
                        source,
                        neighbourhood,
                        neighbourhood_overview,
                        neighbourhood_cleansed,
                        maximum_nights,
                        minimum_nights,
                        minimum_minimum_nights,
                        maximum_minimum_nights,
                        minimum_maximum_nights,
                        maximum_maximum_nights,
                        minimum_nights_avg_ntm,
                        maximum_nights_avg_ntm,
                        has_availability,
                        availability_30,
                        availability_60,
                        availability_90,
                        availability_365,
                        listing_url,
                        name,
                        picture_url,
                        license,
                        instant_bookable
                    FROM listings_df_view
"""

logger.info("Transforming Listings Table With SQL...")

try:
    listingDf = spark.sql(listingDfQuery)
except AnalysisException as e:  # Most common error, failed to analyze SQL query
    logger.error(
        f"Error transforming listings table with SQL: {e}", exc_info=True
    )
    # Keep note we don't have to stop the SparkContext, Glue manages that itself
    raise e
except Exception as e:  # Any other error
    logger.error(
        f"Error transforming listings table with SQL: {e}", exc_info=True
    )
    raise e


logger.info("Transformed Listings Table With SQL")

# Splitting the bathroom column into two
property_baths_split = split(col("bathrooms"), " ", limit=2)

# Adding the two new columns
listingDf = listingDf.withColumn("bathroom_desc", property_baths_split.getItem(1))
listingDf = listingDf.withColumn("bathrooms", property_baths_split.getItem(0))

logger.info("Transforming Host Verifications...")
# Transforming Host Verifications
listingDf = listingDf.withColumn(
    "host_verifications", hostQualifications_array_transform(col("host_verifications"))
)

logger.info("Transformed Host Verifications")

logger.info("Transforming True/False to 1/0...")

# Transforming True/False to 1/0
listingDf = listingDf.withColumns(
    {
        "host_is_superhost": truefalse_to_10(col("host_is_superhost")),
        "host_has_profile_pic": truefalse_to_10(col("host_has_profile_pic")),
        "host_identity_verified": truefalse_to_10(col("host_identity_verified")),
        "has_availability": truefalse_to_10(col("has_availability")),
        "instant_bookable": truefalse_to_10(col("instant_bookable")),
    }
)

logger.info("Transformed True/False to 1/0")

# Recreating Listings View
listingDf.createOrReplaceTempView("listing_df_view")

# Creating Tables for Each Dimension

# Host Table
hostDf = listingDf.select(
    "host_id",
    "host_url",
    "host_name",
    "host_since",
    "host_location",
    "host_about",
    "host_thumbnail_url",
    "host_picture_url",
    "host_neighbourhood",
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_listings_count",
    "host_total_listings_count",
    "host_verifications",
    "host_has_profile_pic",
    "host_identity_verified",
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
)

hostDf = hostDf.dropDuplicates()

# Host Dimensions
# hostQualificationsDf
hostQualificationsDf = hostDf.select(
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_listings_count",
    "host_total_listings_count",
    "host_verifications",
    "host_has_profile_pic",
    "host_identity_verified",
)

hostQualificationsDf = hostQualificationsDf.dropDuplicates()

# Assigning ID
hostQualificationsDf = hostQualificationsDf.withColumn(
    "hostQualifications_id", monotonically_increasing_id()
)

# hostListingsDiagsDf
hostListingsDiagsDf = hostDf.select(
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
)

hostListingsDiagsDf = hostListingsDiagsDf.dropDuplicates()

# Assigning ID
hostListingsDiagsDf = hostListingsDiagsDf.withColumn(
    "hostListingsDiags_id", monotonically_increasing_id()
)

# Property Table
propertyDf = listingDf.select(
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathroom_desc",
    "bathrooms",
    "beds",
    "daily_price",
)

propertyDf = propertyDf.dropDuplicates()

# Assigning ID
propertyDf = propertyDf.withColumn("property_id", monotonically_increasing_id())

# Reviews Table
reviewsDiagnosticsDf = listingDf.select(
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "reviews_per_month",
)

reviewsDiagnosticsDf = reviewsDiagnosticsDf.dropDuplicates()

# Assigning ID
reviewsDiagnosticsDf = reviewsDiagnosticsDf.withColumn(
    "rev_diag_id", monotonically_increasing_id()
)

# Scrapings Table
scrapingsDf = listingDf.select("scrape_id", "last_scraped", "source")

scrapingsDf = scrapingsDf.dropDuplicates()

# Assigning ID
scrapingsDf = scrapingsDf.withColumn("scraping_id", monotonically_increasing_id())

# Neighbourhood Table
neighbourhoodDf = listingDf.select(
    "neighbourhood", "neighbourhood_cleansed", "neighbourhood_cleansed"
)

neighbourhoodDf = neighbourhoodDf.dropDuplicates()

# Assigning ID
neighbourhoodDf = neighbourhoodDf.withColumn(
    "neighbourhood_id", monotonically_increasing_id()
)

# Min Max Insights Table
minMaxInsightsDf = listingDf.select(
    "minimum_nights",
    "maximum_nights",
    "minimum_minimum_nights",
    "maximum_minimum_nights",
    "minimum_maximum_nights",
    "maximum_maximum_nights",
    "minimum_nights_avg_ntm",
    "maximum_nights_avg_ntm",
)

minMaxInsightsDf = minMaxInsightsDf.dropDuplicates()

# Assigning ID
minMaxInsightsDf = minMaxInsightsDf.withColumn(
    "minmax_insights_id", monotonically_increasing_id()
)

# Availability Table
availabilityDf = listingDf.select(
    "has_availability",
    "availability_30",
    "availability_60",
    "availability_90",
    "availability_365",
)

availabilityDf = availabilityDf.dropDuplicates()

# Assigning ID
availabilityDf = availabilityDf.withColumn("avail_id", monotonically_increasing_id())

# Joining Tables

# Host Tables
hostQualifications_host_join_conditions = [
    "host_response_time",
    "host_response_rate",
    "host_acceptance_rate",
    "host_is_superhost",
    "host_listings_count",
    "host_total_listings_count",
    "host_verifications",
    "host_has_profile_pic",
    "host_identity_verified",
]

hostListingsDiags_host_join_conditions = [
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
]

logger.info("Joining Host Tables...")

# Schema Validation for Host Tables
hostSchema = StructType(
    [
        StructField("host_id", IntegerType(), True),
        StructField("host_url", StringType(), True),
        StructField("host_name", StringType(), True),
        StructField("host_since", StringType(), True),
        StructField("host_location", StringType(), True),
        StructField("host_about", StringType(), True),
        StructField("host_thumbnail_url", StringType(), True),
        StructField("host_picture_url", StringType(), True),
        StructField("host_neighbourhood", StringType(), True),
    ]
)

hostQualificationsSchema = StructType(
    [
        StructField("hostQualifications_id", IntegerType(), True),
        StructField("host_response_time", StringType(), True),
        StructField("host_response_rate", StringType(), True),
        StructField("host_acceptance_rate", StringType(), True),
        StructField("host_is_superhost", StringType(), True),
        StructField("host_listings_count", IntegerType(), True),
        StructField("host_total_listings_count", IntegerType(), True),
        StructField("host_verifications", StringType(), True),
        StructField("host_has_profile_pic", StringType(), True),
        StructField("host_identity_verified", StringType(), True),
    ]
)

hostListingsDiagsSchema = StructType(
    [
        StructField("hostListingsDiags_id", IntegerType(), True),
        StructField("calculated_host_listings_count", IntegerType(), True),
        StructField("calculated_host_listings_count_entire_homes", IntegerType(), True),
        StructField(
            "calculated_host_listings_count_private_rooms", IntegerType(), True
        ),
        StructField("calculated_host_listings_count_shared_rooms", IntegerType(), True),
    ]
)

try:
    hostDf.schema.validate(hostSchema)
    hostQualificationsDf.schema.validate(hostQualificationsSchema)

except AnalysisException as e:
    logger.error(f"Error validating host schema: {e}", exc_info=True)
    raise e

try:
    hostDf = (
        hostDf.join(
            hostQualificationsDf, on=hostQualifications_host_join_conditions, how="left"
        )
        .join(
            hostListingsDiagsDf, on=hostListingsDiags_host_join_conditions, how="left"
        )
        .select(
            "host_id",
            "hostQualifications_id",
            "hostListingsDiags_id",
            "host_url",
            "host_name",
            "host_since",
            "host_location",
            "host_about",
            "host_thumbnail_url",
            "host_picture_url",
            "host_neighbourhood",
        )
    )
except Exception as e:  # Chance of error is unlikely, just in case I've put this
    logger.error(f"Error: {e}", exc_info=True)
    raise e

logger.info("Host Tables Joined")

# Final Join
property_join_conditions = [
    "latitude",
    "longitude",
    "daily_price",
    "beds",
    "bathroom_desc",
    "accommodates",
]

reviews_join_conditions = [
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "first_review",
    "last_review",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "reviews_per_month",
]

minmax_join_conditions = [
    "minimum_nights",
    "maximum_nights",
    "minimum_minimum_nights",
    "maximum_minimum_nights",
    "minimum_maximum_nights",
    "maximum_maximum_nights",
    "minimum_nights_avg_ntm",
    "maximum_nights_avg_ntm",
]

logger.info("Joining Final Table...")

try:
    listingDf = (
        listingDf.join(hostDf, on="host_id", how="left")
        .join(propertyDf, on=property_join_conditions, how="left")
        .join(reviewsDiagnosticsDf, on=reviews_join_conditions, how="left")
        .join(scrapingsDf, on=["last_scraped", "source"], how="left")
        .join(
            neighbourhoodDf, on=["neighbourhood", "neighbourhood_cleansed"], how="left"
        )
        .join(minMaxInsightsDf, on=minmax_join_conditions, how="left")
        .join(
            availabilityDf,
            on=[
                "has_availability",
                "availability_30",
                "availability_60",
                "availability_90",
                "availability_365",
            ],
            how="left",
        )
        .select(
            "listing_id",
            "scraping_id",
            "host_id",
            "neighbourhood_id",
            "property_id",
            "minmax_insights_id",
            "avail_id",
            "rev_diag_id",
            "listing_url",
            "name",
            "picture_url",
            "license",
            "instant_bookable",
        )
    )
except Exception as e:  # Chance of error is unlikely, just in case I've put this
    logger.info(f"Error {e}", exc_info=True)
    raise e

logger.info("Final Table Joined")

logger.info("Converting Spark Dataframes to Glue Dynamic Frames...")

# Converting Dataframes to Glue Dynamic Frames
listingDf = DynamicFrame.fromDF(listingDf, glueContext, "listingDf")
hostDf = DynamicFrame.fromDF(hostDf, glueContext, "hostDf")
hostQualificationsDf = DynamicFrame.fromDF(
    hostQualificationsDf, glueContext, "hostQualificationsDf"
)
hostListingsDiagsDf = DynamicFrame.fromDF(
    hostListingsDiagsDf, glueContext, "hostListingsDiagsDf"
)
propertyDf = DynamicFrame.fromDF(propertyDf, glueContext, "propertyDf")
reviewsDiagnosticsDf = DynamicFrame.fromDF(
    reviewsDiagnosticsDf, glueContext, "reviewsDiagnosticsDf"
)
scrapingsDf = DynamicFrame.fromDF(scrapingsDf, glueContext, "scrapingsDf")
neighbourhoodDf = DynamicFrame.fromDF(neighbourhoodDf, glueContext, "neighbourhoodDf")
minMaxInsightsDf = DynamicFrame.fromDF(
    minMaxInsightsDf, glueContext, "minMaxInsightsDf"
)
availabilityDf = DynamicFrame.fromDF(availabilityDf, glueContext, "availabilityDf")

logger.info("Dynamic Frames Created")


logger.info("Writing Dynamic Framces to Redshift...")

# Writing to Redshift
glueContext.write_dynamic_frame.from_catalog(
    frame=listingDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_listings",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=hostDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_host",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=hostQualificationsDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_hqad",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=hostListingsDiagsDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_hld",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=propertyDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_property",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=reviewsDiagnosticsDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_reviews_diagnostics",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=scrapingsDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_scrapings",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=neighbourhoodDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_neighbourhoods",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=minMaxInsightsDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_minmax_insights",
)

glueContext.write_dynamic_frame.from_catalog(
    frame=availabilityDf,
    database="airbnb_transformed_data",
    table="transformed_listings_data_public_availability_info",
)

logger.info("Finished")

logger.info("Job Complete")
