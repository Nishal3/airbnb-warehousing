# Imporing libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, split, monotonically_increasing_id
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    LongType,
    DoubleType,
    DateType,
)

# from pyspark.errors import AnalysisException # Most common error for our script, but AWS Glue does not support it
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
sc = SparkContext()
glueContext = GlueContext(sc)

# Initializing Glue Logger
logger = glueContext.get_logger()

# Creating SparkSession and Job
spark = glueContext.spark_session
job = Job(glueContext)

# Initializing Job
job.init(args["JOB_NAME"], args)

logger.info("Starting Job...")

logger.info("Creating DynamicFrame from RDS...")

# Loading in RDS data from AWS Glue
try:
    RDSdata = glueContext.create_dynamic_frame.from_catalog(
        database="airbnb_untransformed_data",
        table_name="columbus_oh_listings_listings",
        transformation_ctx="RDSdata",
    )
except Exception as e:
    logger.error(f"Error creating DynamicFrame from RDS: {e}")
    raise e

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
except Exception as e:
    logger.error(f"Error transforming listings table with SQL: {e}")
    # Keep note we don't have to stop the SparkContext, Glue manages that itself
    raise e

logger.info("Transformed Listings Table With SQL")

# Splitting the bathroom column into two
property_baths_split = split(col("bathrooms"), " ", limit=2)

# Adding the two new columns
listingDf = listingDf.withColumn("bathroom_desc", property_baths_split.getItem(1))
listingDf = listingDf.withColumn(
    "bathrooms", property_baths_split.getItem(0).cast("decimal")
)

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
    "neighbourhood", "neighbourhood_overview", "neighbourhood_cleansed"
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

logger.info("Validating Host Table Schemas...")

# Schema Validation for Host Tables

hostQualificationsSchema = StructType(
    [
        StructField("hostQualifications_id", IntegerType(), False),
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
        StructField("hostListingsDiags_id", IntegerType(), False),
        StructField("calculated_host_listings_count", IntegerType(), True),
        StructField("calculated_host_listings_count_entire_homes", IntegerType(), True),
        StructField(
            "calculated_host_listings_count_private_rooms", IntegerType(), True
        ),
        StructField("calculated_host_listings_count_shared_rooms", IntegerType(), True),
    ]
)

# Schema Validation
try:
    hostQualificationsDf.schema == (hostQualificationsSchema)
    hostListingsDiagsDf.schema == (hostListingsDiagsSchema)

except Exception as e:
    logger.error(f"Error validating host schema: {e}")
    raise e

logger.info("Host Schemas Validated")

logger.info("Joining Host Tables...")

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
    logger.error(f"Error: {e}")
    raise e

logger.info("Host Tables Joined")

logger.info("Validating Host Table Schema After Join...")
# Validating Host Table Schema After Join
hostSchema = StructType(
    [
        StructField(
            "host_id", LongType(), True
        ),  # Nullable is True because we did not run monotonically_increasing_id
        StructField("hostQualifications_id", IntegerType(), True),
        StructField("hostListingsDiags_id", IntegerType(), True),
        StructField("host_url", StringType(), True),
        StructField("host_name", StringType(), True),
        StructField("host_since", DateType(), True),
        StructField("host_location", StringType(), True),
        StructField("host_about", StringType(), True),
        StructField("host_thumbnail_url", StringType(), True),
        StructField("host_picture_url", StringType(), True),
        StructField("host_neighbourhood", StringType(), True),
    ]
)

try:
    hostDf.schema == (hostSchema)
except Exception as e:
    logger.error(f"Error Validating Host Schema: {e}")
    raise e

logger.info("Host Table Schema After Join Validated")

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

propertyDfSchema = StructType(
    [
        StructField("property_id", LongType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("property_type", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("accommodates", IntegerType(), True),
        StructField("bathrooms", DecimalType(), True),
        StructField("beds", IntegerType(), True),
        StructField("daily_price", DecimalType(10, 2), True),
    ]
)

reviewsDiagnosticsDfSchema = StructType(
    [
        StructField("reviewsDiagnostics_id", LongType(), False),
        StructField("number_of_reviews", IntegerType(), True),
        StructField("number_of_reviews_ltm", IntegerType(), True),
        StructField("number_of_reviews_l30d", IntegerType(), True),
        StructField("first_review", DateType(), True),
        StructField("last_review", DateType(), True),
        StructField("review_scores_rating", IntegerType(), True),
        StructField("review_scores_accuracy", IntegerType(), True),
        StructField("review_scores_cleanliness", IntegerType(), True),
        StructField("review_scores_checkin", IntegerType(), True),
        StructField("review_scores_communication", IntegerType(), True),
        StructField("review_scores_location", IntegerType(), True),
        StructField("review_scores_value", IntegerType(), True),
        StructField("reviews_per_month", DecimalType(10, 2), True),
    ]
)

scrapingsDfSchema = StructType(
    [
        StructField("scraping_id", LongType(), False),
        StructField("scrape_id", LongType(), True),
        StructField("last_scraped", DateType(), True),
        StructField("source", StringType(), True),
    ]
)

neighbourhoodDfSchema = StructType(
    [
        StructField("neighbourhood_id", LongType(), False),
        StructField("neighbourhood", StringType(), True),
        StructField("neighbourhood_overview", StringType(), True),
        StructField("neighbourhood_cleansed", StringType(), True),
    ]
)

minMaxInsightsDfSchema = StructType(
    [
        StructField("minmax_insights_id", LongType(), False),
        StructField("minimum_nights", IntegerType(), True),
        StructField("maximum_nights", IntegerType(), True),
        StructField("minimum_minimum_nights", IntegerType(), True),
        StructField("maximum_minimum_nights", IntegerType(), True),
        StructField("minimum_maximum_nights", IntegerType(), True),
        StructField("maximum_maximum_nights", IntegerType(), True),
        StructField("minimum_nights_avg_ntm", DoubleType(), True),
        StructField("maximum_nights_avg_ntm", DoubleType(), True),
    ]
)

availabilityDfSchema = StructType(
    [
        StructField("availability_id", LongType(), False),
        StructField("has_availability", IntegerType(), True),
        StructField("availability_30", IntegerType(), True),
        StructField("availability_60", IntegerType(), True),
        StructField("availability_90", IntegerType(), True),
        StructField("availability_365", IntegerType(), True),
    ]
)

logger.info("Validating Schemas Before Final Join...")

try:
    propertyDf.schema == (propertyDfSchema)
    reviewsDiagnosticsDf.schema == (reviewsDiagnosticsDfSchema)
    scrapingsDf.schema == (scrapingsDfSchema)
    neighbourhoodDf.schema == (neighbourhoodDfSchema)
    minMaxInsightsDf.schema == (minMaxInsightsDfSchema)
    availabilityDf.schema == (availabilityDfSchema)
except Exception as e:
    logger.error(f"Error Validating Schemas Before Final Join: {e}")
    raise e

logger.info("Schemas Validated Before Final Join")

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
    logger.error(f"Error {e}")
    raise e

logger.info("Final Table Joined")

logger.info("Validating Listings DataFrame After Join...")

listingDfSchema = StructType(
    [
        StructField(
            "listing_id", LongType(), False
        ),  # Nullable because we did not run monotonically_increasing_id
        StructField("scraping_id", LongType(), True),
        StructField("host_id", LongType(), True),
        StructField("neighbourhood_id", LongType(), True),
        StructField("property_id", LongType(), True),
        StructField("minmax_insights_id", LongType(), True),
        StructField("avail_id", LongType(), True),
        StructField("rev_diag_id", LongType(), True),
        StructField("listing_url", StringType(), True),
        StructField("name", StringType(), True),
        StructField("picture_url", StringType(), True),
        StructField("license", StringType(), True),
        StructField("instant_bookable", IntegerType(), True),
    ]
)

try:
    listingDf.schema == (listingDfSchema)
except Exception as e:
    logger.error(f"Error Validating Listings Schema: {e}")
    raise e

logger.info("Listings Schema Validated After Join")

logger.info("Spark Transformations Complete")

logger.info("Converting Spark Dataframes to Glue Dynamic Frames...")

try:
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
    neighbourhoodDf = DynamicFrame.fromDF(
        neighbourhoodDf, glueContext, "neighbourhoodDf"
    )
    minMaxInsightsDf = DynamicFrame.fromDF(
        minMaxInsightsDf, glueContext, "minMaxInsightsDf"
    )
    availabilityDf = DynamicFrame.fromDF(availabilityDf, glueContext, "availabilityDf")

except Exception as e:  # Chance of error is unlikely, just in case I've put this
    logger.error(f"Error Converting Spark Dataframes to Glue Dynamic Frames: {e}")
    raise e

logger.info("Dynamic Frames Created")


logger.info("Writing Dynamic Frames to Redshift...")

# Writing to Redshift

try:
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

except Exception as e:  # Chance of error is unlikely, just in case I've put this
    logger.error(f"Error Writing Dynamic Frames to Redshift: {e}")
    raise e

logger.info("Dynamic Frames Written to Redshift")

job.commit()

logger.info("Job Complete")
