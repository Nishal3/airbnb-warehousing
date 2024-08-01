# JUPYTER NOTEBOOK IS MORE RECENT!!!!!!!!!!!!
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
    if not t_f:
        return 0
    elif "t" in t_f:
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

listingsDfQuery = """
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
                        REPLACE(host_response_rate, '%', '') / 100 AS host_response_rate,
                        REPLACE(host_acceptance_rate, '%', '') / 100 AS host_acceptance_rate,
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
                        REPLACE(price, '$', '') as daily_price,
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

try:
    listingsDf = spark.sql(listingsDfQuery)
except Exception as e:
    logger.error(f"Error transforming listings table with SQL: {e}")
    # Keep note we don't have to stop the SparkContext, Glue manages that itself
    raise e

logger.info("Transformed Listings Table With SQL")

# Splitting the bathroom column into two
property_baths_split = split(col("bathrooms"), " ", limit=2)

# Adding the two new columns
listingsDf = listingsDf.withColumn("bathroom_desc", property_baths_split.getItem(1))
listingsDf = listingsDf.withColumn(
    "bathrooms", property_baths_split.getItem(0).cast("decimal")
)

logger.info("Casting columns to correct data types...")

logger.info("Transforming Host Verifications...")
# Transforming Host Verifications
listingsDf = listingsDf.withColumn(
    "host_verifications", hostQualifications_array_transform(col("host_verifications"))
)

logger.info("Transformed Host Verifications")

logger.info("Transforming True/False to 1/0...")

# Transforming True/False to 1/0
listingsDf = listingsDf.withColumns(
    {
        "host_is_superhost": truefalse_to_10(col("host_is_superhost")),
        "host_has_profile_pic": truefalse_to_10(col("host_has_profile_pic")),
        "host_identity_verified": truefalse_to_10(col("host_identity_verified")),
        "has_availability": truefalse_to_10(col("has_availability")),
        "instant_bookable": truefalse_to_10(col("instant_bookable")),
    }
)

logger.info("Transformed True/False to 1/0")

logger.info("Filling Nulls in Listings Table...")
try:
    listingsDf = listingsDf.na.fill(
        {
            "host_url": "N/A",
            "host_name": "N/A",
            "host_about": "N/A",
            "host_since": "00-00-00",
        }
    )
except Exception as e:
    logger.error(f"Error filling nulls in listings table: {e}")
    raise e

logger.info("Casting columns to correct data types...")

# Casting columns to correct data types
listingsDf = listingsDf.withColumns(
    {
        "listing_id": col("listing_id").cast(LongType()),
        "host_id": col("host_id").cast(LongType()),
        "host_url": col("host_url").cast(StringType()),
        "host_name": col("host_name").cast(StringType()),
        "host_since": col("host_since").cast(DateType()),
        "host_location": col("host_location").cast(StringType()),
        "host_about": col("host_about").cast(StringType()),
        "host_thumbnail_url": col("host_thumbnail_url").cast(StringType()),
        "host_picture_url": col("host_picture_url").cast(StringType()),
        "host_neighbourhood": col("host_neighbourhood").cast(StringType()),
        "host_response_time": col("host_response_time").cast(StringType()),
        "host_response_rate": col("host_response_rate").cast(DecimalType(3, 2)),
        "host_acceptance_rate": col("host_acceptance_rate").cast(DecimalType(3, 2)),
        "host_is_superhost": col("host_is_superhost").cast(IntegerType()),
        "host_listings_count": col("host_listings_count").cast(IntegerType()),
        "host_total_listings_count": col("host_total_listings_count").cast(
            IntegerType()
        ),
        "host_verifications": col("host_verifications").cast(StringType()),
        "host_has_profile_pic": col("host_has_profile_pic").cast(IntegerType()),
        "host_identity_verified": col("host_identity_verified").cast(IntegerType()),
        "calculated_host_listings_count": col("calculated_host_listings_count").cast(
            IntegerType()
        ),
        "calculated_host_listings_count_entire_homes": col(
            "calculated_host_listings_count_entire_homes"
        ).cast(IntegerType()),
        "calculated_host_listings_count_private_rooms": col(
            "calculated_host_listings_count_private_rooms"
        ).cast(IntegerType()),
        "calculated_host_listings_count_shared_rooms": col(
            "calculated_host_listings_count_shared_rooms"
        ).cast(IntegerType()),
        "latitude": col("latitude").cast(DecimalType(18, 15)),
        "longitude": col("longitude").cast(DecimalType(18, 15)),
        "property_type": col("property_type").cast(StringType()),
        "room_type": col("room_type").cast(StringType()),
        "accommodates": col("accommodates").cast(IntegerType()),
        "bathrooms": col("bathrooms").cast(DecimalType(5, 1)),
        "bathrooms_desc": col("bathroom_desc").cast(StringType()),
        "beds": col("beds").cast(IntegerType()),
        "daily_price": col("daily_price").cast(DecimalType(10, 2)),
        "number_of_reviews": col("number_of_reviews").cast(IntegerType()),
        "number_of_reviews_ltm": col("number_of_reviews_ltm").cast(IntegerType()),
        "number_of_reviews_l30d": col("number_of_reviews_l30d").cast(IntegerType()),
        "first_review": col("first_review").cast(DateType()),
        "last_review": col("last_review").cast(DateType()),
        "review_scores_rating": col("review_scores_rating").cast(DecimalType(3, 2)),
        "review_scores_accuracy": col("review_scores_accuracy").cast(DecimalType(3, 2)),
        "review_scores_cleanliness": col("review_scores_cleanliness").cast(
            DecimalType(3, 2)
        ),
        "review_scores_checkin": col("review_scores_checkin").cast(DecimalType(3, 2)),
        "review_scores_communication": col("review_scores_communication").cast(
            DecimalType(3, 2)
        ),
        "review_scores_location": col("review_scores_location").cast(DecimalType(3, 2)),
        "review_scores_value": col("review_scores_value").cast(DecimalType(3, 2)),
        "reviews_per_month": col("reviews_per_month").cast(DecimalType(3, 2)),
        "scrape_id": col("scrape_id").cast(LongType()),
        "last_scraped": col("last_scraped").cast(DateType()),
        "source": col("source").cast(StringType()),
        "neighbourhood": col("neighbourhood").cast(StringType()),
        "neighbourhood_overview": col("neighbourhood_overview").cast(StringType()),
        "neighbourhood_cleansed": col("neighbourhood_cleansed").cast(StringType()),
        "maximum_nights": col("maximum_nights").cast(IntegerType()),
        "minimum_nights": col("minimum_nights").cast(IntegerType()),
        "minimum_minimum_nights": col("minimum_minimum_nights").cast(IntegerType()),
        "maximum_minimum_nights": col("maximum_minimum_nights").cast(IntegerType()),
        "minimum_maximum_nights": col("minimum_maximum_nights").cast(IntegerType()),
        "maximum_maximum_nights": col("maximum_maximum_nights").cast(IntegerType()),
        "minimum_nights_avg_ntm": col("minimum_nights_avg_ntm").cast(DecimalType()),
        "maximum_nights_avg_ntm": col("maximum_nights_avg_ntm").cast(DecimalType()),
        "has_availability": col("has_availability").cast(IntegerType()),
        "availability_30": col("availability_30").cast(IntegerType()),
        "availability_60": col("availability_60").cast(IntegerType()),
        "availability_90": col("availability_90").cast(IntegerType()),
        "availability_365": col("availability_365").cast(IntegerType()),
        "listing_url": col("listing_url").cast(StringType()),
        "name": col("name").cast(StringType()),
        "picture_url": col("picture_url").cast(StringType()),
        "license": col("license").cast(StringType()),
        "instant_bookable": col("instant_bookable").cast(IntegerType()),
    }
)

logger.info("Casted columns to correct data types")

# Creating Tables for Each Dimension

# Host Table
hostDf = listingsDf.select(
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
    "host_quals_id", monotonically_increasing_id()
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
    "host_listings_diags_id", monotonically_increasing_id()
)

# Property Table
propertyDf = listingsDf.select(
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
reviewsDiagnosticsDf = listingsDf.select(
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
scrapingsDf = listingsDf.select("scrape_id", "last_scraped", "source")

scrapingsDf = scrapingsDf.dropDuplicates()

# Assigning ID
scrapingsDf = scrapingsDf.withColumn("scraping_id", monotonically_increasing_id())

# Neighbourhood Table
neighbourhoodDf = listingsDf.select(
    "neighbourhood", "neighbourhood_overview", "neighbourhood_cleansed"
)

neighbourhoodDf = neighbourhoodDf.dropDuplicates()

# Assigning ID
neighbourhoodDf = neighbourhoodDf.withColumn(
    "neighbourhood_id", monotonically_increasing_id()
)

# Min Max Insights Table
minMaxInsightsDf = listingsDf.select(
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
availabilityDf = listingsDf.select(
    "has_availability",
    "availability_30",
    "availability_60",
    "availability_90",
    "availability_365",
)

availabilityDf = availabilityDf.dropDuplicates()

# Assigning ID
availabilityDf = availabilityDf.withColumn(
    "avail_info_id", monotonically_increasing_id()
)

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
        StructField("host_quals_id", LongType(), False),
        StructField("host_response_time", StringType(), True),
        StructField("host_response_rate", DecimalType(3, 2), True),
        StructField("host_acceptance_rate", DecimalType(3, 2), True),
        StructField("host_is_superhost", IntegerType(), True),
        StructField("host_listings_count", IntegerType(), True),
        StructField("host_total_listings_count", IntegerType(), True),
        StructField("host_verifications", StringType(), True),
        StructField("host_has_profile_pic", StringType(), True),
        StructField("host_identity_verified", StringType(), True),
    ]
)

hostListingsDiagsSchema = StructType(
    [
        StructField("host_listings_diags_id", LongType(), False),
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
            "host_quals_id",
            "host_listings_diags_id",
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
        StructField("host_quals_id", LongType(), True),
        StructField("host_listings_diags_id", LongType(), True),
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
        StructField("latitude", DecimalType(18, 15), True),
        StructField("longitude", DecimalType(18, 15), True),
        StructField("property_type", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("accommodates", IntegerType(), True),
        StructField("bathrooms", DecimalType(5, 1), True),
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
        StructField("review_scores_rating", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
        StructField("review_scores_accuracy", DecimalType(3, 2), True),
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
        StructField("minimum_nights_avg_ntm", DecimalType(), True),
        StructField("maximum_nights_avg_ntm", DecimalType(), True),
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
    listingsDf = (
        listingsDf.join(hostDf, on="host_id", how="left")
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
            "avail_info_id",
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

listingsDfSchema = StructType(
    [
        StructField(
            "listing_id", LongType(), False
        ),  # Nullable because we did not run monotonically_increasing_id
        StructField("scraping_id", LongType(), True),
        StructField("host_id", LongType(), True),
        StructField("neighbourhood_id", LongType(), True),
        StructField("property_id", LongType(), True),
        StructField("minmax_insights_id", LongType(), True),
        StructField("avail_info_id", LongType(), True),
        StructField("rev_diag_id", LongType(), True),
        StructField("listing_url", StringType(), True),
        StructField("name", StringType(), True),
        StructField("picture_url", StringType(), True),
        StructField("license", StringType(), True),
        StructField("instant_bookable", IntegerType(), True),
    ]
)

try:
    listingsDf.schema == (listingsDfSchema)
except Exception as e:
    logger.error(f"Error Validating Listings Schema: {e}")
    raise e

logger.info("Listings Schema Validated After Join")

logger.info("Spark Transformations Complete")

logger.info("Converting Spark Dataframes to Glue Dynamic Frames...")

try:
    # Converting Dataframes to Glue Dynamic Frames
    listingsDf = DynamicFrame.fromDF(listingsDf, glueContext, "listingsDf")
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
        frame=hostQualificationsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_host_quals_diags",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=hostListingsDiagsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_host_listings_diags",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=propertyDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_property",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=reviewsDiagnosticsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_reviews_diagnostics",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=scrapingsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_scrapings",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=neighbourhoodDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_neighbourhoods",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=minMaxInsightsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_minmax_insights",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=availabilityDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_availability_info",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=hostDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_host",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )

    glueContext.write_dynamic_frame.from_catalog(
        frame=listingsDf,
        database="airbnb_transformed_data",
        table_name="transformed_columbus_oh_listings_data_public_listings",
        redshift_tmp_dir="s3://nishal-airbnb-transformed-redshift-data/temp",
    )


except Exception as e:  # Chance of error is unlikely, just in case I've put this
    logger.error(f"Error Writing Dynamic Frames to Redshift: {e}")
    raise e

logger.info("Dynamic Frames Written to Redshift")

job.commit()

logger.info("Job Complete")
