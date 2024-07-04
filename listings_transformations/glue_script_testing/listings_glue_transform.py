# Imporing libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, split, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


# UDF to transform host_verifications to a 3 character string
# index 0 = email, index 1 = phone, index 2 = work_email. 1 = verified, 0 = not verified
def hqad_df_host_verifications_transform(verifs_list):
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

# Creating SparkSession and Job
spark = GlueContext.spark_session
job = Job(glueContext)

# Initializing Job
job.init(args["JOB_NAME"], args)

# Loading in RDS data from AWS Glue
RDSdata = glueContext.create_dynamic_frame.from_catalog(
    database="airbnb_untransformed_data",
    table_name="columbus_oh_listings_listings",
    transformation_ctx="RDSdata",
)

listingsDf = RDSdata.toDF()

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

listingsDf = listingsDf.drop(*columns_to_remove)

# Correcting Typo
listingsDf = listingsDf.withColumnRenamed(
    "neighborhood_overview", "neighbourhood_overview"
)

# Actual Transformation
listingsDf.createOrReplaceTempView("listings_df_view")

hqad_array_transform = udf(hqad_df_host_verifications_transform, StringType())

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

listingDf = spark.sql(listingDfQuery)

# Splitting the bathroom column into two
property_baths_split = split(col("bathrooms"), " ", limit=2)

# Adding the two new columns
listingDf = listingDf.withColumn("bathroom_desc", property_baths_split.getItem(1))
listingDf = listingDf.withColumn("bathrooms", property_baths_split.getItem(0))

# Transforming Host Verifications
listingDf = listingDf.withColumn(
    "host_verifications", hqad_array_transform(col("host_verifications"))
)

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
# hqadDf
hqadDf = hostDf.select(
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

hqadDf = hqadDf.dropDuplicates()

# Assigning ID
hqadDf = hqadDf.withColumn("hqad_id", monotonically_increasing_id())

# hldDf
hldDf = hostDf.select(
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
)

hldDf = hldDf.dropDuplicates()

# Assigning ID
hldDf = hldDf.withColumn("hld_id", monotonically_increasing_id())

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
    "price",
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
hqad_host_join_conditions = [
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

hld_host_join_conditions = [
    "calculated_host_listings_count",
    "calculated_host_listings_count_entire_homes",
    "calculated_host_listings_count_private_rooms",
    "calculated_host_listings_count_shared_rooms",
]

hostDf = (
    hostDf.join(hqadDf, on=hqad_host_join_conditions, how="left")
    .join(hldDf, on=hld_host_join_conditions, how="left")
    .select(
        "host_id",
        "hqad_id",
        "hld_id",
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


# Final Join
property_join_conditions = [
    "latitude",
    "longitude",
    "price",
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

listingDf = (
    listingDf.join(hostDf, on="host_id", how="left")
    .join(propertyDf, on=property_join_conditions, how="left")
    .join(reviewsDiagnosticsDf, on=reviews_join_conditions, how="left")
    .join(scrapingsDf, on=["last_scraped", "source"], how="left")
    .join(neighbourhoodDf, on=["neighbourhood", "neighbourhood_cleansed"], how="left")
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

# Converting Dataframes to Glue Dynamic Frames
listingDf = DynamicFrame.fromDF(listingDf, glueContext, "listingDf")
hostDf = DynamicFrame.fromDF(hostDf, glueContext, "hostDf")
hqadDf = DynamicFrame.fromDF(hqadDf, glueContext, "hqadDf")
hldDf = DynamicFrame.fromDF(hldDf, glueContext, "hldDf")
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
