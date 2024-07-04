CREATE TABLE IF NOT EXISTS scrapings (
    scraping_id BIGINT PRIMARY KEY,
    scrape_id BIGINT,
    last_scraped DATE,
    source INT
);

CREATE TABLE IF NOT EXISTS neighbourhoods (
    neighbourhood_id BIGINT PRIMARY KEY,
    neighbourhood VARCHAR(30),
    neighbourhood_overview TEXT,
    neighbourhood_cleansed VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS minmax_insights (
    minmax_insights_id BIGINT PRIMARY KEY,
    maximum_nights INT,
    minimum_nights INT,
    minimum_minimum_nights INT,
    maximum_minimum_nights INT,
    minimum_maximum_nights INT,
    maximum_maximum_nights INT,
    minimum_nights_avg_ntm INT,
    maximum_nights_avg_ntm INT
);

CREATE TABLE IF NOT EXISTS availability_info (
    avail_id BIGINT PRIMARY KEY,
    has_availability INT,
    availability_30 INT,
    availability_60 INT,
    availability_90 INT,
    availability_365 INT
);

CREATE TABLE IF NOT EXISTS hld (
    hld_id BIGINT PRIMARY KEY,
    calculated_host_listings_count INT,
    calculated_host_listings_count_entire_homes INT,
    calculated_host_listings_count_private_rooms INT,
    calculated_host_listings_count_shared_rooms INT
);

CREATE TABLE IF NOT EXISTS hqad (
    hqad_id BIGINT PRIMARY KEY,
    host_response_time VARCHAR(2),
    host_response_rate NUMERIC(3, 2),
    host_acceptance_rate NUMERIC(3, 2),
    host_is_superhost INT,
    host_listings_count INT,
    host_total_listings_count INT,
    host_verifications VARCHAR(3),
    host_has_profile_pic INT,
    host_identity_verified INT
);

CREATE TABLE IF NOT EXISTS host (
    host_id BIGINT PRIMARY KEY,
    hld_id INT,
    hqad_id INT,
    host_url VARCHAR(50),
    host_name VARCHAR(255),
    host_since DATE,
    host_location VARCHAR(255),
    host_about TEXT,
    host_thumbnail_url VARCHAR(255),
    host_picture_url VARCHAR(255),
    host_neighbourhood VARCHAR(255),
    FOREIGN KEY(hld_id) 
        REFERENCES hld(hld_id),
    FOREIGN KEY(hqad_id)
        REFERENCES hqad(hqad_id)
);

CREATE TABLE IF NOT EXISTS property (
    property_id BIGINT PRIMARY KEY,
    latitude NUMERIC(8, 6),
    longitude NUMERIC(8, 6),
    property_type VARCHAR(255),
    room_type VARCHAR(100),
    accommodates INT,
    bathrooms NUMERIC(5, 1),
    bathrooms_desc VARCHAR(50),
    beds INT,
    daily_price NUMERIC(8, 2)
);

CREATE TABLE IF NOT EXISTS reviews_diagnostics (
    rev_diag_id BIGINT PRIMARY KEY,
    number_of_reviews INT,
    number_of_reviews_ltm INT,
    number_of_reviews_l30d INT,
    first_review DATE,
    last_review DATE,
    review_scores_rating NUMERIC(3, 2),
    review_scores_accuracy NUMERIC(3, 2),
    review_scores_cleanliness NUMERIC(3, 2),
    review_scores_checkin NUMERIC(3, 2),
    review_scores_communication NUMERIC(3, 2),
    review_scores_location NUMERIC(3, 2),
    review_scores_value NUMERIC(3, 2),
    reviews_per_month NUMERIC(3, 2)
);

CREATE TABLE IF NOT EXISTS listings (
    listing_id BIGINT PRIMARY KEY,
    scrapings_id BIGINT,
    host_id BIGINT,
    neighbourhood_id BIGINT,
    property_id BIGINT,
    minmax_insights_id BIGINT,
    avail_info_id BIGINT,
    rev_diag_id BIGINT,
    listing_url VARCHAR(100),
    name VARCHAR(255),
    picture_url VARCHAR(255),
    license VARCHAR(10),
    instant_bookable INT,
    FOREIGN KEY(scrapings_id)
        REFERENCES scrapings(scrapings_id),
    FOREIGN KEY(host_id)
        REFERENCES host(host_id),
    FOREIGN KEY(neighbourhood_id)
        REFERENCES neighbourhoods(neighbourhood_id),
    FOREIGN KEY(property_id)
        REFERENCES property(property_id),
    FOREIGN KEY(minmax_insights_id)
        REFERENCES minmax_insights(minmax_insights_id),
    FOREIGN KEY(avail_info_id)
        REFERENCES availability_info(avail_id),
    FOREIGN KEY(rev_diag_id)
        REFERENCES reviews_diagnostics(rev_diag_id)
);
