CREATE TABLE IF NOT EXISTS `scrapings` (
    `scrapings_id` int PRIMARY KEY,
    `scrape_id` int,
    `last_scraped` date,
    `source` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `neighbourhoods` (
    `neighbourhood_id` int PRIMARY KEY,
    `neighbourhood` varchar(100),
    `neighbourhood_overview` varchar(1000),
    `neighbourhood_cleansed` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `minmax_insights` (
    `minmax_insights_id` int PRIMARY KEY,
    `maximum_nights` int,
    `minimum_nights` int,
    `minimum_minimum_nights` int,
    `maximum_minimum_nights` int,
    `minimum_maximum_nights` int,
    `maximum_maximum_nights` int,
    `minimum_nights_avg_ntm` int,
    `maximum_nights_avg_ntm` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `availability_info` (
    `avail_id` int PRIMARY KEY,
    `has_availability` tinyint,
    `availability_30` int,
    `availability_60` int,
    `availability_90` int,
    `availability_365` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `hld` (
    `hld_id` int PRIMARY KEY,
    `calculated_host_listings_count` int,
    `calculated_host_listings_count_entire_homes` int,
    `calculated_host_listings_count_private_rooms` int,
    `calculated_host_listings_count_shared_rooms` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `hqad` (
    `hqad_id` int PRIMARY KEY,
    `host_response_time` varchar(100),
    `host_response_rate` varchar(100),
    `host_acceptance_rate` varchar(100),
    `host_is_superhost` tinyint,
    `host_listings_count` int,
    `host_total_listings_count` int,
    `host_verifications` varchar(100),
    `host_has_profile_pic` tinyint,
    `host_identity_verified` tinyint
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `host` (
    `host_id` int PRIMARY KEY,
    `hld_id` int,
    `hqad_id` int,
    `host_url` varchar(100),
    `host_name` varchar(100),
    `host_since` date,
    `host_location` varchar(100),
    `host_about` varchar(1000),
    `host_thumbnail_url` varchar(100),
    `host_picture_url` varchar(100),
    `host_neighbourhood` varchar(100),
    FOREIGN KEY(hld_id) 
        REFERENCES hld(hld_id),
    FOREIGN KEY(hqad_id)
        REFERENCES hqad(hqad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `property` (
    `property_id` int PRIMARY KEY,
    `latitude` varchar(100),
    `longitude` varchar(100),
    `property_type` varchar(100),
    `room_type` varchar(100),
    `accommodates` int,
    `bathrooms_text` int,
    `beds` int,
    `amenities` varchar(1000),
    `price` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `reviews_diagnostics` (
    `rev_diag_id` int PRIMARY KEY,
    `number_of_reviews` int,
    `number_of_reviews_ltm` int,
    `number_of_reviews_l30d` int,
    `first_review` date,
    `last_review` date,
    `review_scores_rating` int,
    `review_scores_accuracy` int,
    `review_scores_cleanliness` int,
    `review_scores_checkin` int,
    `review_scores_communication` int,
    `review_scores_location` int,
    `review_scores_value` int,
    `reviews_per_month` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `listings` (
    `listing_id` int PRIMARY KEY,
    `scrapings_id` int,
    `host_id` int,
    `neighbourhood_id` int,
    `property_id` int,
    `minmax_insights_id` int,
    `avail_info_id` int,
    `rev_diag_id` int,
    `listing_url` varchar(100),
    `name` varchar(100),
    `picture_url` varchar(100),
    `license` varchar(100),
    `instant_bookable` tinyint,
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
