CREATE TABLE `scrapings` (
    `scrapings_id` int(11),
    `scrape_id` int(11),
    `last_scraped` date,
    `source` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `neighbourhoods` (
    `neighbourhood_id` int(11),
    `neighbourhood` varchar(100),
    `neighbourhood_overview` varchar(1000),
    `neighbourhood_cleansed` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `minmax_insights` (
    `minmax_insights_id` int(11),
    `maximum_nights` int(11),
    `minimum_nights` int(11),
    `minimum_minimum_nights` int(11),
    `maximum_minimum_nights` int(11),
    `minimum_maximum_nights` int(11),
    `maximum_maximum_nights` int(11),
    `minimum_nights_avg_ntm` int(11),
    `maximum_nights_avg_ntm` int(11),
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `availability_info` (
    `avail_id` int(11),
    `has_availability` tinyint(1),
    `availability_30` int(11),
    `availability_60` int(11),
    `availability_90` int(11),
    `availability_365` int(11)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `hld` (
    `hld_id` int(11),
    `calculated_host_listings_count` int(11),
    `calculated_host_listings_count_entire_homes` int(11),
    `calculated_host_listings_count_private_rooms` int(11),
    `calculated_host_listings_count_shared_rooms` int(11)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `hqad` (
    `hqad_id` int(11),
    `response_time` varchar(100),
    `response_rate` varchar(100),
    `acceptance_rate` varchar(100),
    `host_is_superhost` tinyint(1),
    `listings_count` int(11),
    `total_listings_count` int(11),
    `host_verifications` varchar(100),
    `host_has_profile_pic` tinyint(1),
    `host_identity_verified` tinyint(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `host` (
    `host_id` int(11),
    `hld_id` int(11) FOREIGN KEY(hld_id) REFERENCES listing_diagnostics(listing_diagnostics_id),
    `hqad_id` int(11) FOREIGN KEY(hqad_id) REFERENCES hqad(hqad_id),
    `host_url` varchar(100),
    `host_name` varchar(100),
    `host_since` date,
    `host_location` varchar(100),
    `host_about` varchar(1000),
    `host_thumbnail_url` varchar(100),
    `host_picture_url` varchar(100),
    `host_neighbourhood` varchar(100),
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `property` (
    `property_id` int(11),
    `latitude` varchar(100),
    `longitude` varchar(100),
    `property_type` varchar(100),
    `room_type` varchar(100),
    `accommodates` int(11),
    `bathrooms_text` int(11),
    `bedrooms` int(11),
    `beds` int(11),
    `amenities` varchar(1000),
    `price` varchar(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `reviews_diagnostics` (
    `reviews_diagnostics_id` int(11),
    `number_of_reviews` int(11),
    `number_of_reviews_ltm` int(11),
    `first_review` date,
    `last_review` date,
    `review_scores_rating` int(11),
    `review_scores_accuracy` int(11),
    `review_scores_cleanliness` int(11),
    `review_scores_checkin` int(11),
    `review_scores_communication` int(11),
    `review_scores_location` int(11),
    `review_scores_value` int(11),
    `reviews_per_month` int(11)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;

CREATE TABLE `listing_diagnostics` (
    `listing_diagnostics_id` int(11),
    `scrapings_id` int(11) FOREIGN KEY(scrapings_id) REFERENCES scrapings(scrapings_id),
    `neighbourhood_id` int(11) FOREIGN KEY(neighbourhood_id) REFERENCES neighbourhoods(neighbourhood_id),
    `minmax_insights_id` int(11) FOREIGN KEY(minmax_insights_id) REFERENCES minmax_insights(minmax_insights_id),
    `avail_id` int(11) FOREIGN KEY(avail_id) REFERENCES availability_info(avail_id),
    `hld_id` int(11) FOREIGN KEY(hld_id) REFERENCES hld(hld_id),
    `hqad_id` int(11) FOREIGN KEY(hqad_id) REFERENCES hqad(hqad_id),
    `reviews_diagnostics_id` int(11) FOREIGN KEY(reviews_diagnostics_id) REFERENCES reviews_diagnostics(reviews_diagnostics_id),
    `property_id` int(11) FOREIGN KEY(property_id) REFERENCES property(property_id),
    `listing_url` varchar(100),
    `name` varchar(100),
    `picture_url` varchar(100),
    `license` varchar(100),
    `instant_bookable` tinyint(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf-8;
