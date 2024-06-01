# Airbnb Warehousing: _**Warehousing Airbnb Data From Ohio**_

## Data Loading

We are starting off with loading data into RDS to emulate a real-world scenario: data is already in an RDS instance and we need to perform an ETL to move it to Redshift. We have airbnb data about the listings and want to store it into a data warehouse.

### Creating The RDS Instance

1. Log into AWS and search for RDS
2. Click "Databases" then click "Create database"
3. Click "Easy create" then choose MySQL
4. Scroll down a bit and click "Free Tier"
5. Set your database name under "DB instance identifier"
6. Make a master username or leave it as admin
7. Click "Self managed" for credentials management and enter a password for your database
8. Click "Set up EC2 connection" and choose "Connect to an EC2 compute resource"
9. If you have not made an EC2 instance for this separately, click "Create EC2 instance"
   1. Name your EC2 instance
   2. Keep the defaults
   3. Click "Launch instance", you will be prompted to select, create, or proceed without a key pair
   4. If you want a key pair continue reading these steps, or continue without one. No key pair will make the process less secure
   5. Click "Create new key pair" and name it
   6. Leave everything else default and click "Launch instance", **make sure you don't delete the downloaded key pair. we'll need it later to get into the EC2 instance**

Now everything for RDS is complete!

### Loading Our Data Into RDS

1. Go to the EC2 Console

## ETL Process Using AWS Glue

### Creating a Connector to the RDS Instance

1. Go to the AWS Glue home page
2.

## Data and Creative Commons Liscense

Data used: [Columbus, Ohio, United States 26 December, 2023 from Airbnb][data_link]

Creative commons liscense for data: [Liscense][creative_liscense]

<!-- Images  -->

<!-- Uber Data -->

[ data_link ]: http://insideairbnb.com/get-the-data/
[ creative_liscense ]: http://creativecommons.org/licenses/by/4.0/
