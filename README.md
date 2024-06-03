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
   4. If you want a key pair continue reading these steps, or continue without one; no key pair will make the process less secure
   5. Click "Create new key pair" and name it
   6. Leave everything else default and click "Launch instance".

**Make sure you don't delete the downloaded key pair. We might need it later to get into the EC2 instance**

Now everything for RDS is complete!

### Loading Our Data Into RDS

#### Step 1 -- Connecting to EC2 Instance

1. Go to the EC2 Console
2. Select your running EC2 instance that connects to the RDS instance
3. Click "Actions" and click "Connect" at the dropdown
4. Leave everything default and click "Connect"

If this does not work, follow the steps in the SSH client tab in the "Connect to instance" page

#### Step 2 -- Downloading MySQL on EC2 and Getting CSV File

1. Running commands inside EC2 machine to download MySQL CE
   1. Get the download file: `sudo wget https://dev.mysql.com/get/mysql80-community-release-el9-1.noarch.rpm`
   2. Make sure the file is there by running `ls`
   3. Installing the file: `sudo dnf install mysql80-community-release-el9-1.noarch.rpm -y`
   4. Installing MySQL: `sudo dnf install mysql-community-server -y`
   5. Starting MySQL: `sudo systemctl start mysqld`
2. To download the CSV file run `curl -O https://raw.githubusercontent.com/Nishal3/airbnb-warehousing/main/data/listings.csv`

#### Step 3 -- Entering and Loading Data Into RDS

2. Go to the RDS console and click on the database you've created. In the connectivity and security section, there is an endpoint, copy the endpoint

3. Connect to the SQL by running this command: `mysql --user=admin --host=<YOUR_RDS_ENDPOINT> --password` replacing "<YOUR_RDS_ENDPOINT>" with your endpoint. If you made the username something other than admin, make sure you change the `--user` option as well!

4. Create the `columbus_oh_listings` database by running the SQL command `CREATE DATABASE columbus_oh_listings;` and run the `SHOW SCHEMAS;` command to see if this database is present.

5. Run `USE columbus_oh_listings;`

6. Create the `listings` table by copying and pasting the file in this repo named `RDS_listings_table_creation.sql` and running the script

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
