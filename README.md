# Airbnb Warehousing: _**Warehousing Airbnb Data From Ohio**_

## Data Loading

We are starting off with loading data into RDS to emulate a real-world scenario: data is already in an RDS instance and we need to perform an ETL to move it to Redshift. We have airbnb data about the listings and want to store it into a data warehouse after running some sort of transformation.

## 1 Creating The RDS Instance

### 1.1 Finding RDS and Setting up Free Tier

1. Log into AWS and search for RDS
2. Click "Databases" then click "Create database"
3. Click "Easy create" then choose MySQL
4. Scroll down a bit and click "Free Tier"

### 1.2 Database Name, Username, and Password Config

1. Set your database name under "DB instance identifier"
2. Make a master username or leave it as admin
3. Click "Self managed" for credentials management and enter a password for your database

### 1.3 Setting up EC2 Connection

1. Now click "Set up EC2 connection" and choose "Connect to an EC2 compute resource"
2. If you have not made an EC2 instance for this separately, click "Create EC2 instance"
3. Name your EC2 instance
4. Keep the defaults
5. Click "Launch instance", you will be prompted to select, create, or proceed without a key pair
6. If you want a key pair continue reading these steps, or continue without one; no key pair will make the process less secure
7. Click "Create new key pair" and name it
8. Leave everything else default and click "Launch instance"

**Make sure you don't delete the downloaded key pair. We might need it later to get into the EC2 instance**

Now preconfiguration for RDS is complete!

## 2 Loading Our Data Into RDS

### 2.1 Connecting to the EC2 Instance

1. Go to the EC2 Console
2. Click "Instances (running)"
3. Select your running EC2 instance that connects to the RDS instance
4. Click "Actions" and click "Connect" at the dropdown
5. Leave everything default and click "Connect"

**If this does not work, follow the steps in the SSH client tab in the "Connect to instance" page**

### 2.2 Downloading MySQL on EC2 and Getting CSV File

1. Running commands inside EC2 machine to download MySQL CE
   1. Get the download file: `sudo wget https://dev.mysql.com/get/mysql80-community-release-el9-1.noarch.rpm`
   2. Make sure the file is there by running `ls`
   3. Installing the file: `sudo dnf install mysql80-community-release-el9-1.noarch.rpm -y`
   4. Installing MySQL: `sudo dnf install mysql-community-server -y`
   5. Starting MySQL: `sudo systemctl start mysqld`
2. Make sure you are at the home directory, just run the command `cd` to make sure of that
3. To download the CSV file run `curl -O https://raw.githubusercontent.com/Nishal3/airbnb-warehousing/main/data/listings.csv`

### 2.3 Entering and Loading Data Into RDS

**All files used in this step is in the `RDS_data_loading` directory**

1. Go to the RDS console and click on the database you've created. In the connectivity and security section, there is an endpoint, copy and save the endpoint for later

2. Connect to the SQL by running this command inside of the EC2 instance: `mysql --user=admin --host=<YOUR_RDS_ENDPOINT> --password --local-infile` replacing "<YOUR_RDS_ENDPOINT>" with your endpoint. If you made the username something other than admin, make sure you change the `--user` option as well!

3. Enter your RDS password when prompted

4. Create the `columbus_oh_listings` database by running the SQL command `CREATE DATABASE columbus_oh_listings;` and run the `SHOW SCHEMAS;` command to see if this database is present

5. Run `USE columbus_oh_listings;` to use this database for upcoming commands

6. Create the `listings` table by copying and pasting the file in this repo named `RDS_listings_table_creation.sql` and running the script

7. Load data into the table by copying and pasting the `RDS_data_load_script.sql` script and running it

**Now we've loaded our "pre-existing" database!**

## Extract and Transform Part of ETL Using AWS Glue

This marks the start of set-up and usage of AWS Glue to extract and transform the data stored in the RDS instance.

## 3 Creating a Connector to the RDS Instance

### 3.1 Creating an IAM Role for Glue to Access RDS

1. Search IAM in the AWS console, and click "Roles" in the left menu
2. Click "Create role" and select "AWS service"
3. In the "Use case" section, search for Glue
4. Click "Next" and search for "AmazonRDSReadOnlyAccess" and check it
5. Then search and check "AWSGlueServiceRole" and click "Next"
6. Assign a meaningful name and description to the role and then click "Create role"

### 3.2 Creating the Glue Connection and Testing the Connection

1. Go to the AWS Glue home page
2. Click "Data connections" on the left menu
3. In the "Connections" section click "Create connection" and search for MySQL
4. For "Database instances", the RDS instance you created earlier should show up, click that
5. For "Database name" write "columbus_oh_listings"
6. Type in your credentials for the database
7. Click "Network options" and click the VPC associated with the RDS instance, if theres only one click that one
8. You can choose any subnet for subnets and for security groups, choose the one associating the EC2 instance with the RDS instance it should be something like "ec2-rds"
9. Click "Next" and assign a meaningful name and description and then hit "Next" again
10. Click "Create connection" and we're done!

#### 3.2.1 Testing the Connection

1. Choose

## Data and Creative Commons Liscense

Data used: [Columbus, Ohio, United States 26 December, 2023 from Airbnb][data_link]

Creative commons liscense for data: [Liscense][creative_liscense]

<!-- Images  -->

<!-- Airbnb Data -->

[ data_link ]: http://insideairbnb.com/get-the-data/
[ creative_liscense ]: http://creativecommons.org/licenses/by/4.0/
