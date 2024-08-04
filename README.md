# Airbnb Warehousing: _**Warehousing Airbnb Data From Ohio**_

![ visual ]

## Description and Overview of Airbnb Listings Warehousing Project

We will start off with loading data into RDS to emulate a real-world scenario: data is already in an RDS instance and we need to perform an ETL to move it to Redshift. We have airbnb data about listings and want to store it in a data warehouse after running some sort of transformation. Finally, we make a dashboard using Tableau to finalize the project.

I recommend downloading this repo and copying things from the downloaded files. You could also copy and paste directly from GitHub, you're free to pick your poison.

**A High-Level Overview:**

1. Set-up RDS, EC2 connector to RDS, and Redshift Serverless
2. Use EC2 connector to load data into RDS (in the real world, data would already be in RDS, we are setting up for a real-world circumstance)
3. Set up an IAM role for Glue
4. Set up S3 Gateway for Glue connector
5. Set up Database, connector and crawler in Glue
6. Run crawler and create job script
7. Trigger job script
8. Set up a dashboard using Tableau using the data from Redshift

## Preconfiguration and Data Loading

This marks the start of the tutorial. We start with preconfiguring RDS and then loading data into it. Then we preconfigure Redshift.

## 1 Creating The RDS Instance

First we will load data into the RDS instance, but before even that we need to create the RDS instance and EC2 connector instance

### 1.1 Finding RDS and Setting up Free Tier

1. Log into AWS and search for RDS
2. Click "Databases" then click "Create database"
3. Click "Easy create" then choose MySQL
4. Scroll down a bit and click "Free Tier"

### 1.2 Database Name, Username, and Password Config

1. Set your database name under "DB instance identifier"
2. Make a master username or leave it as admin
3. Click "Self managed" for credentials management and select auto-generate OR enter a password for the database

**Make sure you save or remember your database credentials**

### 1.3 Setting up EC2 Connection and Creating RDS Instance

1. Now click "Set up EC2 connection" and choose "Connect to an EC2 compute resource"
2. If you have not made an EC2 instance for this separately, click "Create EC2 instance"
3. Name your EC2 instance
4. Keep the defaults
5. Click "Launch instance", you will be prompted to select, create, or proceed without a key pair
6. If you want a key pair continue reading these steps, or continue without one; no key pair will make the process less secure
7. Click "Create new key pair" and name it
8. Leave everything else default and click "Launch instance"

**Make sure you don't delete the downloaded key pair. We might need it later to get into the EC2 instance**

Now preconfiguration for RDS is complete! The details should look something like this:
![ RDS_config ]

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
   4. Run `sudo yum update` so that the installed file is recognized
   5. Installing MySQL: `sudo dnf install mysql-community-server -y`
   6. Starting MySQL: `sudo systemctl start mysqld`
2. Make sure you are at the home directory, run the command `cd` to make sure of that
3. To download the CSV file run `curl -O https://raw.githubusercontent.com/Nishal3/airbnb-warehousing/main/data/listings.csv`

### 2.3 Entering and Loading Data Into RDS

**All files used in this step is in the `RDS_data_loading` directory**

1. Go to the RDS console and click on the database you've created. In the connectivity and security section, there is an endpoint, copy and save the endpoint for later

2. Connect to the MySQL server by running this command inside of the EC2 instance: `mysql --user=admin --host=<YOUR_RDS_ENDPOINT> --password --local-infile` replacing "<YOUR_RDS_ENDPOINT>" with your endpoint. If you made the username something other than admin, make sure you change the `--user` option as well!

3. Enter your RDS password when prompted

4. Create the `columbus_oh_listings` database by running the SQL command `CREATE DATABASE columbus_oh_listings;` and run the `SHOW SCHEMAS;` command to see if this database is present

5. Run `USE columbus_oh_listings;` to use this database for upcoming commands

6. Create the `listings` table by copying and pasting the file in this repo named `RDS_listings_table_creation.sql` and running the script

7. Load data into the table by copying and pasting the `RDS_data_load_script.sql` script and running it

**Now we've loaded our "pre-existing" database!** It the outputs for both of these should look like this:  
![ EC2_table_creation_and_load_output ]

And if you want, you can run a simple select statement and describe the table as well. If you do it should look like this:  
![ rds_data_outputs ]

## 3 Redshift Serverless Configuration and Table Creation

### 3.1 Creating a Default Workgroup

1. Go to AWS Redshift, click the hamburger menu, and then "Redshift Serverless"
2. On clicking, it will take you to the workgroup creation page, click "Customize Settings"
3. Leave everything other than "Admin user name" and "Admin password" as default
4. Assign a admin username or leave it as the default
5. Click "Manually add the admin password" and type in a secure password or click "Generate a password". Be sure to save the password elsewhere!
6. Click "Save configuration", and that's our workgroup created

The workgroups page should now look like this:
![ redshift_workgroup_screen ]

### 3.2 Creating Database and Tables for Transformed Data

**Ideally we create a Redshfit user in IAM and restrict permissions to only those being used. For learning/dev purposes, we can skip this step**

1. Get into the query editor by clicking "Query editor v2" on the left menu
2. Create the database by clicking the "Create" which has a plus and click "Database"
3. Paste this in as the name: "transformed_columbus_oh_listings_data", or name it yourself, be sure to remember the DB name and apply it where the given name is used!
4. For "Users and groups", have the database user be "admin", then click "Create database"
5. In the query editor, click the second dropdown, it should have "dev" on it, and select the database we just created
6. Create the tables by pasting in the SQL Script from the `table_creation.sql` file, and click "Run"

Now if you open up the folder "transformed_columbus_oh_listings_data/public/Tables/" you should see the newly created tables. Here's what it should look like:
![ redshift_table_creation ]

### 3.3 Creating a Redshift Temporary Directory (S3 Bucket)

To load data into Redshift, we need to initialize a S3 bucket so Redshift can use it as a temporary directory.

**YOU NEED TO CHANGE THE `REDSHIFT_TEMP_DIR` VARIABLE TO REFLECT YOUR S3 BUCKET IN THE `listing_glue_transform.py` FILE**

1. Go to the S3 console
2. Click "Create bucket" and give the bucket a unique name that contains "redshift" in it. Usually your name makes it unique, example: "nishal-airbnb-transformed-temp-redshift-data"
3. Leave everything default and hit "Create bucket"
4. Inside the bucket, create a folder names "temp" by entering the bucket and clicking "Create folder"
5. Change the `REDSHIFT_TEMP_DIR` variable in the `listings_glue_transform.py` file in the `listings_transformations/glue_script/` directory

This is what it should look like inside the bucket:
![ s3_redshift_temp_folder ]

Great! Now we've set up Redshift for the incoming data!

## ETL Using AWS Glue and Querying Loaded Data in Redshift's Query Editor

This marks the start of set-up and usage of AWS Glue to extract, transform, and load the data stored in the RDS instance. We will also query our transformed data in Redshift using its query editor.

## 4 Creating a Connector to the RDS Instance and Redshift Serverless

### 4.1 Creating an IAM Role for Glue to Access RDS and Redshift

**The roles chosen here are not meant for production. Create your own more granular IAM roles for those. Here, we're just using default policies to make the role.**

1. Search IAM in the AWS console, and click "Roles" in the left menu
2. Click "Create role" and select "AWS service"
3. In the "Use case" section, search for Glue
4. Click "Next" and search for "AmazonRDSReadOnlyAccess" and check it
5. Now search for "AmazonRedshiftAllCommandsFullAccess" and check that
6. Then search and check "AWSGlueServiceRole" and click "Next"
7. Assign a meaningful name and description to the role and then click "Create role"

<!-- The connector details should resemble this: -->

### 4.2 Creating an S3 Gateway and Connecting a Subnet

In order for a connection to work, the connector has to be able to access an S3 Gateway. We will be doing that in this step.

1. Navigate to the Virtual Private Cloud (VPC) console
2. Click "Endpoints" on the left menu and then hit "Create endpoint"
3. Name the endpoint a suitable name
4. In "Services", search S3 and hit enter
5. We will use the one labeled "Gateway"
6. Select the default VPC or the VPC you created for this project
7. Select the default route table, it should not have the "RDS-pvt-rt" name
8. Allow full access (bad practice for production, use granular permissions in that case) and then click "Create endpoint"

### 4.3 Creating the Glue Connections

1. Go to the AWS Glue home page
2. Click "Data connections" on the left menu

#### 4.3.1 Creating RDS Connection

1. In the "Connections" section click "Create connection" and search for MySQL
2. For "Database instances", the RDS instance you created earlier should show up, click that
3. For "Database name" write "columbus_oh_listings"
4. Type in your credentials for the database
5. Click "Network options" and click the VPC associated with the RDS instance, if theres only one click that one
6. You can choose any subnet as long as it is part of the default route table, I recommend remembering one subnets first 3-ish digits so you don't have to jump back and forth to VPC and Glue
7. For security groups, choose the one associating the EC2 instance with the RDS instance it should be something like "ec2-rds-#" where # is a number
8. Click "Next" and assign a meaningful name and description and then hit "Next" again
9. Click "Create connection", and we're done!

#### 4.3.2 Creating Redshift Connection

Same steps as RDS connection setup, but selecting Amazon Redshift for the data source

**_Note_: If you assigned the Redshift workgroup a VPC other than the default, you will need to assign the same VPC for the Glue Connection**

1. Click "Create connection" and select Amazon Redshift. Click "Next"
2. The default workgroup we made for Redshift Serverless should be there under "Database instances"
3. Enter the database name: "transformed_columbus_oh_listings_data" or the name you assigned it
4. Enter the username and password you set for the default workgroup
5. Click "Next" and assign it a meaningful name and description that you can recognize later
6. Click "Next" again and hit "Create connection"
7. After creating the connection, edit the connection to have the correct subnet by selecting it and clicking "Actions" -> "Edit"
8. Scroll down, enter your password and under "Network options" -> "Subnet" select the correct subnet

#### 4.4 Testing the Connections

1. Select the connection you just made and click "Actions" -> "Test connection"
2. For the IAM role, put the role we just created and then click "Confirm"
3. Wait for a successful response
4. Rinse and repeat for the other connection you made

**If you don't get a successful response, make sure your IAM role is correct and the VPC selected is the same as the VPC containing your database or redshift workgroup. Make sure the subnet you are using has access to an S3 gateway as well!**

## 5 Using a Glue Crawler to Extract Metadata From Our Data Stores and Previewing Data Using Amazon Athena

### 5.1 Creating the Databases in Glue Catalog to Connect Glue to the Databases From RDS and Redshift

1. In the Glue consoles left menu under "Data Catalog", click "Databases"
2. Click "Add database"
3. Give it a fitting name like "airbnb_untransformed_data" and a description
4. Click create
5. Again, click "Databases" and create another for the transformed data. Give it a fitting name like "airbnb_transformed_data", you know the process from here!

Now we've set up databases in Glue Catalog!

### 5.2 Creating a Crawler to Get the Blueprint of the Data for Both RDS and Redshfit

**Expect to get an ~$0.20 bill after running these crawlers, Glue Crawlers are not an offered service for free tier**

Crawlers don't actually copy the data into Glue. They collect metadata about the data and store in the Glue Catalog. The data stores we "extract data" [not actual data, just metadata] from are stored in a Glue database. Refer to the [ official docs ][ official_glue_docs ] for more info.

#### 5.2.1 RDS Crawler

1. Go to the Glue console and click "Crawlers" on the left menu
2. Hit "Create crawler" and give it a fitting name and description
3. Hit "Next" and click "Add a data source"
4. Make the data source a JDBC connection
5. For the connection, choose the one we made for RDS
6. The path would be "columbus_oh_listings/listings"
7. Click "Next" and choose the IAM role we created before. Proceed by clicking "Next"
8. For the target database, choose the database we made in Glue for the untransformed data and click "Next"
9. Click "Create crawler"
   Here's how it might look:

#### 5.2.2 Redshift Crawler

1. Rinse and repeat for Redshift assigning meaningful names and using the connection and database in Glue associating with Redshift. The only thing different is the data source's path, which is now "transformed_columbus_oh_listings_data/%"

Here's how it might look:

Select the crawlers and hit "Run". And now, we've fetched the column metadata for both data source and target!

## 5 Running a Glue Job to Transform RDS Listings Data and Move it to Redshift

### 5.1 Configuring Job Script

**After running the job, expect a ~$0.20 bill. Running Glue jobs are also not an offered service for AWS free tier**

Download the transformation file before continuing. Go to [ this link ][transformation_file] and click the download button if you have not already downloaded this repo.

1. Navigate to the Glue console
2. Click "ETL jobs" on the left menu
3. Click "Script editor" and click "Upload script"
4. If you downloaded the repo, select the python file in the `listings_transformations/glue_script/` folder if you have not separately downloaded it, if you separately downloaded it, select that.
5. In the "Job details" section give a fitting name and description, then choose the Glue IAM role we made for this project
6. Scroll down and change the "Requested number of workers" to 2. You can leave it 10, but you might get a heftier bill
7. Edit the "Job timeout" option to be 10 minutes as well. We don't want to be racking up a huge AWS bill!
8. Drop down "Advanced properties" and scroll down until you see "Connections"; here we will attatch the two connections we made, the RDS connector and the Redshift connector
9. Click "Choose options" and select the two connections we've made
10. Hit save up at the top right and we're done!

### 5.2

## Data and Creative Commons Liscense for Data

Data used: [Columbus, Ohio, United States 26 December, 2023 from Airbnb][data_link]

Creative commons liscense for data: [Liscense][creative_liscense]

<!-- Images  -->

[ visual ]: https://dqkl9myp5qci5.cloudfront.net/airbnb_listings_etl_visual.png "RDS, Redshift, and Glue Pipeline Visual"
[ RDS_config ]: https://dqkl9myp5qci5.cloudfront.net/RDS_config.png "RDS Details Page"
[ rds_data_outputs ]: https://dqkl9myp5qci5.cloudfront.net/RDS_data_select_describe_output.png "RDS Output After Running Table Creation Script"
[ EC2_table_creation_and_load_output ]: https://dqkl9myp5qci5.cloudfront.net/EC2_table_creation_data_load_outputs.png "Loading Data Into RDS Using EC2"
[ redshift_workgroup_screen ]: https://dqkl9myp5qci5.cloudfront.net/Redshift_workgroup_config.png "Workgroup Screen After Creating Workgroup"
[ redshift_table_creation ]: https://dqkl9myp5qci5.cloudfront.net/Redshift_table_creation.png "Redshift Table Creation Output"
[ s3_redshift_temp_folder ]: https://dqkl9myp5qci5.cloudfront.net/S3_Redshift_temp_bucket.png "Redshift S3 Temporary Directory Bucket"

<!-- Airbnb Data -->

[ data_link ]: http://insideairbnb.com/get-the-data/
[ creative_liscense ]: http://creativecommons.org/licenses/by/4.0/

<!-- Other Links -->

[ official_glue_docs ]: https://docs.aws.amazon.com/glue/latest/dg/console-tables.html
[ transformation_file ]: https://github.com/Nishal3/airbnb-warehousing/blob/main/listings_transformations/glue_script/listings_glue_transform.py
