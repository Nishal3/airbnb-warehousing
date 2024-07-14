# AWS Glue Script Testing

## Description

This is where the script to transform the listings data is housed. Copy and paste this script into the Glue Job when it is needed.

**_Note_: This script cannot be run locally because we would need to make our environment perfect to run it and install dependencies.**

### If You DO Want to Run This Locally, Here are the General Requirements

To make things work, I recommend using a virtual environment like a GitHub codespace or EC2 instance. Reason being to make this work, we need to install an older version of:

- Spark(3.3.0)
- Java(8)
- Python(3.10)

I would follow the [ aws-glue-libs ][ aws-glue-libs ] github repo to do this for Glue version 4.0. I know doing this is going to be tedious because I've tried, but you do you, and make sure to have fun!

<!-- Links -->

[aws-glue-libs]: https://github.com/awslabs/aws-glue-libs
