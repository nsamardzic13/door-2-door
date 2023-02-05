# Door2Door Code Challenge

**door2door collects the live position of all vehicles in its fleet in real-time via a GPS sensor in each**
vehicle. These vehicles run in operating periods that are managed by door2door's operators. An API is
responsible for collecting information from the vehicles and place it on an S3 bucket, in raw format, to
be consumed.

**The goal of this challenge is to automate the build of a simple yet scalable data lake and data warehouse**
that will enable our BI team to answer questions.

## **Technical assumptions**

* The fetching process should only get data from a certain day on each run and should run every day;
* **Files on the ”raw” S3 bucket can disappear but we might want to process them differently in the**
  future;
* **No need to answer the question stated in the introduction;**
* **If your solution is setup to run locally, it must be containerized;**
* **There is no need for paid, expensive and highly performant data warehouses. You can use a ”standard” SQL database.**

## Solution

The solution is using various technologies. The solution is cloud-based on AWS.

Technologies used are:

* **Deployed via Web Console**
  * AWS S3
  * * Landing and Data Lake Buckets
  * AWS RDS - Postgres
    * In the real world scenario, this would be a DWH (such as AWS Redshift)
  * AWS Secrets Manager
    * Storing RDS credentials
* **Deployed via AWS CDK**
  * AWS Lambda
    * Scheduled Python code extracting, transforming and loading data
    * Moving processed data to Data Lake bucket
  * AWS SNS, Cloudwatch
    * Creating alarm in case of lambda's failure with email alerting

### Lambda Steps

Lambda is split in multiple steps:

1. Extracting data from Landing S3
2. Load data to staging tables
3. Execute stored procedures on RDS
4. Move processed S3 objects to Data Lake Bucket
5. Truncate staging tables in case there were no failures

**Note:** Moving processed S3 objects is currently commented out. Objects are just being copied to the Data Lake Bucket, but they are not deleted from the source Landing Bucket to save some time in case some more testing needs to be done.

### Helper Scripts

Besided AWS Solution, additionals script were created. Some of them are used as a way to prepare environment and some to get introducted to dataset used for this challenge. Each "helper script" can be found in its own folder:

* 00-DataCheck
  * Simple .ipynb notebook showing basic data information such as null count and data type
* 01-PopulateS3
  * Script used to transfer data from local folder (data) to landing S3 bucket
* SetupRDSObject
  * Script executing .sql queries. Creating schemas, tables and stored procedures in RDS Postgres.

Except 00-DataCheck, other script are a "one-time deal". They are just being executed one time to create desired environment and database structure needed for this challenge.

## Deploying the Solution

To deploy the solution created using CDK, we can use simple command:

```bash
cdk deploy --profile <profile_name>
```
