
## Event based batch analytics framework
<!-- Choose a self-explaining name for your project. -->

### Description
Batch processing of input files using an end-to-end workflow done by Amazon EventBridge and AWS StepFunctions. Building an Amazon QuickSight dashboard for Sales and Trend Analysis.

<!-- ## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge. -->

### Architecture
Architecture of proposed solution:
<div align="center">
![Architecture](architecture/architecture__2_.png){width=60%}
</div>

### AWS Services & TechStack Used

Following are the AWS services taken under consideration to accomplish this project:

- **AWS Glue Studio:** Glue Studio to run/check the pyspark script.<br>
- **Amazon S3:** Holding target and source CSV data files, triggering AWS Lambda.<br>
- **Amazon DynamoDB:** Maintains the configuration/metadata of table like columns, it has and their data types.<br>
- **Amazon EventBridge:** triggers AWS Lambda.<br>
- **AWS Lambda:** Generates Datasets, validates dataset acc to defined rules, tracks files and updates dynamodb, Triggers required AWS service on demand.<br>
- **AWS Step Function:** For workflow orchestration of aggregation job.<br>
- **Amazon Quicksight:** Building an Amazon QuickSight dashboard for Sales and Trend Analysis.<br>
- **Amazon EMR clusters:** To run pyspark job on top of it(to aggregate files/datasets).<br>

**Technology Stack:** Python, PySpark

<!-- ## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection. -->

## Steps
1. An AWS Lambda(which is mimicking the real scenario of the files that stores will generate and sends to S3) generates files(dataset) with anomalies & stores in Amazon S3.<br>

2. Validate the files on Amazon S3 event via AWS Lambda , and moving those files to a different path in the same bucket.<br>

3. Tracking the validated files by managing the metadata in Amazon DynamoDB. If the stores has sent all the files to be processed, will start the execution of AWS Step Function to aggregate the files(aggregating files for sale amount, tax amount, & discount amount) using EMR Step and store the aggregated files in Amazon S3.<br>

4. Using Amazon QuickSight , we can generate the dashboard of the aggregated data in Amazon S3.<br>

