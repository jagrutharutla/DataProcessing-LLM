
***

# GenAI Data Pipeline

Leverages OpenAI GPT-4, AWS Lambda, Step Functions, and AWS RDS to automate and streamline ETL workflows with enhanced real-time insights.

***

## Overview

The **GenAI Data Pipeline** is a cloud-native data processing solution integrating Large Language Model (LLM) APIs for automated data pre-processing. This setup efficiently processes data using Lambda-based ETL functions orchestrated by AWS Step Functions with storage and querying powered by AWS RDS. The pipeline also evaluates metadata in real-time for meaningful business insights.

***

## Key Features

- **Automated Data Pre-processing** using OpenAI GPT-4 API within AWS Lambda.
- **Streamlined ETL Workflow** orchestrated through AWS Step Functions.
- **Real-time Metadata Evaluation** ensuring high data quality and operational insights.
- **Scalable Storage** using AWS RDS for reliable and performant data persistence.

***

## AWS Services Utilized

| Service           | Purpose                                           |
|-------------------|-------------------------------------------------|
| AWS Lambda         | Serverless compute for data processing functions |
| AWS Step Functions | Orchestration of Lambda functions workflow       |
| Amazon RDS         | Relational database for storing processed data   |
| AWS Secrets Manager| Secure storage of API keys and DB credentials    |
| Amazon S3          | Intermediate storage for raw and cleansed data   |
| Amazon CloudWatch  | Monitoring and logging of Lambda and Step Functions |

***

## Architecture Diagram

```
[Raw Data] --> S3 --> Lambda (Profiling) --> Lambda (Quality Check) --> Lambda (Cleaning) --> Lambda (Aggregation) --> RDS
                            |                                        |                  |
                            +--> Step Functions orchestrates workflow
                            +--> OpenAI GPT-4 API invoked within Lambdas
```

***

## AWS Setup Instructions

### 1. Set Up AWS RDS

- Launch an Amazon RDS instance (e.g., PostgreSQL).
- Configure security groups to allow Lambda functions access.
- Create necessary databases and tables for processed data storage.

### 2. Configure AWS Secrets Manager

- Store OpenAI API key and RDS credentials securely.
- Grant Lambda functions permission to access these secrets.

### 3. Create Amazon S3 Buckets

- Create buckets for storing raw input data and intermediate results.
- Structure folders for profiling, quality check, cleaning, and aggregation outputs.

### 4. Deploy AWS Lambda Functions

- Prepare Lambda functions based on Python scripts:
  - `DataProfiling.py` — Profiles raw data.
  - `Quality_Check.py` — Performs data quality checks.
  - `Data_Cleaning.py` — Cleans and preprocesses data.
  - `Data_Aggregation.py` — Aggregates processed data.
  - `MetaData.py` — Manages metadata evaluations and enhancements.
- Package dependencies including OpenAI SDK.
- Set environment variables (e.g., secrets ARN, S3 bucket names).
- Assign appropriate IAM roles with access permissions to S3, RDS, Secrets Manager, and CloudWatch.

### 5. Set Up AWS Step Functions

- Create a Step Functions state machine to orchestrate Lambda invocation order:
  - Profiling -> Quality Check -> Cleaning -> Aggregation
- Include retry and error handling strategies.
- Ensure proper transition to next Lambda upon success.

### 6. Monitoring and Logging

- Configure CloudWatch logs for all Lambda functions.
- Set alarms for function failures or performance thresholds.

***

## Data Processing Workflow

1. **Data Profiling** (`DataProfiling.py` Lambda)
   - Analyzes raw data characteristics, distributions, and metadata.
   - Outputs report for data readiness assessment.

2. **Quality Check** (`Quality_Check.py` Lambda)
   - Validates data integrity and quality against expected standards.
   - Flags anomalies and missing values.

3. **Data Cleaning** (`Data_Cleaning.py` Lambda)
   - Applies transformations, imputations, and normalization.

4. **Data Aggregation** (`Data_Aggregation.py` Lambda)
   - Summarizes and aggregates data to final formats suitable for analysis.

5. **Metadata Handling** (`MetaData.py` Lambda)
   - Evaluates and adds metadata for better traceability and context.

Each step employs OpenAI GPT-4 API calls to enhance processing intelligence and decision-making heuristics.

***

## Getting Started

1. Clone this repository.
2. Follow the AWS setup instructions above.
3. Deploy Lambda functions in the specified order.
4. Start the Step Functions state machine to initiate data processing.
5. Monitor the pipeline through AWS Console and CloudWatch.

***

