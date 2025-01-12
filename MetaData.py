import boto3
import pandas as pd
import requests
import json
import zipfile
from io import BytesIO
import re
import psycopg2

#SNS Initialization
sns = boto3.client('sns')
# Initialize S3 client
s3 = boto3.client('s3')
# API Endpoint and Headers
url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": "eyJraWQiOiJpb2dUeXR5amt4WFVtcWM5TFVEVmN4NEx3VkY4WEtnN0RzRlwvUU1yMngzWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTciLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV82cU4zY0tGZWIiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3djE1ZW01YTBpcXZiM2huNXI2OWNnMzQ4NSIsIm9yaWdpbl9qdGkiOiI1NzMwODc4Mi1iODNhLTQ2MmQtODgwMi0wNTYyMWRjNmE1ZjUiLCJldmVudF9pZCI6ImJjYjA2NDE0LWFmZGQtNDVkNi1iMmIxLWU1OTM2ZDk0ZWI2OSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoib3BlbmlkIiwiYXV0aF90aW1lIjoxNzMxOTA3ODU4LCJleHAiOjE3MzE5MTE0NTgsImlhdCI6MTczMTkwNzg1OCwianRpIjoiZTA2YzZkN2EtNjJkMC00YTU3LWJiNWMtYmNlMTNmNDhiYmQxIiwidXNlcm5hbWUiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTcifQ.naTFRmb6LjMl0l7kXfwW6GsBgqoOflRfeaZroziZfj84SdZx7Mc5nFxrR5fteaDIrjlkrBVcTdcilY5lhzoIZxmgIOqqFknDG1s5qj5ltvCKHubqdiaiuXse2h7x8TwHOacO8TQ-pVnn1olhCk4XadAr3CN_NGHfFpjrcbWmbIUoF9-CbZ_MutQHrFtJylqiI2G52NO0OUPv27OJQYCSG-r1TUXQrt9F9Cgtdaz9vnN5UAFfhQh82QgI7nhyDdg0cc8l28ghcCfZRIHkXjNJEMcS9zCK9mehq92JG1TQuV24-qbzLPS_xp3MlYZWXx760cIz_wtW7iAfnQFlrf0p8g",
    "x-api-key": "Bearer sk-5tWPlrwVTszYu_wk_HjRNA" # Replace with your API Key
}

def lambda_handler(event, context):
    try:
        # Define the bucket name and zip key directly
        bucket_name = "asu-hackathon-data-727646476284"
        zip_key = "asu_hackathon_files.zip"

        # Step 1: Download and process metadata
        metadata = download_process_metadata(bucket_name, zip_key)

        # Step 2: Connect to RDS
        conn = get_db_connection()

        # Step 3: Store metadata in RDS
        store_metadata_in_rds(conn, metadata)

        # Step 4: Close the connection
        conn.close()

        print("Metadata processing completed.")

        # Publish notification to SNS
        response = sns.publish(
            TopicArn="arn:aws:sns:us-east-1:727646476284:metadata_sns",  # Replace with your Topic ARN
            Message=json.dumps({
                "status": "Metadata processed successfully",
                "details": "Add any relevant details here if needed"
            }),
            Subject="Metadata Processing Completed"
        )
        print(f"SNS publish response: {response}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Metadata processed and notification sent successfully"})
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def get_db_connection(): 
    print('starting connection') 
    conn = psycopg2.connect( 
        host="metadatadb.cv4sgquoeli5.us-east-1.rds.amazonaws.com", # Host is the Endpoint URL provided if you click into the details of your RDS 
        database="metaDataDB", # This is the name of your database 
        user="dbuser", # This is the username that you have created when you made the RDS 
        password="JAMSquad123", # This is the password that you created when you made the RDS 
        port="5432" # This will always be 5432 while using postgres 
    ) 
    print('connection made') 
    return conn 

def store_metadata_in_rds(conn, metadata):
    """
    Create tables and store metadata values for each file in the RDS database.

    Args:
        conn (psycopg2.Connection): A connection to the PostgreSQL database.
        metadata (dict): A dictionary containing metadata suggestions for each file.

    Returns:
        None
    """
    try:
        for file_name, details in metadata.items():
            # Generate a sanitized table name
            table_name = file_name.replace("/", "_").replace(".", "_")

            # Extract metadata suggestions
            suggestions = details.get("metadata_suggestions", "").split("\n")
            
            # Parse metadata into key-value pairs
            metadata_pairs = []
            for suggestion in suggestions:
                if ":" in suggestion:
                    key, value = map(str.strip, suggestion.split(":", 1))
                    metadata_pairs.append((key, value))

            if not metadata_pairs:
                print(f"No valid metadata suggestions for {file_name}. Skipping table creation.")
                continue

            # Create the table dynamically
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                metadata_key TEXT NOT NULL,
                metadata_value TEXT
            );
            """
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
                print(f"Table '{table_name}' created successfully.")

            # Insert metadata into the table
            insert_query = f"INSERT INTO {table_name} (metadata_key, metadata_value) VALUES (%s, %s);"
            with conn.cursor() as cur:
                cur.executemany(insert_query, metadata_pairs)
                conn.commit()
                print(f"Metadata for '{file_name}' inserted successfully.")

    except psycopg2.Error as e:
        print(f"Error storing metadata in RDS: {e}")
        conn.rollback()
        raise e



def extract_metadata_from_datasets(datasets):
    """
    Extract metadata from datasets using the GPT-4 API. The metadata fields
    are decided based on dataset analysis by the model.

    Args:
        datasets (dict): A dictionary with file names as keys and Pandas DataFrames as values.

    Returns:
        dict: A dictionary with file names as keys and metadata as values.
    """
    metadata_dict = {}

    for file_name, df in datasets.items():
        # Create a sample from the dataset for analysis
        sample_data = df.head(5).to_json()  # Convert the first 5 rows to JSON for GPT analysis
        column_names = list(df.columns)
        prompt = (
            f"I have a dataset with the following sample data:\n{sample_data}\n"
            f"And the following column names:\n{column_names}\n"
            "Please analyze this dataset and suggest the most relevant metadata fields to extract such as Names and identifiers, Dates, Description, Data type, File type, Data origin, Instrument(s) used, Data acquisition details, Data processing methods, Legal and ethical agreements, Citations. "
            "Give the metadata fields in following format - file_name:List of metadata suggestions along with value. "
            "Avoid Descriptions for each suggestion. "
        )
        response = call_gpt4_api(prompt)

        if response:
            metadata_dict[file_name] = {
                "metadata_suggestions": response
            }
        else:
            metadata_dict[file_name] = {
                "metadata_suggestions": "Failed to retrieve metadata suggestions from GPT-4."
            }

    return metadata_dict


def download_process_metadata(bucket_name, zip_key):
    """
    Download a ZIP file from S3, extract datasets, and generate metadata using GPT-4.

    Args:
        bucket_name (str): Name of the S3 bucket.
        zip_key (str): Key of the ZIP file in the S3 bucket.

    Returns:
        dict: Metadata for each file in the ZIP archive.
    """
    print("Downloading and processing ZIP file...")
    datasets = download_and_extract_zip(bucket_name, zip_key)
    print("Extracting metadata from datasets...")
    metadata = extract_metadata_from_datasets(datasets)
    return metadata


def download_and_extract_zip(bucket_name, zip_key):
    """
    Download a ZIP file from S3 and return all file names and file content as Pandas DataFrames.
    """
    response = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = BytesIO(response['Body'].read())
    file_data = {}
    with zipfile.ZipFile(zip_data, 'r') as z:
        print("Files in the ZIP archive:", z.namelist())
        for file_name in z.namelist():
            if not file_name.endswith('/'):  # Exclude directories
                with z.open(file_name) as file:
                    try:
                        df = pd.read_csv(file)
                        file_data[file_name] = df
                    except Exception as e:
                        print(f"Error reading {file_name}: {e}")
    return file_data

def call_gpt4_api(prompt):
    """
    Call the GPT-4 API with the specified prompt and return the response.
    """
    payload = {
        "model": "Azure OpenAI GPT-4o (External)",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "stream": False
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            print(f"Error: API request failed with status code {response.status_code}")
            print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


    
    
   
