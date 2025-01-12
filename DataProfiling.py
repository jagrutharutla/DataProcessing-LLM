import boto3
import pandas as pd
import requests
import json
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Initialize S3 client
s3 = boto3.client("s3")

# API Endpoint and Headers
url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": "eyJraWQiOiJpb2dUeXR5amt4WFVtcWM5TFVEVmN4NEx3VkY4WEtnN0RzRlwvUU1yMngzWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTciLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV82cU4zY0tGZWIiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3djE1ZW01YTBpcXZiM2huNXI2OWNnMzQ4NSIsIm9yaWdpbl9qdGkiOiI1NzMwODc4Mi1iODNhLTQ2MmQtODgwMi0wNTYyMWRjNmE1ZjUiLCJldmVudF9pZCI6ImJjYjA2NDE0LWFmZGQtNDVkNi1iMmIxLWU1OTM2ZDk0ZWI2OSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoib3BlbmlkIiwiYXV0aF90aW1lIjoxNzMxOTA3ODU4LCJleHAiOjE3MzE5MTE0NTgsImlhdCI6MTczMTkwNzg1OCwianRpIjoiZTA2YzZkN2EtNjJkMC00YTU3LWJiNWMtYmNlMTNmNDhiYmQxIiwidXNlcm5hbWUiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTcifQ.naTFRmb6LjMl0l7kXfwW6GsBgqoOflRfeaZroziZfj84SdZx7Mc5nFxrR5fteaDIrjlkrBVcTdcilY5lhzoIZxmgIOqqFknDG1s5qj5ltvCKHubqdiaiuXse2h7x8TwHOacO8TQ-pVnn1olhCk4XadAr3CN_NGHfFpjrcbWmbIUoF9-CbZ_MutQHrFtJylqiI2G52NO0OUPv27OJQYCSG-r1TUXQrt9F9Cgtdaz9vnN5UAFfhQh82QgI7nhyDdg0cc8l28ghcCfZRIHkXjNJEMcS9zCK9mehq92JG1TQuV24-qbzLPS_xp3MlYZWXx760cIz_wtW7iAfnQFlrf0p8g",  # Replace with your valid token
    "x-api-key": "Bearer sk-5tWPlrwVTszYu_wk_HjRNA"  # Replace with your valid API key
}

# Define S3 buckets and keys
zip_bucket_name = "asu-hackathon-data-727646476284"
zip_file_key = "asu_hackathon_files.zip"
output_bucket_name = "hackathon-output-bucket-258964"


def lambda_handler(event, context):
    try:

        for record in event['Records']:
            sns_message = json.loads(record['Sns']['Message'])
            print(f"Received SNS message: {sns_message}")
            # Process files (limit to first 3,000 rows)
            file_data = download_and_extract_zip(zip_bucket_name, zip_file_key, max_rows=500)
            profile_data_and_generate_files(file_data, output_bucket_name)
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Data profiling completed successfully"})
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def download_and_extract_zip(bucket_name, zip_key, max_rows=500):
    """
    Download and extract ZIP file from S3 and limit to 3,000 rows per file.
    """
    response = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = BytesIO(response["Body"].read())
    file_data = {}
    with zipfile.ZipFile(zip_data, "r") as z:
        print("Files in the ZIP archive:", z.namelist())
        for file_name in z.namelist():
            if not file_name.endswith("/"):
                with z.open(file_name) as file:
                    try:
                        df = pd.read_csv(file)
                        file_data[file_name] = df.iloc[:max_rows]  # Limiting to first 3,000 rows
                    except Exception as e:
                        print(f"Error reading {file_name}: {e}")
    return file_data

def truncate_column_values(df, max_length=50):
    """
    Truncate long string values in the DataFrame for concise API prompts.
    """
    truncated_df = df.copy()
    for col in truncated_df.select_dtypes(include=["object"]).columns:
        truncated_df[col] = truncated_df[col].astype(str).str[:max_length]
    return truncated_df

def upload_file_to_s3(file_buffer, bucket_name, key):
    """
    Upload a file buffer to S3.
    """
    s3.put_object(Bucket=bucket_name, Key=key, Body=file_buffer.getvalue())
    print(f"File uploaded successfully to s3://{bucket_name}/{key}")

def call_api(prompt, retries=3, delay=2):
    """
    Call the Claude API with a prompt.
    """
    payload = {
        "model": "Azure OpenAI GPT-4o (External)",
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            else:
                print(f"API request failed (Attempt {attempt + 1}/{retries}) with status code {response.status_code}")
                print(response.text)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred (Attempt {attempt + 1}/{retries}): {e}")
        time.sleep(delay)
    return None

def process_whole_data(file_name, df):
    """
    Send the entire dataset for profiling and handle API response gracefully.
    """
    truncated_df = truncate_column_values(df)
    data_sample = truncated_df.to_string(index=False)
    prompt = f"""
    Analyze the dataset below and provide a detailed column-wise profiling in a tab-separated table format.
    For each column, include:
    - Column Name
    - Data Type : Data type of the column (e.g., integer, string, date, etc.).
    - Column Description : A short description of what the column likely represents based on its name and sample data.
    - Unique Values : Number of unique values in the column.
    - % Unique Values : Percentage of unique values in the column.
    - Null Values : Number of null (missing) values in the column.
    - % Null Values : Percentage of null (missing) values in the column.
    - Minimum Value : The minimum value in the column (for numeric values only).
    - Maximum Value : The maximum value in the column (for numeric values only).
    - Mode : The most frequently occurring value(s) in the column (for numeric values only).
    - Outliers : Any possible outliers in the column.

    Only return a single tab-separated table.
    Dataset:
    {data_sample}
    """
    print(f"Sending full data for profiling: {file_name}")
    response = call_api(prompt)

    # Log raw response for debugging
    if response is None:
        print(f"Error: No response for file {file_name}.")
        return None
    print(f"Raw response for {file_name}:\n{response[:500]}...")  # Log first 500 chars for debugging

    try:
        # Validate and clean the response
        report_lines = response.strip().split("\n")
        structured_data = [line.split("\t") for line in report_lines if len(line.split("\t")) == 11]
        if not structured_data:
            print(f"Error: Profiling report for {file_name} is empty or not properly structured.")
            return None
        return structured_data
    except Exception as e:
        print(f"Error parsing response for {file_name}: {e}")
        return None


def process_file(file_name, df, output_bucket):
    """
    Process an individual file by sending the data for profiling and saving results.
    """
    print(f"Processing file: {file_name}")
    profiling_results = process_whole_data(file_name, df)
    if not profiling_results:
        print(f"Error: Profiling report for {file_name} is empty or not properly structured.")
        return

    try:
        # Convert structured data to DataFrame
        headers = profiling_results[0]
        data_rows = profiling_results[1:]
        report_df = pd.DataFrame(data_rows, columns=headers)

        # Save to CSV
        file_buffer = BytesIO()
        report_df.to_csv(file_buffer, index=False)
        file_buffer.seek(0)  # Reset buffer pointer

        upload_file_to_s3(file_buffer, output_bucket, f"{file_name.replace('.csv', '_profiling.csv')}")
        print(f"Data profiling for {file_name} completed successfully!")
    except Exception as e:
        print(f"Error parsing or saving profiling results for {file_name}: {e}")


def profile_data_and_generate_files(file_data, output_bucket):
    """
    Profile each file concurrently.
    """
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, file_name, df, output_bucket)
                   for file_name, df in file_data.items()]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")

