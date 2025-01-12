import os
import boto3
import pandas as pd
import requests
from io import BytesIO
import time

# Initialize S3 client
s3 = boto3.client("s3")

# Claude API Endpoint and Headers
url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": "eyJraWQiOiJpb2dUeXR5amt4WFVtcWM5TFVEVmN4NEx3VkY4WEtnN0RzRlwvUU1yMngzWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIwNDc4NzQ2OC04MDMxLTcwYTgtNThmYy05ZDhmMGMwY2ZhYzEiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV82cU4zY0tGZWIiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3djE1ZW01YTBpcXZiM2huNXI2OWNnMzQ4NSIsIm9yaWdpbl9qdGkiOiJhN2MxYTAyMS1kMDU3LTQ4NDMtOTBiZi1jYjgzZTRjYzkwNDciLCJ0b2tlbl91c2UiOiJhY2Nlc3MiLCJzY29wZSI6Im9wZW5pZCIsImF1dGhfdGltZSI6MTczMTg5NjUxOSwiZXhwIjoxNzMxOTAwMTE5LCJpYXQiOjE3MzE4OTY1MTksImp0aSI6IjNjNjI5MTdhLTcyOGItNDQzNS1iOGJiLTMxOWVhNDJiYmNmNiIsInVzZXJuYW1lIjoiMDQ3ODc0NjgtODAzMS03MGE4LTU4ZmMtOWQ4ZjBjMGNmYWMxIn0.SDDIOqN0w8pv2CqSCF4o_r9TrCp8d0JKHID-0imMhae9Bue6_4XiPU1_oT4p44gV9kmEvkSntr8f9hiJShEvcweTQisyutKJbCHbn16KJd2xCKsRIda2LUSeuuOwmv66k9OYM1scS-zkh-co9yfTQBU1uGRhH1CMo-ybEfxkK6xHIgkPT_sLFuQ9viXNGqyoNzgq_T7AY4inmz4H0VjhkG9iRMIG_gHWAY40Zl2PbJgtVkxXv4D3cpRYOi22IT3p_4iDSs7-DE9Yl7ZOpgkP4GDi6xY5mhnVFuGreu9Atn7Enlos7zCfhtwG4VKUUJx3rAFS4zqSJ_d4rOzj1UujtQ",
    "x-api-key": "Bearer sk-5tWPlrwVTszYu_wk_HjRNA"  # Replace with a valid API key
}

# Define S3 bucket and folder details
bucket_name = "hackathon-output-bucket-258964"
input_folder = "cleaned_files/asu_hackathon_files/"
output_folder = "DataAggregation"

# Perform data aggregation for files with a 500-row limit
perform_data_aggregation(bucket_name, input_folder, output_folder, max_rows=10)


def lambda_handler(event, context):
    try:
        

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Metadata processed and stored successfully"})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def list_files_in_s3_folder(bucket_name, folder_name):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    files = [content['Key'] for content in response.get('Contents', []) if content['Key'] != folder_name]
    return files

def download_file_from_s3(bucket_name, file_key, max_rows=500):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(BytesIO(response['Body'].read()), on_bad_lines="skip")  # Skip malformed lines
    return df.iloc[:max_rows]  # Limit to the first 500 rows

def upload_file_to_s3(file_buffer, bucket_name, key):
    s3.put_object(Bucket=bucket_name, Key=key, Body=file_buffer.getvalue())
    print(f"File uploaded successfully to s3://{bucket_name}/{key}")

def call_claude_for_aggregation(prompt, retries=3, delay=2):
    payload = {
        "model": "Anthropic Claude-V3.5 Sonnet (Internal)",
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            else:
                print(f"API request failed (Attempt {attempt + 1}/{retries}) with status code {response.status_code}")
                print(response.text)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred (Attempt {attempt + 1}/{retries}): {e}")
        time.sleep(delay)
    return None

def process_file(file_key, df, bucket_name, output_folder):
    data_sample = df.to_string(index=False)
    prompt = f"""
    You are an advanced data analyst. Perform data aggregation on the following dataset to derive actionable insights. Your task is to:
    
    1. Aggregate the data:
        * For numerical columns: Calculate sums, averages, medians, standard deviations, and counts.
        * For categorical columns: Summarize by frequency counts and relationships with numerical columns.
        * For temporal columns: Identify trends, peak periods, or averages by time intervals.
    
    2. Generate a well-structured CSV:
        * Include a title that describes the report's focus (e.g., "Product Performance Summary", "Customer Insights").
        * Include meaningful column headers and rows.
        * Group data by logical categories (e.g., by time period, categories, or regions) to create aggregated metrics.
        * Ensure the output is clean and ready for direct analysis or visualization.
    
    3. Output only the final aggregated dataset in CSV format. Avoid additional text, explanations, or steps.
    
    Dataset:
    {data_sample}
    """
    aggregation_result = call_claude_for_aggregation(prompt)
    
    if aggregation_result is None:
        print(f"Error: No response from Claude for file {file_key}.")
        return

    try:
        output_file_key = f"{output_folder}/{os.path.basename(file_key).replace('.csv', '_aggregation.csv')}"
        csv_buffer = BytesIO(aggregation_result.encode("utf-8"))
        upload_file_to_s3(csv_buffer, bucket_name, output_file_key)
    except Exception as e:
        print(f"Error saving aggregation report for {file_key}: {e}")

def perform_data_aggregation(bucket_name, input_folder, output_folder, max_rows=500):
    files = list_files_in_s3_folder(bucket_name, input_folder)
    if not files:
        print(f"No files found in S3 folder: {input_folder}")
        return

    print(f"Found files: {files}")
    for file_key in files:
        try:
            df = download_file_from_s3(bucket_name, file_key, max_rows=max_rows)
            process_file(file_key, df, bucket_name, output_folder)
        except Exception as e:
            print(f"Error processing file {file_key}: {e}")
