import boto3
import pandas as pd
import requests
import json
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from io import StringIO
import time
import logging

# Define S3 buckets and keys
zip_bucket_name = "asu-hackathon-data-727646476284"
zip_file_key = "asu_hackathon_files.zip"
output_bucket_name = "hackathon-output-bucket-258964"



def lambda_handler(event, context):
    try:
        file_data = download_and_extract_zip(zip_bucket_name, zip_file_key)
        clean_all_files(file_data, output_bucket_name)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data is cleaned"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }



# Initialize S3 client
s3 = boto3.client('s3')
# Setup logging
logging.basicConfig(level=logging.INFO, filename="data_cleaning3.log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
# API Endpoint and Headers
url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": "eyJraWQiOiJpb2dUeXR5amt4WFVtcWM5TFVEVmN4NEx3VkY4WEtnN0RzRlwvUU1yMngzWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1NGI4MTQ2OC05MGQxLTcwZGQtZTYwNy04ZWIwZTUzNDlhNGIiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV82cU4zY0tGZWIiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3djE1ZW01YTBpcXZiM2huNXI2OWNnMzQ4NSIsIm9yaWdpbl9qdGkiOiJhMzc3Mjc5YS04NDFlLTQ2ZTItODVmZi0zMjU3OTM5ODVhOTciLCJldmVudF9pZCI6ImE4NWIzNzllLTE0MmUtNGM2NC05ZjZjLWQzNjAyNDgyY2I0NCIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoib3BlbmlkIiwiYXV0aF90aW1lIjoxNzMxOTAyNzI0LCJleHAiOjE3MzE5MDYzMjQsImlhdCI6MTczMTkwMjcyNCwianRpIjoiZjhlMjk5MTQtMTQ2Zi00ZThiLTkyNzEtOWYyODlmNTRmOWI0IiwidXNlcm5hbWUiOiI1NGI4MTQ2OC05MGQxLTcwZGQtZTYwNy04ZWIwZTUzNDlhNGIifQ.LvVbU_-L3eck7ElV7RH2eI0aISac9r8V975uG4uuv2Tb3G_eyh1m1VYsqQvImQVYfdkx5lb1pjDOP6ETC9yeRO3smslVRYoYSmy3OJVhquWF2lLIqYq02IsWNjF7Fag8kXzi64HCKQ4noMLJxVL9xFgvwWx1YuR-pNI1dtAerxITCb9_K8WpKer1XdFpRP_QzQQH3sWl5ACtPrhkAqvxmELVKIaa_08qiCppdWU1u7nrJP1DiGMudwYm8fGZHUDLNpVDecLgUE7UgFcJf11hz0cXGaZiXHqPD-sYPb0lqqN66KW_akoWhmZr0_2eyTIQq6F0q_2I1kK37GpDJX4NRA",  # Replace with your authorization token
    "x-api-key": "Bearer sk-5tWPlrwVTszYu_wk_HjRNA"  # Replace with your API Key
}
def download_and_extract_zip(bucket_name, zip_key):
    """
    Download and extract ZIP file from S3 and return a dictionary of DataFrames.
    """
    response = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = BytesIO(response['Body'].read())
    file_data = {}
    with zipfile.ZipFile(zip_data, 'r') as z:
        logger.info(f"Files in the ZIP archive: {z.namelist()}")
        for file_name in z.namelist():
            if not file_name.endswith('/'):  # Exclude directories
                with z.open(file_name) as file:
                    try:
                        df = pd.read_csv(file)
                        file_data[file_name] = df.iloc[:500]
                    except Exception as e:
                        logger.error(f"Error reading {file_name}: {e}")
    return file_data
def call_gpt4_api_with_retries(prompt, max_retries=3):
    """
    Call the GPT-4 API with retries and exponential backoff.
    """
    retries = 0
    while retries < max_retries:
        try:
            payload = {
                "model": "Azure OpenAI GPT-4o (External)",
                "messages": [{"role": "user", "content": prompt}],
                "stream": False
            }
            logger.info(f"Making API call... Attempt {retries + 1}")
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            logger.info(f"API Response Status: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                logger.info(f"API call succeeded.")
                return result['choices'][0]['message']['content']
            elif response.status_code == 503:
                logger.warning(f"Service Unavailable. Retrying... Attempt {retries + 1}")
                time.sleep(2 ** retries)  # Exponential backoff
                retries += 1
            else:
                logger.error(f"API request failed with status code {response.status_code}")
                logger.error(response.text)
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            retries += 1
            time.sleep(2 ** retries)
    logger.error("Max retries reached. Failed to clean chunk.")
    return None
def clean_chunk(chunk_df):
    """
    Clean a chunk of the file using the GPT-4 API and return the cleaned data.
    """
    prompt = f"""
    You are given the following dataset in dataframe format:
    ```
    {chunk_df}
    Perform data cleaning on the provided data:
    1. Remove duplicate rows.
    2. Analyze missing data types (MCAR, MAR, MNAR) and determine their nature using statistical tests.
    3. Impute missing values using appropriate methods:
       - MCAR: Mean/median/mode imputation.
       - MAR: Multiple or regression-based imputation.
       - MNAR: Suggest domain-specific approaches.
    4. Identify and handle anomalies in numerical columns (e.g., replace unrealistic ages with the median).
    5. Apply all the above changes to the data_sample and return updated data.
    Only return a single tab-separated table.
    I don't want any additional explanation or text.
    If you are receiving data from same file in chunks, return uniform data every single time.
    Sample format of the output for a file with 2 columns:
    column_one,name
    101,sathwika
    202,jane
    303,john
    """
    logger.info("Sending cleaning request to GPT-4 API for a chunk...")
    response = call_gpt4_api_with_retries(prompt)
    if response:
        cleaned_data = response.strip().splitlines()
        # Remove the first and last lines from the output
        cleaned_data = cleaned_data[1:-1]
        return cleaned_data
    else:
        logger.error("Failed to clean chunk.")
        return []
def process_large_file(file_name, df, output_bucket, chunk_size=50):
    """
    Split the DataFrame into smaller chunks, clean each chunk, and combine them into a single file.
    """
    num_chunks = (len(df) + chunk_size - 1) // chunk_size  # Calculate number of chunks
    string_buffer = StringIO()
    writer = csv.writer(string_buffer)  # Default is comma-separated
    header_written = False  # Track whether the header has been written
    for i in range(num_chunks):
        chunk_df = df.iloc[i * chunk_size: (i + 1) * chunk_size]
        logger.info(f"Processing chunk {i + 1}/{num_chunks} for file {file_name}")
        cleaned_chunk = clean_chunk(chunk_df)
        if i > 0:
            cleaned_chunk = cleaned_chunk[1:-1]
        if cleaned_chunk:
            for row in cleaned_chunk:
                # Write header only once
                if not header_written:
                    header = row.strip().split("\t")
                    writer.writerow(header)
                    header_written = True
                else:
                    values = row.strip().split("\t")
                    writer.writerow(values)
        else:
            logger.error(f"Chunk {i + 1} of file {file_name} failed to clean.")
            continue  # Skip to the next chunk
    # Encode the StringIO content to bytes
    string_buffer.seek(0)
    byte_buffer = BytesIO(string_buffer.getvalue().encode('utf-8'))
    # Upload the combined cleaned file to S3
    file_name = file_name.split("/")[1]
    cleaned_file_name = file_name.replace(".csv", "_cleaned.csv")
    upload_file_to_s3(byte_buffer, output_bucket, cleaned_file_name)
    logger.info(f"File {file_name} cleaned and uploaded successfully as a single CSV!")
def upload_file_to_s3(file_buffer, bucket_name, key):
    """
    Upload a file buffer to S3 within the 'cleaned_files' folder.
    """
    cleaned_key = f"cleaned_files/{key}"
    s3.put_object(Bucket=bucket_name, Key=cleaned_key, Body=file_buffer.read())
    logger.info(f"File uploaded successfully to s3://{bucket_name}/{cleaned_key}")
def clean_all_files(file_data, output_bucket):
    """
    Clean all files sequentially and upload the cleaned versions to S3.
    """
    for file_name, df in file_data.items():
        process_large_file(file_name, df, output_bucket, chunk_size=50)










