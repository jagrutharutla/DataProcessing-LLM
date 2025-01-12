import boto3
import zipfile
import requests
import pandas as pd
import json
from io import BytesIO
import time
import math

def lambda_handler(event, context):
    try:
        process_all_files()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Quality checks processed and stored successfully"})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# AWS S3 configuration
input_bucket = "asu-hackathon-data-727646476284"
output_bucket = "hackathon-output-bucket-258964"
zip_file_key = "asu_hackathon_files.zip"
output_folder = "original_quality_check"

api_url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": "eyJraWQiOiJpb2dUeXR5amt4WFVtcWM5TFVEVmN4NEx3VkY4WEtnN0RzRlwvUU1yMngzWT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTciLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV82cU4zY0tGZWIiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3djE1ZW01YTBpcXZiM2huNXI2OWNnMzQ4NSIsIm9yaWdpbl9qdGkiOiJmNTM0ZTEwYi00ZmIwLTQ0MjEtOGE4Mi01ZWE0ZGQzNjVhMjYiLCJldmVudF9pZCI6IjA0YWMyNDBhLTA4M2ItNDdjOC1iODdhLTNhYTMyNGI4OWZlNSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoib3BlbmlkIiwiYXV0aF90aW1lIjoxNzMxODcxNDA1LCJleHAiOjE3MzE5MDU0MTAsImlhdCI6MTczMTkwMTgxMCwianRpIjoiMjNhYTA2MjEtZWM2YS00ZjRlLWI3N2QtNjBkMjM4NjE2NTFiIiwidXNlcm5hbWUiOiJmNDM4ZDQxOC0yMDIxLTcwZDAtZmJmZC1iZjQ2NTc0MzI5OTcifQ.Nk93QU5oNk1GBaMJ2DVpaBfme9D2gbRnc88lmvVoq3HfXRAAEDJ2B3t1E71qPSF5OEkVyhtAzS_OR99R3w-02r5EmhS9sFGyv_TmqddBpiklwT-KuzuSw00R57IIpCQrBdWNPhMYrHaMOg0mVwVE3MrR1CUg3ECXZHQ_wEejiU2Bm-qzbuh9SD6pUonzUdSD5NEMM7R12s5M6h89AH0lyWWz8Jdvo2CCxYsWu1iEGoMaOnL8GEAMNI3NDcbDNdPrJ7AX8dvb7sqQaJ1zJEi-XZ6RzhQjy2MQcwC5yB7XEp7hHvADztfMrxYeaDNMGfxUKtmu2GZ0gE2gn2_i9hYUiw",
    "x-api-key": "Bearer sk-5tWPlrwVTszYu_wk_HjRNA"
}

s3 = boto3.client("s3")

MAX_ROWS_TO_PROCESS = 100  # Limit to the first 3000 rows
MAX_ROWS_PER_BATCH = 50    # Process in batches of 200 rows
MAX_RETRIES = 3            # Maximum retries for API requests

# Function to download and extract all files from a ZIP file
def download_and_extract_all_files(bucket_name, zip_key):
    response = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = BytesIO(response["Body"].read())
    
    extracted_files = {}
    with zipfile.ZipFile(zip_data, "r") as z:
        for file_name in z.namelist():
            if file_name.endswith('.csv'):
                try:
                    with z.open(file_name) as file:
                        extracted_files[file_name] = pd.read_csv(file)
                except Exception as e:
                    print(f"Error processing file {file_name}: {e}")
    return extracted_files

# Function to upload a file to S3
def upload_file_to_s3(data, bucket_name, key):
    s3.put_object(Bucket=bucket_name, Key=key, Body=data.encode("utf-8"))
    print(f"File uploaded successfully to s3://{bucket_name}/{key}")

# Function to save the report locally
def save_report_locally(data, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"Report saved locally as {file_name}")
    
def exponential_backoff_retry(attempt):
    return min(2 ** attempt, 30)
    
# Step 3: Process Data in Batches
def process_in_batches(df, batch_size):
    """
    Processes a DataFrame in batches and sends each batch to the API.

    Args:
        df (DataFrame): Input DataFrame to process.
        batch_size (int): Number of rows per batch.

    Returns:
        str: Combined JSON reports from all batches.
    """
    df = df.head(MAX_ROWS_TO_PROCESS)  # Limit to the first MAX_ROWS_TO_PROCESS rows
    num_batches = math.ceil(len(df) / batch_size)
    batch_reports = []

    for i in range(num_batches):
        start_row = i * batch_size
        end_row = min((i + 1) * batch_size, len(df))
        batch_df = df.iloc[start_row:end_row]
        print(f"Processing batch {i + 1}/{num_batches}: Rows {start_row} to {end_row}")

        for attempt in range(MAX_RETRIES):
            try:
                csv_content = batch_df.to_csv(index=False)
                payload = {
                    # "model": "Anthropic Claude-V3.5 Sonnet (Internal)",
                    "model": "Azure OpenAI GPT-4o (External)",
                    "messages": [
                        {
                            "role": "user",
                            "content": f"""
                                Perform a detailed quality check on this CSV dataset and suggest necessary transformations to improve the quality of the data. 
                                The output should be in a structured JSON format with the following sections:

                                ### Sections:
                                1. **File Overview**:
                                   - File name.
                                   - Number of rows and columns.
                                   - List of column names with their data types.

                                2. **Quality Check Summary**:
                                   - Provide a list of detected issues in the dataset with details.

                                3. **Detailed Quality Checks**:
                                   - **Completeness**:
                                     - Identify missing values for each column.
                                     - Suggest transformations to handle missing data, such as filling with mean/median, forward-fill, or removing rows with missing values.
                                   - **Accuracy**:
                                     - Validate correctness of data values against logical constraints or expected ranges (e.g., no negative values in age, date formats).
                                     - Highlight anomalies and suggest transformations to fix these issues.
                                   - **Consistency**:
                                     - Identify logical inconsistencies between related columns (e.g., `start_date` is after `end_date`).
                                     - Recommend transformations to ensure internal coherence, such as fixing mismatches or reformatting date columns.
                                   - **Uniqueness**:
                                     - Check for duplicate rows or non-unique values in key columns.
                                     - Suggest transformations to remove duplicates or enforce uniqueness constraints.
                                   - **Conformity**:
                                     - Validate that all values adhere to expected formats:
                                       - Dates should follow `YYYY-MM-DD`.
                                       - Emails should be valid.
                                       - Phone numbers should match the required format.
                                     - Suggest transformations to fix non-conforming values.
                                   - **Statistical Properties**:
                                     - Analyze numeric columns for outliers using interquartile range (IQR) or standard deviation.
                                     - Recommend transformations to handle outliers, such as winsorization or capping extreme values.
                                   - **Categorical Column Cleanup**:
                                     - Identify inconsistent or incorrect categories in categorical fields.
                                     - Suggest transformations to clean and standardize categories.

                                4. **Recommended Transformations**:
                                   - A summary of all transformations to improve data quality.

                                5. **Transformed Dataset**:
                                   - Provide a preview of the dataset after applying the suggested transformations (limited to the first 10 rows).

                                Ensure all recommendations are tailored to the specific characteristics of this dataset. Return the output in a structured JSON format.

                                Dataset:
                                {csv_content}
                            """
                        }
                    ],
                    "stream": False
                }

                response = requests.post(api_url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    result = response.json()
                    content = result.get("choices", [])[0].get("message", {}).get("content", "")
                    batch_reports.append(content)
                    break  # Exit retry loop on success
                else:
                    print(f"API request failed with status {response.status_code}: {response.text}")
            except Exception as e:
                print(f"Error during API request for batch {i + 1}, attempt {attempt + 1}: {e}")

            time.sleep(2)  # Delay before retrying

        else:
            print(f"Batch {i + 1} failed after {MAX_RETRIES} retries.")
        time.sleep(1)  
        
    return "\n".join(batch_reports)

def post_process_json(raw_json):
    """
    Cleans and beautifies a JSON object by formatting it with proper indentation 
    and removing unnecessary characters like newlines in string values.

    Args:
    - raw_json (str or dict): The JSON object as a string or dictionary.

    Returns:
    - str: A beautified JSON string.
    """
    try:
        # If raw_json is a string, parse it into a dictionary
        if isinstance(raw_json, str):
            json_data = json.loads(raw_json)
        else:
            json_data = raw_json

        # Beautify the JSON with indentation and sorting keys
        beautified_json = json.dumps(json_data, indent=4, ensure_ascii=False)
        beautified_json = beautified_json.replace("\\n", "").replace("\\", "")

        return beautified_json

    except Exception as e:
        raise ValueError(f"Error processing JSON: {e}")


# Step 5: Process All Files
def process_all_files():
    """
    Processes all CSV files in the ZIP archive by extracting them, processing in batches,
    post-processing the output, and uploading the results to S3.
    """
    try:
        print("Downloading and extracting files from ZIP...")
        extracted_files = download_and_extract_all_files(input_bucket, zip_file_key)
    except Exception as e:
        print(f"Error downloading or extracting files: {e}")
        return

    for file_name, df in extracted_files.items():
        print(f"Processing file: {file_name} with {len(df)} rows...")
        start_time = time.time()

        try:
            # Process in batches
            quality_report = process_in_batches(df, MAX_ROWS_PER_BATCH)

            # Post-process and beautify the JSON
            beautified_output = post_process_json({"report": quality_report})

            # Upload to S3
            report_key = f"{output_folder}/{file_name.replace('.csv', '_quality_check.json')}"
            upload_file_to_s3(beautified_output, output_bucket, report_key)

            end_time = time.time()
            print(f"Quality check for {file_name} completed successfully! Time taken: {end_time - start_time:.2f} seconds.")

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")


    
    
