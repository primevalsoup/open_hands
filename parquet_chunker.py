import os
import math
import pandas as pd
import boto3
from typing import Optional

def chunk_and_upload_parquet(
    input_file: str,
    bucket_name: str,
    chunk_size_mb: int = 250,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    prefix: str = ""
) -> None:
    """
    Takes a parquet file, splits it into chunks of specified size, and uploads to S3.
    
    Args:
        input_file: Path to input parquet file
        bucket_name: Name of the S3 bucket
        chunk_size_mb: Size of each chunk in MB (default: 250)
        aws_access_key_id: AWS access key ID (optional if using AWS credentials file)
        aws_secret_access_key: AWS secret access key (optional if using AWS credentials file)
        prefix: Prefix for S3 object keys (optional)
    """
    # Read the parquet file
    df = pd.read_parquet(input_file)
    
    # Calculate total size and number of chunks needed
    total_size_bytes = df.memory_usage(deep=True).sum()
    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    num_chunks = math.ceil(total_size_bytes / chunk_size_bytes)
    
    # Calculate rows per chunk (approximate)
    rows_per_chunk = len(df) // num_chunks
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    ) if (aws_access_key_id and aws_secret_access_key) else boto3.client('s3')
    
    # Get base filename without extension
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    
    # Split and upload chunks
    for i in range(num_chunks):
        start_idx = i * rows_per_chunk
        end_idx = None if i == num_chunks - 1 else (i + 1) * rows_per_chunk
        
        # Create chunk
        chunk_df = df.iloc[start_idx:end_idx]
        
        # Create temporary file for chunk
        chunk_filename = f"temp_chunk_{i}.parquet"
        chunk_df.to_parquet(chunk_filename)
        
        # Upload to S3
        s3_key = f"{prefix}{base_filename}_part_{i+1:03d}.parquet"
        s3_client.upload_file(chunk_filename, bucket_name, s3_key)
        
        # Remove temporary file
        os.remove(chunk_filename)
        
        print(f"Uploaded chunk {i+1}/{num_chunks} to s3://{bucket_name}/{s3_key}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Split and upload parquet file to S3")
    parser.add_argument("input_file", help="Path to input parquet file")
    parser.add_argument("bucket_name", help="S3 bucket name")
    parser.add_argument("--chunk-size", type=int, default=250,
                      help="Size of each chunk in MB (default: 250)")
    parser.add_argument("--aws-access-key-id", help="AWS access key ID")
    parser.add_argument("--aws-secret-access-key", help="AWS secret access key")
    parser.add_argument("--prefix", default="", help="Prefix for S3 object keys")
    
    args = parser.parse_args()
    
    chunk_and_upload_parquet(
        args.input_file,
        args.bucket_name,
        args.chunk_size,
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.prefix
    )