# Python will connect to the CRH postgres DB for each env. Fetch the DB details from Vault.
# Better Pass the DB details for each env from the GHA CI/CD Pipeline
# Then it will find the audit tables and take its backup, zip it
# Then it will store the zip files to AWS S3 buckets
# Above python script must be called via GHA CI/CD pipeline


import os
import psycopg2
import boto3
import tempfile
import zipfile
from datetime import datetime

# CONFIGURATION
# DB_CONFIG = {
#     'host': 'your-db-host',
#     'port': '5432',
#     'dbname': 'your-db-name',
#     'user': 'your-username',
#     'password': 'your-password'
# }

S3_BUCKET = 'audit-table-archives'
S3_REGION = 'us-east-1'  # e.g., 'us-east-1'


# STEP 1: Connect to PostgreSQL
# def connect_postgres():
#     conn = psycopg2.connect(**DB_CONFIG)
#     return conn


# Get all audit tables
# def get_audit_tables(conn):
#     query = """
#     SELECT table_schema, table_name
#     FROM information_schema.tables
#     WHERE table_type='BASE TABLE'
#     AND LOWER(table_name) LIKE '%audit%';
#     """
#     with conn.cursor() as cur:
#         cur.execute(query)
#         return cur.fetchall()
#


# STEP 3: Export tables to CSV and zip them
# def backup_tables(conn, tables):
#     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#     zip_filename = f"audit_backup_{timestamp}.zip"
#     zip_path = os.path.join(tempfile.gettempdir(), zip_filename)
#
#     with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#         for schema, table in tables:
#             filename = f"{schema}.{table}.csv"
#             csv_path = os.path.join(tempfile.gettempdir(), filename)
#             with open(csv_path, 'w') as f:
#                 with conn.cursor() as cur:
#                     cur.copy_expert(f"COPY {schema}.{table} TO STDOUT WITH CSV HEADER", f)
#             zipf.write(csv_path, arcname=filename)
#             os.remove(csv_path)
#
#     return zip_path


# def upload_to_s3(file_path):
#     s3 = boto3.client('s3', region_name=S3_REGION)
#     file_key = os.path.basename(file_path)
#     s3.upload_file(file_path, S3_BUCKET, file_key)
#     print(f"✅ Uploaded to s3://{S3_BUCKET}/{file_key}")
#     os.remove(file_path)

def upload_zip_files_to_s3(local_dir, bucket_name, s3_prefix="audit_zipped_files"):
    """
    Uploads all .zip files from the given local directory to the specified S3 bucket.

    :param local_dir: Local directory containing .zip files
    :param bucket_name: S3 bucket name
    :param s3_prefix: Optional folder (prefix) inside the S3 bucket
    """
    s3 = boto3.client('s3')

    if not os.path.exists(local_dir):
        raise ValueError(f"Directory does not exist: {local_dir}")

    for file_name in os.listdir(local_dir):
        if file_name.endswith(".zip"):
            local_file_path = os.path.join(local_dir, file_name)
            s3_key = f"{s3_prefix}/{file_name}" if s3_prefix else file_name

            print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
            s3.upload_file(local_file_path, bucket_name, s3_key)


# MAIN
def main():
    import sys
    # conn = connect_postgres()
    # try:
    # tables = get_audit_tables(conn)
    # if not tables:
    #     print("⚠️ No audit tables found.")
    #     return
    #
    # zip_path = backup_tables(conn, tables)
    # upload_to_s3(zip_path)
    # zip_file = 'audit_backup.zip'
    # bucket_name = S3_BUCKET
    # s3_key = f'audit-backups/{zip_file}'
    # upload_file_to_s3(zip_file, bucket_name, s3_key)

    local_path = sys.argv[1] if len(sys.argv) > 1 else "./zipped_files"
    bucket = os.environ.get("S3_BUCKET_NAME", "audit-table-archives")
    s3_folder = os.environ.get("S3_FOLDER", "audit_zipped_files")  # e.g., "zipped_backups"

    upload_zip_files_to_s3(local_path, bucket, s3_folder)

# finally:
# conn.close()

if __name__ == "__main__":
    main()
