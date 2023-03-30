import boto3
import pandas as pd
import re
import os

class DataStandardize:
    def __init__(self, bucket_name, key, aws_access_key_id, aws_secret_access_key,encoding='utf-8',errors='ignore'):
        self.bucket_name = bucket_name
        self.key = key
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key)
        self.obj = self.s3.get_object(Bucket=self.bucket_name, Key=self.key)
        self.df = pd.read_csv(self.obj['Body'], encoding=encoding)

    def remove_ascii_chars(self):
        self.df = self.df.applymap(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))

    def standardize_date_format(self, date_cols):
        for col in date_cols:
            self.df[col] = pd.to_datetime(self.df[col], format='%d-%b-%y %H.%M.%S.%f').dt.strftime('%Y-%m-%d')

    def remove_extra_quotes(self):
        self.df.columns = self.df.columns.str.replace('"', '')

    def upload_cleaned_data(self, dest_key,encoding='utf-8'):
        #cleaned_data = self.df.to_csv(index=False, encoding=encoding)
        cleaned_data = self.df.to_parquet(index=False)
        self.s3.put_object(Bucket=self.bucket_name, Key=dest_key, Body=cleaned_data)
        print(f"Successfully cleaned and uploaded {self.key} to {dest_key} in S3.")

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
BUCKET_NAME = 'org-sample-data'

if __name__ == "__main__":
    user_companies = DataStandardize(
        bucket_name=BUCKET_NAME,
        key='Input/user_companies.csv',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        encoding = 'ISO-8859-1'
    )
    user_companies.remove_ascii_chars()
    user_companies.standardize_date_format(date_cols=['HAS_AGREED_WITH_TERMS_AT', 'USER_ACTIVATION_LAST_NOTIFIED_AT'])
    user_companies.remove_extra_quotes()
    user_companies.upload_cleaned_data(dest_key='standardized_data/user_companies_csv/user_companies_cleaned.parquet')

    user_login_locations = DataStandardize(
        bucket_name=BUCKET_NAME,
        key='Input/user_login_locations.csv',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        encoding='ISO-8859-1'
    )
    user_login_locations.remove_ascii_chars()
    user_login_locations.standardize_date_format(date_cols=['CREATED_AT'])
    user_login_locations.remove_extra_quotes()
    user_login_locations.upload_cleaned_data(dest_key='standardized_data/user_login_locations_csv/user_login_locations_cleaned.parquet')