import os
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from faker import Faker
from google.cloud import storage

# Initialize Faker for generating fake data
fake = Faker()

# Define the number of rows to generate
num_rows = 10000

# budget name
bucket_name = os.getenv('BUCKET_NAME')

# Create a list of dictionaries with fake customer data

for i in range(50):
    data = []
    for _ in range(num_rows):
        data.append({
            "id": str(uuid.uuid4()),  # Generate a UUID for each row
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address(),
            "city": fake.city(),
            "state": fake.state(),
            "zip_code": fake.zipcode(),
            "country": fake.country(),
            "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "gender": fake.random_element(elements=("Male", "Female", "Other")),
            # Add more fields as needed
        })

    # Create a Pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Create a Google Cloud Storage client
    storage_client = storage.Client()

    # File path
    file_path = f"customer_data_{i}.parquet"

    # Open a write stream to the bucket
    with storage_client.bucket(bucket_name).blob(file_path).open("wb") as f:
        # Write the Parquet file to the stream
        pq.write_table(table, f)

    # Print a success message
    print(f"Successfully saved {num_rows} rows of customer data to gs://{bucket_name}/{file_path}")
