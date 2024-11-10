import boto3
from concurrent.futures import ThreadPoolExecutor

# Initialize S3 client
s3_client = boto3.client('s3')


def update_lifecycle(bucket_name):
    """
    Updates the lifecycle policy of the given S3 bucket to delete objects after 1 day,
    delete expired object delete markers, and delete incomplete multipart uploads.
    """
    lifecycle_configuration = {
        'Rules': [
            {
                'ID': 'DeleteObjectsAfter1Day',
                'Filter': {'Prefix': ''},
                'Status': 'Enabled',
                'Expiration': {'Days': 1},  # Deletes objects 1 day after creation
                'NoncurrentVersionExpiration': {'NoncurrentDays': 1},  # For versioned objects
            },
            {
                'ID': 'DeleteExpiredObjectDeleteMarkers',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Expiration': {
                    'ExpiredObjectDeleteMarker': True  # Deletes expired object delete markers
                },
            },
            {
                'ID': 'DeleteIncompleteMultipartUploads',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'AbortIncompleteMultipartUpload': {
                    'DaysAfterInitiation': 1  # Deletes incomplete multipart uploads after 1 day
                },
            },
        ]
    }

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print(f"Updated lifecycle policy for bucket: {bucket_name}")
    except Exception as e:
        print(f"Error updating lifecycle for bucket {bucket_name}: {e}")


def process_bucket(bucket_name, prefix):
    """
    Checks if a bucket matches the prefix and applies lifecycle policies.
    """
    if bucket_name.startswith(prefix):
        print(f"Processing bucket: {bucket_name}")
        # Apply lifecycle rule to delete objects after 1 day
        update_lifecycle(bucket_name)


def main(prefix):
    # List all buckets
    buckets = s3_client.list_buckets()['Buckets']
    bucket_names = [bucket['Name'] for bucket in buckets]

    # Use ThreadPoolExecutor for multithreading
    with ThreadPoolExecutor(max_workers=15) as executor:
        # Process each bucket in a separate thread, passing the prefix as an additional argument
        executor.map(lambda bucket_name: process_bucket(bucket_name, prefix), bucket_names)


if __name__ == "__main__":
    main('my-test-')
