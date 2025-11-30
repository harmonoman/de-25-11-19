import boto3
from botocore.exceptions import ClientError


class S3FileClient:
    """
    A hybrid S3 client that automatically chooses the best authentication method.

    Authentication priority:
    1. Explicit AWS credentials (aws_access_key / aws_secret_key)
    2. Named AWS profile (aws_profile)
    3. Default AWS credential chain (env vars, EC2/ECS role, etc.)

    Supports:
    - Uploading files from disk
    - Downloading files to disk
    - Checking if an S3 object exists
    """

    def __init__(
        self,
        logger,
        aws_profile: str | None = None,
        aws_access_key: str | None = None,
        aws_secret_key: str | None = None,
        region_name: str | None = None
    ):
        """
        Initialize the S3 client using the strongest available authentication method.

        Parameters
        ----------
        logger : logging.Logger
            Logger instance used for structured logging.
        aws_profile : str, optional
            Name of the AWS CLI profile to use.
        aws_access_key : str, optional
            Explicit AWS access key ID.
        aws_secret_key : str, optional
            Explicit AWS secret access key.
        region_name : str, optional
            AWS region to use for the S3 client.

        Notes
        -----
        Authentication resolution occurs in the following order:
        1. Explicit access key + secret key
        2. AWS profile
        3. Default AWS credential chain
        """
        self.logger = logger
        self.region_name = region_name

        # ------------------------------
        # 1. Explicit Access Keys
        # ------------------------------
        if aws_access_key and aws_secret_key:
            self.logger.info("Initializing S3 client with explicit AWS credentials.")
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
            self.s3 = session.client("s3")
            return

        # ------------------------------
        # 2. AWS Profile
        # ------------------------------
        if aws_profile:
            self.logger.info(f"Initializing S3 client using AWS profile: {aws_profile}")
            session = boto3.Session(profile_name=aws_profile, region_name=region_name)
            self.s3 = session.client("s3")
            return

        # ------------------------------
        # 3. Default AWS Credential Chain
        # ------------------------------
        self.logger.info("Initializing S3 client using default AWS credential chain.")
        self.s3 = boto3.client("s3", region_name=region_name)

    # -------------------------------------------------------------------------
    # Upload
    # -------------------------------------------------------------------------
    def upload_file(self, local_path: str, bucket: str, key: str) -> bool:
        """
        Upload a local file to an S3 bucket.

        Parameters
        ----------
        local_path : str
            Path to the local file on disk.
        bucket : str
            Name of the S3 bucket.
        key : str
            Full S3 object key (path inside the bucket).

        Returns
        -------
        bool
            True if the upload succeeds, False if an S3 error occurs.
        """
        try:
            self.logger.info(f"Uploading {local_path} → s3://{bucket}/{key}")
            self.s3.upload_file(local_path, bucket, key)
            return True
        except ClientError as e:
            self.logger.error(f"S3 upload failed: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------------------
    # Download
    # -------------------------------------------------------------------------
    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        """
        Download an S3 object to a local file.

        Parameters
        ----------
        bucket : str
            Name of the S3 bucket.
        key : str
            Key of the S3 object to download.
        local_path : str
            Local path where the file will be saved.

        Returns
        -------
        bool
            True if the download succeeds, False if an S3 error occurs.
        """
        try:
            self.logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
            self.s3.download_file(bucket, key, local_path)
            return True
        except ClientError as e:
            self.logger.error(f"S3 download failed: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------------------
    # Exists
    # -------------------------------------------------------------------------
    def exists(self, bucket: str, key: str) -> bool:
        """
        Check whether an S3 object exists.

        Parameters
        ----------
        bucket : str
            Name of the S3 bucket.
        key : str
            Key of the S3 object to check.

        Returns
        -------
        bool
            True if the object exists, False otherwise.
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
