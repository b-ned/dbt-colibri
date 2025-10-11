"""
Remote Manifest Support

Handles downloading manifests from remote locations (S3, GCP, HTTP).
"""

import hashlib
import json
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


def download_manifest(url: str, cache_dir: str) -> str:
    """
    Download a manifest from a remote location and cache it locally.

    Args:
        url: Remote URL (http://, https://, s3://, gs://)
        cache_dir: Directory to cache the downloaded file

    Returns:
        Path to the cached local file
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    # Create cache directory
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # Generate a consistent cache filename based on URL
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    cache_file = cache_path / f"colibri-manifest-{url_hash}.json"

    # Download based on scheme
    if scheme in ('http', 'https'):
        _download_http(url, cache_file)
    elif scheme == 's3':
        _download_s3(url, cache_file)
    elif scheme == 'gs':
        _download_gcs(url, cache_file)
    else:
        raise ValueError(f"Unsupported URL scheme: {scheme}")

    return str(cache_file)


def _download_http(url: str, cache_file: Path):
    """Download from HTTP/HTTPS"""
    try:
        import urllib.request
        
        with urllib.request.urlopen(url) as response:
            data = response.read()
            
        # Validate it's valid JSON
        json.loads(data)
        
        with open(cache_file, 'wb') as f:
            f.write(data)
            
    except Exception as e:
        raise RuntimeError(f"Failed to download manifest from {url}: {e}")


def _download_s3(url: str, cache_file: Path):
    """Download from S3"""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        raise ImportError(
            "boto3 is required for S3 support. Install with: pip install boto3"
        )

    parsed = urlparse(url)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, str(cache_file))
        
        # Validate it's valid JSON
        with open(cache_file) as f:
            json.load(f)
            
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to download from S3: {e}")


def _download_gcs(url: str, cache_file: Path):
    """Download from Google Cloud Storage"""
    try:
        from google.cloud import storage
        from google.cloud.exceptions import GoogleCloudError
    except ImportError:
        raise ImportError(
            "google-cloud-storage is required for GCS support. "
            "Install with: pip install google-cloud-storage"
        )

    parsed = urlparse(url)
    bucket_name = parsed.netloc
    blob_name = parsed.path.lstrip('/')

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(str(cache_file))
        
        # Validate it's valid JSON
        with open(cache_file) as f:
            json.load(f)
            
    except GoogleCloudError as e:
        raise RuntimeError(f"Failed to download from GCS: {e}")


def clear_cache(cache_dir: Optional[str] = None):
    """Clear the manifest cache"""
    if cache_dir is None:
        cache_dir = str(Path.home() / ".cache" / "dbt-colibri")
    
    cache_path = Path(cache_dir)
    if cache_path.exists():
        for file in cache_path.glob("colibri-manifest-*.json"):
            file.unlink()


