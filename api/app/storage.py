from minio import Minio
from .config import settings

_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE,
)


def upload_stream(
    bucket: str, key: str, stream, length: int, content_type: str
) -> None:
    _client.put_object(bucket, key, stream, length, content_type=content_type)
