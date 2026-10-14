import os


class Settings:
    DATABASE_URL = os.getenv("DATABASE_URL")
    RABBITMQ_URL = os.getenv("RABBITMQ_URL")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", MINIO_ENDPOINT)
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
    UPLOADS_BUCKET = os.getenv("UPLOADS_BUCKET", "uploads")
    CONVERTED_BUCKET = os.getenv("CONVERTED_BUCKET", "converted")


settings = Settings()
