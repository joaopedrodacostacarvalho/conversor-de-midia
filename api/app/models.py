from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text
from .db import Base


class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    status = Column(String, nullable=False, default="PENDENTE")
    source_key = Column(String, nullable=False)
    source_type = Column(String, nullable=False)
    source_format = Column(String, nullable=False)
    target_format = Column(String, nullable=False)
    notify_email = Column(String, nullable=False)
    converted_url = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    notified_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
