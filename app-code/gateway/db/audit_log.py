import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, String, Float, Integer, Text
from gateway.db.session import Base

def _uuid() -> str:
    return str(uuid.uuid4())

def _now() -> datetime:
    return datetime.now(timezone.utc)

class AuditLogORM(Base):
    """
    Persisted audit log for Gateway intents.
    """
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, default=_uuid)
    request_id = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), default=_now, index=True)
    user_id = Column(String, nullable=False, index=True)
    domain = Column(String, nullable=False, index=True)
    action = Column(String, nullable=False)
    parameters = Column(Text, nullable=True)  # JSON string
    source_protocol = Column(String, nullable=False, default="unknown", index=True)
    status_code = Column(Integer, nullable=False)
    duration_ms = Column(Float, nullable=False)
    error_detail = Column(Text, nullable=True)
