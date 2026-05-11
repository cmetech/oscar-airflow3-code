import logging

logger = logging.getLogger(__name__)

# Storage types for attachments


class AttachmentStorageType:
    INLINE = "inline"
    FILE = "file"
    REDIS = "redis"
