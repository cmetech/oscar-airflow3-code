import logging

logger = logging.getLogger(__name__)


def normalize_boolean(value):
    """
    Normalize various boolean-like values to True/False

    Args:
        value: The value to normalize (can be bool, str, int)

    Returns:
        bool: True or False
    """
    logger.info(f"[NORMALIZE_BOOLEAN] Normalizing value: {value} of type {type(value)}")

    if isinstance(value, bool):
        logger.info(f"[NORMALIZE_BOOLEAN] Value is already bool, returning {value}")
        return value

    if isinstance(value, (str, int)):
        # Convert to lowercase string for comparison
        str_value = str(value).lower().strip()
        # List of values that mean True
        true_values = ["true", "1", "yes", "enable", "enabled", "on"]
        result = str_value in true_values
        logger.info(f"[NORMALIZE_BOOLEAN] Converted {value} to {result}")
        return result

    logger.info("[NORMALIZE_BOOLEAN] Value is not bool/str/int, returning False")
    return False
