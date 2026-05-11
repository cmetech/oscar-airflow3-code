import re
import logging
from typing import Optional, Dict, List, Tuple

logger = logging.getLogger(__name__)


def extract_ip_address(text: str) -> Optional[str]:
    """
    Extracts the first valid IPv4 address found in the input string.
    
    Args:
        text (str): The string to search.
    
    Returns:
        Optional[str]: The first IP address found, or None if no IP is present.
    """
    if not text:
        return None
        
    # IPv4 pattern with proper validation (0-255 for each octet)
    ipv4_pattern = r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'
    
    match = re.search(ipv4_pattern, text)
    if match:
        ip = match.group(0)
        logger.debug(f"[EMAIL_EXTRACTOR] Found IPv4 address: {ip}")
        return ip
    
    return None


def extract_ipv6_address(text: str) -> Optional[str]:
    """
    Extracts the first valid IPv6 address found in the input string.
    
    Args:
        text (str): The string to search.
    
    Returns:
        Optional[str]: The first IPv6 address found, or None if no IPv6 is present.
    """
    if not text:
        return None
        
    # Comprehensive IPv6 pattern (handles all valid formats including compressed)
    ipv6_pattern = (
        r'\b(?:'
        r'(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|'  # Full IPv6
        r'(?:[0-9a-fA-F]{1,4}:){1,7}:|'  # Compressed with leading ::
        r'(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|'  # Compressed
        r'(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|'  # Compressed
        r'(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|'  # Compressed
        r'(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|'  # Compressed
        r'(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|'  # Compressed
        r'[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}|'  # Compressed
        r':(?::[0-9a-fA-F]{1,4}){1,7}|'  # Compressed with only ::
        r'::(?:ffff:)?(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'  # IPv4-mapped
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
        r')\b'
    )
    
    match = re.search(ipv6_pattern, text, re.IGNORECASE)
    if match:
        ip = match.group(0)
        logger.debug(f"[EMAIL_EXTRACTOR] Found IPv6 address: {ip}")
        return ip
    
    return None


def extract_hostname(text: str) -> Optional[str]:
    """
    Extracts the first valid hostname/FQDN found in the input string.
    
    Args:
        text (str): The string to search.
    
    Returns:
        Optional[str]: The first hostname found, or None if no hostname is present.
    """
    if not text:
        return None
        
    # Hostname/FQDN pattern
    # Must start with alphanumeric, can contain hyphens but not at the end of segments
    # Must have at least one dot and a valid TLD (2+ letters)
    hostname_pattern = r'\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}\b'
    
    # Common file extensions and URL schemes to exclude
    exclusions = [
        r'\.(?:jpg|jpeg|png|gif|pdf|doc|docx|txt|csv|log|zip|tar|gz)$',
        r'^(?:http|https|ftp|mailto|file)://',
        r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'  # Email addresses
    ]
    
    matches = re.finditer(hostname_pattern, text)
    
    for match in matches:
        hostname = match.group(0)
        
        # Skip if it looks like a file extension or URL
        skip = False
        for exclusion in exclusions:
            if re.search(exclusion, hostname, re.IGNORECASE):
                skip = True
                break
        
        if not skip:
            # Additional validation: ensure it's not just numbers with dots (like version numbers)
            if not re.match(r'^[\d.]+$', hostname):
                logger.debug(f"[EMAIL_EXTRACTOR] Found hostname: {hostname}")
                return hostname
    
    return None


def extract_ip_or_hostname(text: str) -> Dict[str, Optional[str]]:
    """
    Extracts both IP addresses and hostnames from the input string.
    Returns the first of each type found.
    
    Args:
        text (str): The string to search.
    
    Returns:
        Dict[str, Optional[str]]: Dictionary with 'ip_address' and 'hostname' keys.
    """
    if not text:
        return {"ip_address": None, "hostname": None}
    
    # Try IPv4 first, then IPv6
    ipv4 = extract_ip_address(text)
    ipv6 = extract_ipv6_address(text) if not ipv4 else None
    hostname = extract_hostname(text)
    
    return {
        "ip_address": ipv4 or ipv6,
        "hostname": hostname
    }


def extract_all_ips_and_hostnames(text: str) -> Dict[str, List[str]]:
    """
    Extracts all IP addresses and hostnames from the input string.
    Useful for debugging or when you need all matches.
    
    Args:
        text (str): The string to search.
    
    Returns:
        Dict[str, List[str]]: Dictionary with lists of all found IPs and hostnames.
    """
    if not text:
        return {"ip_addresses": [], "hostnames": []}
    
    # Find all IPv4 addresses
    ipv4_pattern = r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'
    ipv4_addresses = re.findall(ipv4_pattern, text)
    
    # Find all IPv6 addresses
    ipv6_pattern = (
        r'\b(?:'
        r'(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|'
        r'(?:[0-9a-fA-F]{1,4}:){1,7}:|'
        r'(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|'
        r'(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|'
        r'(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|'
        r'(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|'
        r'(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|'
        r'[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}|'
        r':(?::[0-9a-fA-F]{1,4}){1,7}|'
        r'::(?:ffff:)?(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
        r')\b'
    )
    ipv6_addresses = re.findall(ipv6_pattern, text, re.IGNORECASE)
    
    # Find all hostnames
    hostname_pattern = r'\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}\b'
    potential_hostnames = re.findall(hostname_pattern, text)
    
    # Filter out false positives from hostnames
    exclusions = [
        r'\.(?:jpg|jpeg|png|gif|pdf|doc|docx|txt|csv|log|zip|tar|gz)$',
        r'^(?:http|https|ftp|mailto|file)://',
        r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    ]
    
    hostnames = []
    for hostname in potential_hostnames:
        skip = False
        for exclusion in exclusions:
            if re.search(exclusion, hostname, re.IGNORECASE):
                skip = True
                break
        
        if not skip and not re.match(r'^[\d.]+$', hostname):
            hostnames.append(hostname)
    
    # Remove duplicates while preserving order
    ipv4_addresses = list(dict.fromkeys(ipv4_addresses))
    ipv6_addresses = list(dict.fromkeys(ipv6_addresses))
    hostnames = list(dict.fromkeys(hostnames))
    
    all_ips = ipv4_addresses + ipv6_addresses
    
    return {
        "ip_addresses": all_ips,
        "hostnames": hostnames
    }


def validate_ip_address(ip: str) -> Tuple[bool, Optional[str]]:
    """
    Validates if a string is a valid IP address and returns its type.
    
    Args:
        ip (str): The IP address to validate.
    
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, ip_type) where ip_type is 'ipv4' or 'ipv6'
    """
    if not ip:
        return False, None
    
    # Check IPv4
    ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
    if re.match(ipv4_pattern, ip):
        return True, 'ipv4'
    
    # Check IPv6
    try:
        # Simple validation by checking if colons are present and hex chars
        if ':' in ip:
            # Remove IPv4 suffix if present
            ipv6_part = ip.split(':')
            # Basic hex validation
            for part in ipv6_part:
                if part and not all(c in '0123456789abcdefABCDEF.' for c in part):
                    return False, None
            return True, 'ipv6'
    except:
        pass
    
    return False, None


def validate_hostname(hostname: str) -> bool:
    """
    Validates if a string is a valid hostname according to RFC standards.
    
    Args:
        hostname (str): The hostname to validate.
    
    Returns:
        bool: True if valid hostname, False otherwise.
    """
    if not hostname or len(hostname) > 253:
        return False
    
    # Remove trailing dot if present
    if hostname.endswith('.'):
        hostname = hostname[:-1]
    
    # Check each label
    labels = hostname.split('.')
    
    # Must have at least 2 labels (e.g., example.com)
    if len(labels) < 2:
        return False
    
    for label in labels:
        # Check label length
        if not label or len(label) > 63:
            return False
        
        # Must start with alphanumeric
        if not label[0].isalnum():
            return False
        
        # Must end with alphanumeric
        if not label[-1].isalnum():
            return False
        
        # Can only contain alphanumeric and hyphens
        if not all(c.isalnum() or c == '-' for c in label):
            return False
    
    # Last label (TLD) must be at least 2 chars and alphabetic
    if len(labels[-1]) < 2 or not labels[-1].isalpha():
        return False
    
    return True