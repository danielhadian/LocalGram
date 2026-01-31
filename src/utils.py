import logging
import sys
import os
from typing import Optional, Tuple, Any, Dict

def setup_logger(name: str = "LocalGram", log_file: str = "localgram.log", level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger that outputs to console and file.
    
    Args:
        name: Name of the logger
        log_file: Path to the log file
        level: Logging level (default: logging.INFO)
    
    Returns:
        Configured logging.Logger instance
    """
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setFormatter(formatter)

    # Ensure log directory exists if path has one
    if os.path.dirname(log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    handler_file = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    handler_file.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers if setup is called multiple times
    if not logger.handlers:
        logger.addHandler(handler_console)
        logger.addHandler(handler_file)

    return logger

def get_proxy_settings(config: Dict[str, Any]) -> Optional[Tuple[int, str, int, bool, str, str]]:
    """
    Returns proxy settings tuple for Telethon based on config.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Tuple compatible with Telethon proxy argument, or None
    """
    import socks
    
    proxy_conf = config.get('proxy', {})
    if not proxy_conf.get('enabled', False):
        return None
        
    host = proxy_conf.get('host')
    port = proxy_conf.get('port')
    
    if not host or not port:
        return None

    username = proxy_conf.get('username')
    password = proxy_conf.get('password')
    
    proxy_type = socks.SOCKS5
    if proxy_conf.get('type', 'SOCKS5').upper() == 'HTTP':
        proxy_type = socks.HTTP
        
    return (proxy_type, host, port, True, username, password)


def validate_config(config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validates the configuration dictionary.
    
    Args:
        config: Configuration dictionary loaded from YAML
        
    Returns:
        Tuple (is_valid, error_message)
    """
    required_keys = ['api_id', 'api_hash']
    for key in required_keys:
        if key not in config:
            return False, f"Missing required config key: {key}"
            
    if not isinstance(config.get('channels'), list):
         return False, "Config 'channels' must be a list"
         
    return True, None
