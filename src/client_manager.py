import os
import yaml
from typing import List, Dict, Any, Optional
from telethon import TelegramClient
from src.utils import setup_logger, get_proxy_settings

logger = setup_logger("ClientManager")

class ClientManager:
    """
    Manages the Telethon client lifecycle, configuration, and connection.
    """
    def __init__(self, config_path: str = "config.yaml"):
        self.config: Dict[str, Any] = self._load_config(config_path)
        self.client: Optional[TelegramClient] = None
        
    def _load_config(self, path: str) -> Dict[str, Any]:
        """Loads and parses the YAML configuration file."""
        if not os.path.exists(path):
            logger.error(f"Config file not found: {path}")
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def get_client(self) -> TelegramClient:
        """
        Initializes and returns the Telethon client instance.
        """
        if self.client:
            return self.client

        api_id = self.config.get('api_id')
        api_hash = self.config.get('api_hash')
        session_name = self.config.get('session_name', 'anon_session')
        
        # Helper logging for proxy status
        proxy = get_proxy_settings(self.config)
        if proxy:
            # proxy tuple: (type, host, port, rdns, user, pass)
            logger.info(f"Initializing Telegram Client with proxy: {proxy[1]}:{proxy[2]}")
        else:
            logger.info("Initializing Telegram Client without proxy")

        self.client = TelegramClient(
            session_name, 
            api_id, 
            api_hash,
            proxy=proxy,
            system_version="4.16.30-vxCUSTOM"
        )
        return self.client

    async def start(self) -> TelegramClient:
        """
        Connects to Telegram and authenticates the user (if needed).
        Result: Active client instance.
        """
        client = self.get_client()
        await client.start()
        
        me = await client.get_me()
        logger.info(f"Logged in as: {me.first_name} (@{me.username})")
        return client

    def get_monitored_channels(self) -> List[str]:
        """Returns the list of channel usernames/IDs to monitor."""
        return self.config.get('channels', [])
