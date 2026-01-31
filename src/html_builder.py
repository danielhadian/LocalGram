import os
import logging
from jinja2 import Environment, FileSystemLoader
from typing import List, Dict, Any, Optional

logger = logging.getLogger("HtmlBuilder")

class HtmlBuilder:
    """
    Handles generation of HTML pages using Jinja2 templates.
    """
    def __init__(self, templates_dir: str = "templates", output_dir: str = ".", storage_manager: Any = None):
        self.env = Environment(loader=FileSystemLoader(templates_dir))
        self.output_dir = output_dir
        self.storage = storage_manager
        
    def render_index(self, channels: List[Dict[str, Any]]) -> None:
        """
        Renders the main index page with the list of channels.
        
        Args:
            channels: List of channel dictionaries (from DB)
        """
        try:
            template = self.env.get_template("index.html")
            html = template.render(channels=channels)
            
            output_path = os.path.join(self.output_dir, "index.html")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info("Updated index.html")
        except Exception as e:
            logger.error(f"Failed to render index: {e}")

    def render_channel(self, channel_data: Dict[str, Any], messages: List[Dict[str, Any]]) -> None:
        """
        Renders a specific channel's page in channels/ directory.
        
        Args:
            channel_data: Dictionary of channel info
            messages: List of message dictionaries
        """
        try:
            # Create channels dir if not exists (redundant safety)
            channels_dir = os.path.join(self.output_dir, "channels")
            os.makedirs(channels_dir, exist_ok=True)

            # Fix media paths for subdirectory context
            view_messages = []
            for msg in messages:
                msg_copy = dict(msg) # msg is dict from Row
                if msg_copy.get('media_path'):
                     # Prepend ../ to the existing path
                     msg_copy['media_path'] = "../" + msg_copy['media_path']
                view_messages.append(msg_copy)

            # Also fix channel avatar path if exists
            view_channel = dict(channel_data)
            if view_channel.get('avatar_path'):
                view_channel['avatar_path'] = "../" + view_channel['avatar_path']

            template = self.env.get_template("channel.html")
            html = template.render(channel=view_channel, messages=view_messages)
            
            output_path = os.path.join(channels_dir, f"{channel_data['username']}.html")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"Updated channels/{channel_data['username']}.html")
        except Exception as e:
            logger.error(f"Failed to render channel page for {channel_data.get('username')}: {e}")

    def update_channel(self, channel_db_id):
        """
        Fetches latest data from DB and re-renders the channel page.
        This is a simple implementation; for huge channels, incremental append is better.
        But for a personal archive, re-rendering 10k messages is fast enough on modern CPUs.
        """
        if not self.storage:
            logger.warning("Storage manager not provided to HtmlBuilder, cannot auto-update channel.")
            return

        # Fetch channel info
        pass
