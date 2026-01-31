import os
import asyncio
import logging
from telethon import events
from src.utils import setup_logger

logger = setup_logger("Archiver")

from collections import defaultdict

class Archiver:
    def __init__(self, client_manager, storage_manager, html_builder):
        self.client_mgr = client_manager
        self.storage = storage_manager
        self.builder = html_builder
        self.client = None
        self.monitored_channels = {} # username -> channel_entity
        self.render_locks = defaultdict(asyncio.Lock) # db_id -> Lock

    async def start(self):
        """
        Starts the listening loop.
        """
        self.client = await self.client_mgr.start()
        
        # Resolve channels
        await self._resolve_channels()
        
        # Register global handler (more robust) and filter manually
        self.client.add_event_handler(self._handle_new_message, events.NewMessage())
        
        logger.info("Archiver started. Listening for new messages...")
        
        # Robust disconnection handling
        while True:
            try:
                await self.client.run_until_disconnected()
                break # Clean exit
            except Exception as e:
                logger.error(f"Connection lost: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                # Client usually survives simple disconnects, but if we need to restart:
                if not self.client.is_connected():
                     await self.client.connect()

    async def _resolve_channels(self):
        """
        Resolves configured channel usernames to entities.
        """
        config_channels = self.client_mgr.get_monitored_channels()
        for ch_name in config_channels:
            try:
                entity = await self.client.get_entity(ch_name)
                # Store by ID for robust lookup
                self.monitored_channels[entity.id] = entity
                
                # Register in DB
                avatar_path = None
                try:
                     # Download profile photo
                     photo_path = f"downloads/{entity.username}/profile.jpg"
                     os.makedirs(os.path.dirname(photo_path), exist_ok=True)
                     avatar_path = await self.client.download_profile_photo(entity, file=photo_path)
                except Exception as ex:
                    logger.error(f"Failed to download avatar for {ch_name}: {ex}")

                download_path = self.client_mgr.config.get('download_path', 'downloads')
                
                db_id = self.storage.get_or_create_channel(
                    entity.id, 
                    entity.title, 
                    entity.username, 
                    f"{download_path}/{entity.username}",
                    avatar_path
                )
                
                logger.info(f"Monitoring channel: {entity.title} (@{entity.username})")
                
                
                # Render initial HTML (Empty or existing) immediately
                # self._update_channel_html(db_id, entity) -> Moved to after backfill
                
                # Update index immediately so user sees the channel appears
                try:
                   all_channels = self.storage.get_all_channels()
                   if all_channels:
                       self.builder.render_index(all_channels)
                except Exception as ex:
                   logger.error(f"Failed to update index during loop: {ex}")

                # Backfill last 100 messages - DISABLED by user request for pure real-time monitoring
                # logger.info(f"Backfilling last 100 messages for {entity.username}...")
                # history = await self.client.get_messages(entity, limit=100)
                # for msg in history:
                #     await self._process_message(msg, entity, render=False)
                
                # Render channel once after resolving (since backfill is disabled)
                await self._update_channel_html(db_id, entity)
                
            except Exception as e:
                logger.error(f"Failed to resolve or backfill channel {ch_name}: {e}")
        
        # Generate index.html ONCE after resolving all channels
        try:
            all_channels = self.storage.get_all_channels()
            if all_channels:
                self.builder.render_index(all_channels)
        except Exception as e:
            logger.error(f"Failed to render initial index: {e}")

    async def _handle_new_message(self, event):
        """
        Callback for new messages.
        """
        try:
            chat = await event.get_chat()
            if not chat or chat.id not in self.monitored_channels:
                # DEBUG: Uncomment to see ALL messages even from unmonitored chats
                # logger.debug(f"Ignored message from {chat.title if chat else 'Unknown'}")
                return

            msg = event.message
            logger.info(f"New message in {chat.title}: {msg.id}")
            # FIRE-AND-FORGET: Do not await! Schedule it and return immediately to receive the next packet.
            asyncio.create_task(self._process_message(msg, chat, render=True))
        except Exception as e:
            logger.error(f"Error handling live message: {e}")

    async def _process_message(self, msg, chat_entity, render=True):
        """
        Common processor with RETRY LOGIC for robustness (Zero-Data-Loss).
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                db_id = self.storage.get_or_create_channel(chat_entity.id, chat_entity.title, chat_entity.username, "", None)
                
                # Logic Check: Does message exist?
                existing_msg = self.storage.get_message(db_id, msg.id)
                if existing_msg:
                    # If exists, check if media is missing from disk
                    media_path = existing_msg.get('media_path')
                    if media_path:
                        # media_path is relative, e.g. downloads/username/file
                        if not os.path.exists(media_path):
                            logger.warning(f"Media missing for msg {msg.id}, re-downloading...")
                            # Proceed to download logic
                        else:
                            return False # Everything exists
                    else:
                        return False # Exists, no media
    
    
                # Download Media
                media_path = None
                if msg.media:
                    # Filter media types based on config
                    allowed_types = self.client_mgr.config.get('media_types', [])
                    
                    should_download = False
                    
                    # Check specific types
                    if 'photo' in allowed_types and msg.photo:
                        should_download = True
                    elif 'video' in allowed_types and msg.video:
                        should_download = True
                    elif 'document' in allowed_types and msg.document:
                         should_download = True
                    
                    if should_download:
                         media_path = await self._download_media(msg, chat_entity.username)
                
                # Save to DB
                if not existing_msg:
                    saved = self.storage.save_message(
                        db_id, 
                        msg.id, 
                        msg.date, 
                        msg.text, 
                        media_path, 
                        msg.grouped_id
                    )
                else:
                    saved = True
    
                if saved and render:
                    # Lock guarded render
                    async with self.render_locks[db_id]:
                         await self._update_channel_html(db_id, chat_entity)
                
                return saved

            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{max_retries} failed for msg {msg.id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1)) # Exponential backoff: 2s, 4s, 6s
                else:
                    logger.critical(f"Permanently failed to process message {msg.id} after retries.")
                    return False

    async def _download_media(self, message, username):
        """
        Downloads media to downloads/{username}/{date_id_filename}
        """
        try:
            date_str = message.date.strftime("%Y%m%d")
            path = f"downloads/{username}"
            os.makedirs(path, exist_ok=True)
            
            # Telethon automatically handles file extension
            filename = f"{date_str}_{message.id}"
            
            final_path = await message.download_media(file=f"{path}/{filename}")
            if final_path:
                logger.info(f"Downloaded media: {final_path}")
                # Make path relative for HTML
                return os.path.relpath(final_path, start=".")
            return None
        except Exception as e:
            logger.error(f"Media download failed: {e}")
            return None

    async def _update_channel_html(self, db_id, chat_entity):
        """
        Async wrapper that runs the heavy sync rendering in a separate thread.
        This prevents the asyncio loop from blocking, allowing concurrent message handling.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._render_sync, db_id, chat_entity)
        
    def _render_sync(self, db_id, chat_entity):
        """
        Heavy blocking CPU/IO task: Fetch from DB and Render Template.
        """
        try:
            messages = self.storage.get_messages(db_id, limit=5000) # Get all messages
            
            channel_row = self.storage.get_channel_by_id(db_id)
            if not channel_row:
                 channel_data = {
                    'title': chat_entity.title,
                    'username': chat_entity.username,
                    'avatar_path': None
                 }
            else:
                 channel_data = {
                    'title': channel_row['title'],
                    'username': channel_row['username'],
                    'avatar_path': channel_row['avatar_path']
                 }
            
            self.builder.render_channel(channel_data, messages)
        except Exception as e:
            logger.error(f"Failed to render channel {chat_entity.username}: {e}")
