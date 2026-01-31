import os
import asyncio
import logging
from telethon import events
from src.utils import setup_logger

logger = setup_logger("Archiver")

class Archiver:
    def __init__(self, client_manager, storage_manager, html_builder):
        self.client_mgr = client_manager
        self.storage = storage_manager
        self.builder = html_builder
        self.client = None
        self.monitored_channels = {} # username -> channel_entity

    async def start(self):
        """
        Starts the listening loop.
        """
        self.client = await self.client_mgr.start()
        
        # Resolve channels
        await self._resolve_channels()
        
        # Register handlers
        self.client.add_event_handler(self._handle_new_message, events.NewMessage(chats=list(self.monitored_channels.values())))
        
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
                self.monitored_channels[ch_name] = entity
                
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
                # Removed to prevent startup lag
                # try:
                #    all_channels = self.storage.get_all_channels()
                #    if all_channels:
                #        self.builder.render_index(all_channels)
                # except Exception as ex:
                #    logger.error(f"Failed to update index during loop: {ex}")

                # Backfill last 100 messages
            # Backfill last 100 messages
                logger.info(f"Backfilling last 100 messages for {entity.username}...")
                history = await self.client.get_messages(entity, limit=100)
                for msg in history:
                    await self._process_message(msg, entity, render=False)
                
                # Render channel once after backfill
                self._update_channel_html(db_id, entity)
                
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
            msg = event.message
            logger.info(f"New message in {chat.title}: {msg.id}")
            # Always render for live messages
            await self._process_message(msg, chat, render=True)
        except Exception as e:
            logger.error(f"Error handling live message: {e}")

    async def _process_message(self, msg, chat_entity, render=True):
        """
        Common processor for both live and history messages.
        """
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
                        # Proceed to download logic (don't return False yet)
                    else:
                        # Everything exists. Skip.
                        return False
                else:
                    # Message exists and had no media. Skip.
                    return False


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
                     
                     is_video = msg.video is not None
                     is_audio = msg.audio is not None
                     is_voice = msg.voice is not None
                     is_sticker = msg.sticker is not None
                     
                     if not is_video and not is_audio and not is_voice and not is_sticker:
                         should_download = True
                
                if should_download:
                     media_path = await self._download_media(msg, chat_entity.username)
            
            # Save to DB (only if not existing, or update if needed - for now we just handle initial save)
            # If we just re-downloaded media for an existing message, we don't strictly need to update DB 
            # because the path strategy is constant. But for robustness, we try insert.

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
                # If we are here, it means we re-downloaded a missing file.
                # The DB record is fine, we just restored the file.
                saved = True

            if saved and render:
                self._update_channel_html(db_id, chat_entity)
            
            return saved

        except Exception as e:
            logger.error(f"Error processing message {msg.id}: {e}")
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

    def _update_channel_html(self, db_id, chat_entity):
        """
        Triggers HTML rebuild for the channel.
        """
        messages = self.storage.get_messages(db_id, limit=5000) # Get all messages for now
        
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
