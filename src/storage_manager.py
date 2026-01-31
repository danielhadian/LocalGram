import sqlite3
import os
import logging
from datetime import datetime

logger = logging.getLogger("StorageManager")

class StorageManager:
    def __init__(self, db_path="archive.db"):
        self.db_path = db_path
        self._configure_db()
        self._init_db()

    def _configure_db(self):
        """Enable WAL mode for better concurrency."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error as e:
            logger.warning(f"Failed to enable WAL mode: {e}")

    def _init_db(self):
        """Initialize the database schema."""
        create_channels_table = """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            username TEXT,
            folder_path TEXT,
            avatar_path TEXT
        );
        """
        create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            telegram_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            message_text TEXT,
            media_path TEXT,
            grouped_id INTEGER,
            FOREIGN KEY (channel_id) REFERENCES channels (id),
            UNIQUE(channel_id, telegram_id)
        );
        """
        
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute(create_channels_table)
                cursor.execute(create_messages_table)
                conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")

    def get_or_create_channel(self, telegram_id, title, username, folder_path, avatar_path=None):
        """
        Retrieves a channel ID or creates a new entry.
        Returns the internal database ID (not telegram ID).
        """
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM channels WHERE telegram_id = ?", (telegram_id,))
                row = cursor.fetchone()
                
                if row:
                    return row[0]
                
                cursor.execute(
                    "INSERT INTO channels (telegram_id, title, username, folder_path, avatar_path) VALUES (?, ?, ?, ?, ?)",
                    (telegram_id, title, username, folder_path, avatar_path)
                )
                conn.commit()
                return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error managing channel {title}: {e}")
            return None


    def get_channel_by_id(self, db_id):
        """Retrieves a single channel by DB ID."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM channels WHERE id = ?", (db_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Error fetching channel {db_id}: {e}")
            return None

    def save_message(self, channel_db_id, telegram_message_id, date, text, media_path=None, grouped_id=None):
        """
        Saves a message to the archive. Returns True if saved, False if duplicate.
        """
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                # Check for duplicate
                cursor.execute(
                    "SELECT id FROM messages WHERE channel_id = ? AND telegram_id = ?",
                    (channel_db_id, telegram_message_id)
                )
                if cursor.fetchone():
                    return False

                cursor.execute(
                    """INSERT INTO messages 
                    (channel_id, telegram_id, date, message_text, media_path, grouped_id)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (channel_db_id, telegram_message_id, date.isoformat(), text, media_path, grouped_id)
                )
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Error saving message {telegram_message_id}: {e}")
            return False

    def get_messages(self, channel_db_id, limit=100, offset=0):
        """Retrieves messages for a channel, ordered by date."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT * FROM messages 
                       WHERE channel_id = ? 
                       ORDER BY telegram_id ASC
                       LIMIT ? OFFSET ?""",
                    (channel_db_id, limit, offset)
                )
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error fetching messages: {e}")
            return []

    def get_all_channels(self):
        """Retrieves all channels from the database."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM channels")
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error fetching channels: {e}")
            return []

    def get_message(self, channel_db_id, telegram_id):
        """Retrieves a single message by channel and telegram ID."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM messages WHERE channel_id = ? AND telegram_id = ?",
                    (channel_db_id, telegram_id)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Error fetching message: {e}")
            return None

    def message_exists(self, channel_db_id, telegram_id):
        """Checks if a message already exists in the database."""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM messages WHERE channel_id = ? AND telegram_id = ?",
                    (channel_db_id, telegram_id)
                )
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking message existence: {e}")
            return False

    def clear_all_data(self):
        """
        DANGEROUS: Wipes all data from database and deletes downloaded files.
        Returns True if successful.
        """
        try:
            # 1. Clear Database
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM messages")
                cursor.execute("DELETE FROM channels")
                cursor.execute("DELETE FROM sqlite_sequence") # Reset autoincrement
                conn.commit()
                cursor.execute("VACUUM") # Reclaim space
            
            # 2. Delete File Content
            import shutil
            
            # Clear downloads
            if os.path.exists("downloads"):
                shutil.rmtree("downloads")
                os.makedirs("downloads")
                
            # Clear generated channels html
            if os.path.exists("channels"):
                shutil.rmtree("channels")
                os.makedirs("channels")
            
            logger.warning("SYSTEM RESET: All data cleared by user.")
            return True
        except Exception as e:
            logger.error(f"Failed to clear data: {e}")
            return False
