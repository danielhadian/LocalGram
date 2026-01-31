import asyncio
import logging
import sys
from src.client_manager import ClientManager
from src.storage_manager import StorageManager
from src.html_builder import HtmlBuilder
from src.archiver import Archiver
from src.utils import setup_logger, validate_config
import yaml

# Setup main logger
logger = setup_logger("Main")

async def main():
    logger.info("Starting LocalGram System...")
    
    # Initialize Components
    try:
        # Validate Config
        with open("config.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        is_valid, error = validate_config(config)
        if not is_valid:
             logger.critical(f"Configuration Invalid: {error}")
             sys.exit(1)

        client_mgr = ClientManager("config.yaml")
        storage_mgr = StorageManager("archive.db")
        html_builder = HtmlBuilder(templates_dir="templates", output_dir=".", storage_manager=storage_mgr)
        
        archiver = Archiver(client_mgr, storage_mgr, html_builder)
        
        # Initial Index Render (in case previous run happened)
        # Initial Index Render (in case previous run happened)
        channels = storage_mgr.get_all_channels()
        html_builder.render_index(channels if channels else [])
            
        from src.server import run_server
        
        # Start Web Server (Sync, Threaded)
        run_server(storage_mgr, html_builder)
        
        # Start Archiver
        await archiver.start()
        
    except FileNotFoundError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


def shutdown_handler(signal_received, frame):
    logger.info(f"Signal {signal_received} received. Shutting down gracefully...")
    # Raise keyboard interrupt to trigger main loop exit
    raise KeyboardInterrupt

if __name__ == '__main__':
    import signal
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("System stopped by user.")
        # Asyncio loop is closed by run(), but to be safe we rely on proper async cleanup within components if needed.
        # Telethon handles its own cleanup if run_until_disconnected returns.
    except Exception as e:
        logger.critical(f"Critical system failure: {e}", exc_info=True)
