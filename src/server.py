import logging
import os
import json
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from functools import partial

logger = logging.getLogger("WebServer")

class ArchiverHandler(SimpleHTTPRequestHandler):
    def __init__(self, storage_manager, html_builder, *args, **kwargs):
        self.storage = storage_manager
        self.builder = html_builder
        # Directory to serve static files from (current dir)
        super().__init__(*args, directory=".", **kwargs)

    def do_POST(self):
        if self.path == '/api/clear_data':
            try:
                logger.info("Received Clear Data request")
                success = self.storage.clear_all_data()
                
                if success:
                    # Reset index.html
                    self.builder.render_index([])
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'status': 'success', 'message': 'Data cleared'}).encode())
                else:
                    self.send_error(500, "Failed to clear data")
            except Exception as e:
                logger.error(f"API Error: {e}")
                self.send_error(500, str(e))
        else:
            self.send_error(404, "Page Not Found", "The requested resource does not exist.")

    def log_message(self, format, *args):
        # Override to use our logger instead of stderr
        logger.info("%s - - [%s] %s" %
                     (self.client_address[0],
                      self.log_date_time_string(),
                      format%args))

def run_server(storage_manager, html_builder, host='0.0.0.0', port=8080):
    """
    Starts the HTTP server in a separate thread.
    Non-blocking.
    """
    try:
        # Create a partial to pass our custom arguments to the handler
        handler = partial(ArchiverHandler, storage_manager, html_builder)
        
        if host == '0.0.0.0': host = '' # HTTPServer quirks
        server = HTTPServer((host, port), handler)
        server.allow_reuse_address = True
        
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True # Daemon thread exits when main program exits
        thread.start()
        
        logger.info(f"Web Server running at http://{host}:{port} (Threaded)")
        return server
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
        return None
