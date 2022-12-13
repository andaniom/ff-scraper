from http.server import BaseHTTPRequestHandler

from scraper import Scraper


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        Scraper().main()
        return