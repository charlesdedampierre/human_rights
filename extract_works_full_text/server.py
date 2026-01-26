#!/usr/bin/env python3
"""Simple HTTP server that serves files and saves feedback."""

import http.server
import json
from pathlib import Path
from datetime import datetime

PORT = 8080
FEEDBACK_FILE = Path(__file__).parent / "feedback.json"


class FeedbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/save-feedback":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)

            try:
                feedback = json.loads(post_data.decode("utf-8"))
                feedback["saved_at"] = datetime.now().isoformat()

                with open(FEEDBACK_FILE, "w", encoding="utf-8") as f:
                    json.dump(feedback, f, indent=2, ensure_ascii=False)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "saved", "file": str(FEEDBACK_FILE)}).encode())
                print(f"\n✓ Feedback saved to {FEEDBACK_FILE}")
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


if __name__ == "__main__":
    print(f"Starting server at http://localhost:{PORT}")
    print(f"Feedback will be saved to: {FEEDBACK_FILE}")
    print("Press Ctrl+C to stop\n")

    with http.server.HTTPServer(("", PORT), FeedbackHandler) as httpd:
        httpd.serve_forever()
