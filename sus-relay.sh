#!/usr/bin/env bash

START_PORT=${1:-7429}
PORT=$START_PORT

if ! command -v python3 &>/dev/null; then
  echo "Error: python3 not found. Install via: brew install python"
  exit 1
fi

while [[ $PORT -le $((START_PORT + 10)) ]]; do
  if ! lsof -iTCP:$PORT -sTCP:LISTEN &>/dev/null 2>&1; then
    break
  fi
  echo "Port $PORT in use, trying $((PORT + 1))..."
  PORT=$((PORT + 1))
done

if [[ $PORT -gt $((START_PORT + 10)) ]]; then
  echo "Error: ports $START_PORT-$((START_PORT + 10)) all in use. Run: bash sus-relay.sh <port>"
  exit 1
fi

echo ""
echo "Sus Relay is up on http://localhost:$PORT"
echo "Forwarding to https://api.newrelic.com/graphql"
echo "Press Ctrl+C to stop."
echo ""

python3 - $PORT << 'EOF'
import sys, json, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1])

class SusRelay(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  {self.command} -> {args[1] if len(args) > 1 else ''}")

    def do_OPTIONS(self):
        self._send(204)

    def do_POST(self):
        body    = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        api_key = self.headers.get("Api-Key", "")
        req = urllib.request.Request(
            "https://api.newrelic.com/graphql",
            data=body,
            headers={"Content-Type": "application/json", "Api-Key": api_key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                self._send(r.status, r.read())
        except urllib.error.HTTPError as e:
            self._send(e.code, e.read())
        except Exception as e:
            self._send(502, json.dumps({"errors": [{"message": str(e)}]}).encode())

    def _send(self, status, body=b""):
        self.send_response(status)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Api-Key")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

HTTPServer(("127.0.0.1", PORT), SusRelay).serve_forever()
EOF
