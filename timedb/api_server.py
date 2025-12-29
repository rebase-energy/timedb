"""
Simple script to run the FastAPI server.
Usage: python -m timedb.api_server
Or: uvicorn timedb.api:app --host 127.0.0.1 --port 8000
"""
import uvicorn
from .api import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)

