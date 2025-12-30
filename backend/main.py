"""Main FastAPI application."""

import logging
import sys
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from backend.config import settings
from backend.api.routes import processing, suggestions, schema
from backend.api.websocket import websocket_endpoint

# Configure logging - force output to stdout with no buffering
# Set Python to unbuffered mode for immediate output
import os
os.environ['PYTHONUNBUFFERED'] = '1'

# Try to set stdout to line buffering
try:
    sys.stdout.reconfigure(line_buffering=True)
except:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True  # Force reconfiguration
)

# Disable uvicorn access logs (we'll use our own)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Set specific loggers to DEBUG for detailed output
logging.getLogger("backend").setLevel(logging.DEBUG)
logging.getLogger("backend.services.suggestion_service").setLevel(logging.DEBUG)
logging.getLogger("backend.api.routes").setLevel(logging.DEBUG)
logging.getLogger("backend.api.routes.suggestions").setLevel(logging.DEBUG)
logging.getLogger("backend.api.routes.processing").setLevel(logging.DEBUG)
logging.getLogger("backend.utils.websocket_manager").setLevel(logging.DEBUG)
logging.getLogger("backend.services.nl2data_service").setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

# Force flush to ensure logs appear immediately
sys.stdout.flush()

logger.info("=" * 80)
logger.info("BACKEND: Starting FastAPI application")
logger.info("=" * 80)
sys.stdout.flush()

# Lifespan context manager for startup/shutdown
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events."""
    # Startup
    print("\n" + "=" * 80, flush=True)
    print("BACKEND STARTUP: Application is ready to receive requests", flush=True)
    print("=" * 80 + "\n", flush=True)
    logger.info("=" * 80)
    logger.info("BACKEND STARTUP: Application is ready to receive requests")
    logger.info("=" * 80)
    sys.stdout.flush()
    yield
    # Shutdown (if needed)
    logger.info("BACKEND: Shutting down")

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    lifespan=lifespan
)

logger.info(f"FastAPI app created: {settings.api_title} v{settings.api_version}")
sys.stdout.flush()

# Request logging middleware
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # CRITICAL: Use multiple methods to ensure output appears
        # Method 1: Direct print to stderr (always shows)
        import sys
        print("\n" + "=" * 80, file=sys.stderr, flush=True)
        print(f"!!! INCOMING REQUEST: {request.method} {request.url.path} !!!", file=sys.stderr, flush=True)
        print(f"  Client: {request.client.host if request.client else 'unknown'}", file=sys.stderr, flush=True)
        
        # Method 2: Print to stdout
        print("\n" + "-" * 80, flush=True)
        print(f"INCOMING REQUEST: {request.method} {request.url.path}", flush=True)
        print(f"  Client: {request.client.host if request.client else 'unknown'}", flush=True)
        if request.query_params:
            print(f"  Query params: {dict(request.query_params)}", flush=True)
        
        # Method 3: Logger
        logger.info("=" * 80)
        logger.info(f"INCOMING REQUEST: {request.method} {request.url.path}")
        logger.info(f"  Client: {request.client.host if request.client else 'unknown'}")
        if request.query_params:
            logger.info(f"  Query params: {dict(request.query_params)}")
        
        # Force flush everything
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Process request
        response = await call_next(request)
        
        # Log response
        process_time = time.time() - start_time
        
        # Method 1: stderr
        print(f"!!! RESPONSE: {request.method} {request.url.path} -> {response.status_code} ({process_time:.3f}s) !!!", file=sys.stderr, flush=True)
        print("=" * 80, file=sys.stderr, flush=True)
        
        # Method 2: stdout
        print(f"RESPONSE: {request.method} {request.url.path} -> {response.status_code} ({process_time:.3f}s)", flush=True)
        print("-" * 80, flush=True)
        
        # Method 3: logger
        logger.info(f"RESPONSE: {request.method} {request.url.path} -> {response.status_code} ({process_time:.3f}s)")
        logger.info("-" * 80)
        
        # Force flush
        sys.stdout.flush()
        sys.stderr.flush()
        
        return response

# Add request logging middleware (before CORS so we see all requests)
app.add_middleware(RequestLoggingMiddleware)

# Verify middleware is registered
logger.info("=" * 80)
logger.info("MIDDLEWARE: RequestLoggingMiddleware registered")
logger.info(f"  Middleware stack: {[m.__class__.__name__ for m in app.user_middleware]}")
logger.info("=" * 80)
sys.stdout.flush()
print("MIDDLEWARE REGISTERED - RequestLoggingMiddleware is active!", file=sys.stderr, flush=True)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(processing.router)
app.include_router(suggestions.router)
app.include_router(schema.router)

# WebSocket
app.websocket("/ws/connect/{job_id}")(websocket_endpoint)


@app.get("/health")
async def health():
    """Health check endpoint."""
    logger.info("=" * 80)
    logger.info("HEALTH CHECK: Endpoint called")
    logger.info("=" * 80)
    sys.stdout.flush()
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    import sys
    
    # Configure uvicorn logging to show our logs
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["default"],
        },
        "loggers": {
            "uvicorn": {"level": "INFO"},
            "uvicorn.error": {"level": "INFO"},
            "uvicorn.access": {"level": "WARNING"},  # We use our own request logging
            "backend": {"level": "DEBUG"},
            "backend.api.routes": {"level": "DEBUG"},
            "backend.services": {"level": "DEBUG"},
        },
    }
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000, 
        workers=1,
        log_config=log_config,
        access_log=False  # Disable uvicorn's access log, we use our own
    )

