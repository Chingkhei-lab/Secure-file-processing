import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Response

from app import storage
from app.auth import require_auth
from app.config import get_settings
from app.db import engine
from app.health import run_all_checks
from app.jobs import close_queue
from app.models import ApiKey
from app.redis_client import close_redis
from app.routers import uploads

settings = get_settings()
logging.basicConfig(level=settings.log_level)
log = logging.getLogger("fileservice")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.to_thread(storage.ensure_bucket)
    except Exception as exc:  # noqa: BLE001
        # Don't crash on startup; /health will report the problem.
        log.warning("Could not ensure S3 bucket: %s", exc)
    yield
    await close_queue()
    await close_redis()
    await engine.dispose()


app = FastAPI(title="Secure File Processing Service", version="0.1.0", lifespan=lifespan)
app.include_router(uploads.router)


@app.get("/health", tags=["ops"])
async def health(response: Response):
    checks = await run_all_checks(engine, settings)
    healthy = all(v == "ok" for v in checks.values())
    if not healthy:
        response.status_code = 503
    return {"status": "ok" if healthy else "degraded", "checks": checks}


@app.get("/v1/whoami", tags=["auth"])
async def whoami(key: ApiKey = Depends(require_auth)):
    """Authenticated test route: proves your key and signature work."""
    return {"api_key_id": str(key.id), "name": key.name, "prefix": key.key_prefix}
