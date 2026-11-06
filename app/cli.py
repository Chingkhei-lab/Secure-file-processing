"""Admin commands. Run inside the api container:

    docker compose exec api python -m app.cli create-key --name demo
    docker compose exec api python -m app.cli list-keys
    docker compose exec api python -m app.cli revoke-key --prefix fsk_ab12cd34
"""
import argparse
import asyncio

from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal, engine
from app.models import ApiKey
from app.security import (
    KEY_PREFIX_LEN,
    derive_signing_secret,
    generate_api_key,
    hash_api_key,
)


async def create_key(name: str) -> None:
    api_key = generate_api_key()
    async with SessionLocal() as session:
        row = ApiKey(name=name, key_hash=hash_api_key(api_key), key_prefix=api_key[:KEY_PREFIX_LEN])
        session.add(row)
        await session.commit()
        await session.refresh(row)
    secret = derive_signing_secret(get_settings().signing_master_secret, row.id)
    print("Key created. Save these NOW - they cannot be shown again.\n")
    print(f"  name:            {name}")
    print(f"  api key:         {api_key}")
    print(f"  signing secret:  {secret}")


async def list_keys() -> None:
    async with SessionLocal() as session:
        rows = (await session.execute(select(ApiKey).order_by(ApiKey.created_at))).scalars()
        for r in rows:
            state = "active" if r.is_active else "REVOKED"
            print(f"{r.key_prefix}  {state:8}  {r.name}  last used: {r.last_used_at or 'never'}")


async def revoke_key(prefix: str) -> None:
    async with SessionLocal() as session:
        rows = (
            await session.execute(select(ApiKey).where(ApiKey.key_prefix == prefix))
        ).scalars().all()
        if not rows:
            print(f"No key with prefix {prefix}")
            return
        for r in rows:
            r.is_active = False
        await session.commit()
        print(f"Revoked {len(rows)} key(s) with prefix {prefix}")


async def main() -> None:
    parser = argparse.ArgumentParser(prog="app.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("create-key")
    p.add_argument("--name", required=True)
    sub.add_parser("list-keys")
    p = sub.add_parser("revoke-key")
    p.add_argument("--prefix", required=True)
    args = parser.parse_args()

    try:
        if args.cmd == "create-key":
            await create_key(args.name)
        elif args.cmd == "list-keys":
            await list_keys()
        elif args.cmd == "revoke-key":
            await revoke_key(args.prefix)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
