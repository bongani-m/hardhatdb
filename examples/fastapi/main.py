"""CRUD API for the HardhatDB server.

Start that server first, then:

    pip install -r requirements.txt
    python main.py

    curl -s localhost:8080/people?size=2
    curl -s 'localhost:8080/people?name=Jane'

MYSQL_ADDRS is every MySQL address, comma-separated. Reads and writes use
any of them. A broken connection tries the next address. Leave it unset to
use MYSQL_HOST and MYSQL_PORT (default localhost:3306):

    MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308 python main.py

Another connection can still see an older copy. A write whose connection
breaks before a result comes back is sent to the next address. Connections
use TLS and trust ../../certs/ca.crt. Set MYSQL_TLS_CA to another PEM
file, or to off for a plaintext server.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from store import Conflict, Filter, MySQLStore, NotFound, Person, Store

log = logging.getLogger("example_fastapi")

DEFAULT_PAGE = 1
DEFAULT_SIZE = 2
MAX_PAGE_SIZE = 100
MAX_BODY = 1 << 20
FIELDS = {"name", "email", "phone_numbers", "created_at"}


def create_app(store: Store | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.store is None:
            app.state.store = mysql_from_env()
        yield

    app = FastAPI(title="People", version="1.0.0", lifespan=lifespan)
    app.state.store = store

    @app.get("/people")
    def list_people(request: Request) -> JSONResponse:
        page, size, filt = parse_list_query(request)
        result = store_of(request).list(filt, page, size)
        return JSONResponse(
            {
                "page": page,
                "size": size,
                "total": result.total,
                "people": [person_doc(p) for p in result.people],
            }
        )

    @app.post("/people", status_code=201)
    async def create_person(request: Request) -> JSONResponse:
        body = await read_json(request)
        person = person_from_create(body)
        person = store_of(request).insert(person)
        return JSONResponse(
            person_doc(person),
            status_code=201,
            headers={"Location": f"/people/{person.id}"},
        )

    @app.get("/people/{person_id}")
    def get_person(person_id: str, request: Request) -> JSONResponse:
        return JSONResponse(person_doc(store_of(request).get(parse_id(person_id))))

    @app.put("/people/{person_id}")
    async def update_person(person_id: str, request: Request) -> JSONResponse:
        body = await read_json(request)
        person = person_from_update(parse_id(person_id), body)
        store_of(request).update(person)
        return JSONResponse(person_doc(person))

    @app.delete("/people/{person_id}", status_code=204)
    def delete_person(person_id: str, request: Request) -> Response:
        store_of(request).delete(parse_id(person_id))
        return Response(status_code=204)

    @app.exception_handler(BadRequest)
    def bad_request(_request: Request, exc: BadRequest) -> JSONResponse:
        return JSONResponse({"error": exc.message}, status_code=400)

    @app.exception_handler(NotFound)
    def not_found(_request: Request, _exc: NotFound) -> JSONResponse:
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.exception_handler(Conflict)
    def conflict(_request: Request, _exc: Conflict) -> JSONResponse:
        return JSONResponse({"error": "person already exists"}, status_code=409)

    @app.exception_handler(Exception)
    def internal(request: Request, exc: Exception) -> JSONResponse:
        if isinstance(exc, (BadRequest, NotFound, Conflict)):
            raise exc
        log.exception("store error")
        return JSONResponse({"error": "internal error"}, status_code=500)

    return app


class BadRequest(Exception):
    def __init__(self, message: str):
        self.message = message


def store_of(request: Request) -> Store:
    return request.app.state.store


def person_doc(person: Person) -> dict:
    return {
        "id": person.id,
        "name": person.name,
        "email": person.email,
        "phone_numbers": normalize(person.phone_numbers),
        "created_at": format_time(person.created_at),
    }


def normalize(phones: list[str] | None) -> list[str]:
    return [] if phones is None else phones


def format_time(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond:06d}Z"


def parse_id(value: str) -> int:
    try:
        person_id = int(value)
    except ValueError:
        person_id = 0
    if person_id < 1 or person_id > 2**63 - 1:
        raise BadRequest("id must be a positive integer")
    return person_id


def parse_list_query(request: Request) -> tuple[int, int, Filter]:
    q = request.query_params
    page = DEFAULT_PAGE
    if "page" in q:
        page = positive_int(q["page"], "page must be an integer >= 1")
    size = DEFAULT_SIZE
    if "size" in q:
        size = positive_int(q["size"], f"size must be an integer from 1 to {MAX_PAGE_SIZE}")
        if size > MAX_PAGE_SIZE:
            raise BadRequest(f"size must be an integer from 1 to {MAX_PAGE_SIZE}")
    if page > (2**63 - 1) // size:
        raise BadRequest("page is too large")
    filt = Filter(name=q.get("name", ""), email=q.get("email", ""), phone=q.get("phone", ""))
    if q.get("created_after"):
        filt.created_after = parse_labeled_time("created_after", q["created_after"])
    if q.get("created_before"):
        filt.created_before = parse_labeled_time("created_before", q["created_before"])
    return page, size, filt


def positive_int(value: str, message: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise BadRequest(message) from None
    if number < 1:
        raise BadRequest(message)
    return number


def person_from_create(body: dict) -> Person:
    name, email = required_name_email(body)
    created = datetime.now(timezone.utc)
    if "created_at" in body and body["created_at"] is not None:
        created = parse_labeled_time("created_at", body["created_at"])
    phones = body["phone_numbers"] if "phone_numbers" in body else None
    return Person(
        id=0,
        name=name,
        email=email,
        phone_numbers=normalize(phone_list(phones, required=False)),
        created_at=created,
    )


def person_from_update(person_id: int, body: dict) -> Person:
    name, email = required_name_email(body)
    if "phone_numbers" not in body or body["phone_numbers"] is None:
        raise BadRequest("phone_numbers is required")
    if "created_at" not in body or body["created_at"] is None:
        raise BadRequest("created_at is required")
    return Person(
        id=person_id,
        name=name,
        email=email,
        phone_numbers=phone_list(body["phone_numbers"], required=True),
        created_at=parse_labeled_time("created_at", body["created_at"]),
    )


def required_name_email(body: dict) -> tuple[str, str]:
    name = text_field(body, "name")
    email = text_field(body, "email")
    if not name.strip() or not email.strip():
        raise BadRequest("name and email are required")
    return name.strip(), email.strip()


def text_field(body: dict, field: str) -> str:
    if field not in body or body[field] is None:
        return ""
    value = body[field]
    if not isinstance(value, str):
        raise BadRequest(f"{field} has the wrong type")
    return value


def phone_list(value: object, required: bool) -> list[str]:
    if value is None:
        if required:
            raise BadRequest("phone_numbers is required")
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise BadRequest("phone_numbers has the wrong type")
    return value


def parse_labeled_time(label: str, value: object) -> datetime:
    if not isinstance(value, str):
        raise BadRequest(f"{label} must be RFC3339 or YYYY-MM-DD HH:MM:SS")
    try:
        return parse_time(value)
    except ValueError:
        raise BadRequest(f"{label} must be RFC3339 or YYYY-MM-DD HH:MM:SS") from None


def parse_time(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
        for layout in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(value, layout)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def read_json(request: Request) -> dict:
    content_type = request.headers.get("content-type", "")
    if content_type:
        media = content_type.split(";", 1)[0].strip().lower()
        if media != "application/json":
            raise BadRequest("content type must be application/json")
    raw = await request.body()
    if len(raw) > MAX_BODY:
        raise BadRequest("request body is too large")
    if not raw:
        raise BadRequest("request body is required")
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        raise BadRequest("invalid JSON body") from None
    if not isinstance(body, dict):
        raise BadRequest("invalid JSON body")
    unknown = set(body) - FIELDS
    if unknown:
        name = sorted(unknown)[0]
        raise BadRequest(f"unknown field {name!r}")
    return body


def tls_ca_path() -> str | None:
    """PEM file used to verify the server. MYSQL_TLS_CA=off skips TLS."""
    if "MYSQL_TLS_CA" in os.environ:
        raw = os.environ["MYSQL_TLS_CA"]
        if raw in ("", "off"):
            return None
        return raw
    return str(Path(__file__).resolve().parent.parent.parent / "certs" / "ca.crt")


def mysql_addrs() -> list[str]:
    """Every node. MYSQL_ADDRS overrides MYSQL_HOST and MYSQL_PORT."""
    addrs = split_addrs(os.environ.get("MYSQL_ADDRS", ""))
    if addrs:
        return addrs
    return [f"{env('MYSQL_HOST', 'localhost')}:{env('MYSQL_PORT', '3306')}"]


def mysql_from_env() -> MySQLStore:
    addrs = mysql_addrs()
    ca = tls_ca_path()
    if ca:
        log.info("MySQL TLS CA %s", ca)
    store = MySQLStore(
        addrs,
        user=env("MYSQL_USER", "root"),
        password=env("MYSQL_PASSWORD", "dev-only-change-me"),
        database=env("MYSQL_DB", "mydb"),
        ssl_ca=ca,
    )
    log.info("MySQL %s", ", ".join(addrs))
    return store


def split_addrs(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def env(key: str, fallback: str) -> str:
    return os.environ.get(key) or fallback


def listen_addr() -> tuple[str, int]:
    raw = env("HTTP_ADDR", ":8080")
    if raw.startswith(":"):
        return "0.0.0.0", int(raw[1:])
    host, port = raw.rsplit(":", 1)
    return host or "0.0.0.0", int(port)


app = create_app()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    host, port = listen_addr()
    log.info("API listening on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
