"""Accounts API for the HardhatDB server.

Start that server first, then:

    pip install -r requirements.txt
    python main.py

    curl -s localhost:8000/accounts?size=2
    curl -s localhost:8000/cluster

MYSQL_ADDRS is every MySQL address, comma-separated. One request stays on
one node. A broken connection tries the next address. Leave it unset to
use MYSQL_HOST and MYSQL_PORT (default localhost:3306):

    MYSQL_ADDRS=127.0.0.1:3306,127.0.0.1:3307,127.0.0.1:3308 python main.py

Creating an account writes the account and its first note in one transaction,
then reads that row back on the same connection. Another connection can still
see an older copy. A write whose connection breaks before a result comes back
is sent to the next address. Connections use TLS and trust ../certs/ca.crt.
Set MYSQL_TLS_CA to another PEM file, or to off for a plaintext server.
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
from pydantic import BaseModel, ConfigDict, Field

from store import (
    Account,
    Conflict,
    Filter,
    InsertResult,
    MySQLStore,
    NodeStatus,
    NotFound,
    Note,
    Store,
)

log = logging.getLogger("example_fastapi")

DEFAULT_PAGE = 1
DEFAULT_SIZE = 20
MAX_PAGE_SIZE = 100
MAX_BODY = 1 << 20
FIELDS = {"id", "name", "email", "status", "tags", "note", "created_at", "node"}


class NoteDoc(BaseModel):
    id: int
    body: str
    created_at: str


class AccountDoc(BaseModel):
    id: int
    name: str
    email: str
    status: int
    tags: list[str]
    created_at: str
    notes: list[NoteDoc]


class AccountListDoc(BaseModel):
    page: int
    size: int
    total: int
    accounts: list[AccountDoc]


class OtherNodeDoc(BaseModel):
    addr: str
    account: AccountDoc | None


class InsertDoc(BaseModel):
    account: AccountDoc
    wrote_on: str
    read_back: AccountDoc | None
    other_node: OtherNodeDoc


class NodeDoc(BaseModel):
    addr: str
    role: str
    leader: str
    commit_index: str
    applied_index: str
    lag: str
    suffrage: str
    error: str


class ClusterDoc(BaseModel):
    nodes: list[NodeDoc]


class ErrorDoc(BaseModel):
    error: str


class CreateBody(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "name": "Lin",
                "email": "lin@example.com",
                "status": 1,
                "tags": ["demo"],
                "note": "hello",
            }
        },
    )
    name: str = Field(description="Required.")
    email: str = Field(description="Required.")
    status: int | None = Field(default=None, description="Integer from 0 to 127. Defaults to 1.")
    tags: list[str] | None = Field(default=None, description="Omitted or null becomes an empty list.")
    note: str = Field(description="First note. Required.")
    created_at: str | None = Field(default=None, description="RFC3339 or YYYY-MM-DD HH:MM:SS. Defaults to now.")
    node: str | None = Field(default=None, description="MySQL address to write through. Empty uses the first address.")
    id: int | None = Field(default=None, description="Shard key. Omit on a single Raft group.")


class UpdateBody(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "name": "Lin",
                "email": "lin@example.com",
                "status": 0,
                "tags": ["later"],
                "created_at": "2024-01-02T03:04:05Z",
            }
        },
    )
    name: str = Field(description="Required.")
    email: str = Field(description="Required.")
    status: int = Field(description="Integer from 0 to 127.")
    tags: list[str] = Field(description="Required. Use an empty list to clear tags.")
    created_at: str = Field(description="RFC3339 or YYYY-MM-DD HH:MM:SS.")
    node: str | None = Field(default=None, description="MySQL address. The node query parameter overrides this.")


def body_schema(model: type[BaseModel]) -> dict:
    return {
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": model.model_json_schema()}},
        }
    }


def query_param(name: str, schema: dict, description: str) -> dict:
    return {"name": name, "in": "query", "required": False, "schema": schema, "description": description}


NODE_QUERY = query_param("node", {"type": "string"}, "MySQL address. Empty uses the first address.")
LIST_QUERIES = [
    query_param("page", {"type": "integer", "minimum": 1, "default": DEFAULT_PAGE}, "Page number, starting at 1."),
    query_param(
        "size",
        {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_SIZE, "default": DEFAULT_SIZE},
        f"Page size, from 1 to {MAX_PAGE_SIZE}.",
    ),
    query_param("name", {"type": "string"}, "Substring match on name."),
    query_param("email", {"type": "string"}, "Substring match on email."),
    query_param("tag", {"type": "string"}, "Substring match on tags."),
    query_param("status", {"type": "integer", "minimum": 0, "maximum": 127}, "Exact status."),
    query_param("created_after", {"type": "string"}, "RFC3339 or YYYY-MM-DD HH:MM:SS."),
    query_param("created_before", {"type": "string"}, "RFC3339 or YYYY-MM-DD HH:MM:SS."),
    NODE_QUERY,
]
ERRORS = {
    400: {"model": ErrorDoc, "description": "Bad request"},
    404: {"model": ErrorDoc, "description": "Not found"},
    409: {"model": ErrorDoc, "description": "Account already exists"},
    500: {"model": ErrorDoc, "description": "Internal error"},
}


def create_app(store: Store | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.store is None:
            app.state.store = mysql_from_env()
        yield

    app = FastAPI(title="Accounts", version="1.0.0", lifespan=lifespan)
    app.state.store = store

    @app.get("/cluster", response_model=ClusterDoc, responses={500: ERRORS[500]})
    def cluster(request: Request) -> JSONResponse:
        return JSONResponse({"nodes": [node_doc(node) for node in store_of(request).status()]})

    @app.get(
        "/accounts",
        response_model=AccountListDoc,
        responses={400: ERRORS[400], 500: ERRORS[500]},
        openapi_extra={"parameters": LIST_QUERIES},
    )
    def list_accounts(request: Request) -> JSONResponse:
        page, size, filt, node = parse_list_query(request)
        result = store_of(request).list(filt, page, size, node)
        return JSONResponse(
            {
                "page": page,
                "size": size,
                "total": result.total,
                "accounts": [account_doc(account) for account in result.accounts],
            }
        )

    @app.post(
        "/accounts",
        status_code=201,
        response_model=InsertDoc,
        responses={400: ERRORS[400], 409: ERRORS[409], 500: ERRORS[500]},
        openapi_extra=body_schema(CreateBody),
    )
    async def create_account(request: Request) -> JSONResponse:
        body = await read_json(request)
        account, note, node = account_from_create(body)
        result = store_of(request).insert(account, note, node)
        return JSONResponse(
            insert_doc(result),
            status_code=201,
            headers={"Location": f"/accounts/{result.account.id}"},
        )

    @app.get(
        "/accounts/{account_id}",
        response_model=AccountDoc,
        responses={400: ERRORS[400], 404: ERRORS[404], 500: ERRORS[500]},
        openapi_extra={"parameters": [NODE_QUERY]},
    )
    def get_account(account_id: str, request: Request) -> JSONResponse:
        return JSONResponse(account_doc(store_of(request).get(parse_id(account_id), request.query_params.get("node", ""))))

    @app.put(
        "/accounts/{account_id}",
        response_model=AccountDoc,
        responses={400: ERRORS[400], 404: ERRORS[404], 500: ERRORS[500]},
        openapi_extra={**body_schema(UpdateBody), "parameters": [NODE_QUERY]},
    )
    async def update_account(account_id: str, request: Request) -> JSONResponse:
        body = await read_json(request)
        account = account_from_update(parse_id(account_id), body)
        node = request.query_params.get("node") or str(body.get("node") or "")
        store_of(request).update(account, node)
        return JSONResponse(account_doc(account))

    @app.delete(
        "/accounts/{account_id}",
        status_code=204,
        responses={400: ERRORS[400], 404: ERRORS[404], 500: ERRORS[500]},
        openapi_extra={"parameters": [NODE_QUERY]},
    )
    def delete_account(account_id: str, request: Request) -> Response:
        store_of(request).delete(parse_id(account_id), request.query_params.get("node", ""))
        return Response(status_code=204)

    @app.exception_handler(BadRequest)
    def bad_request(_request: Request, exc: BadRequest) -> JSONResponse:
        return JSONResponse({"error": exc.message}, status_code=400)

    @app.exception_handler(NotFound)
    def not_found(_request: Request, _exc: NotFound) -> JSONResponse:
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.exception_handler(Conflict)
    def conflict(_request: Request, _exc: Conflict) -> JSONResponse:
        return JSONResponse({"error": "account already exists"}, status_code=409)

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


def account_doc(account: Account) -> dict:
    return {
        "id": account.id,
        "name": account.name,
        "email": account.email,
        "status": account.status,
        "tags": normalize(account.tags),
        "created_at": format_time(account.created_at),
        "notes": [note_doc(note) for note in account.notes],
    }


def note_doc(note: Note) -> dict:
    return {"id": note.id, "body": note.body, "created_at": format_time(note.created_at)}


def insert_doc(result: InsertResult) -> dict:
    return {
        "account": account_doc(result.account),
        "wrote_on": result.wrote_on,
        "read_back": account_doc(result.read_back) if result.read_back else None,
        "other_node": {
            "addr": result.other_addr,
            "account": account_doc(result.other) if result.other else None,
        },
    }


def node_doc(node: NodeStatus) -> dict:
    return {
        "addr": node.addr,
        "role": node.role,
        "leader": node.leader,
        "commit_index": node.commit_index,
        "applied_index": node.applied_index,
        "lag": node.lag,
        "suffrage": node.suffrage,
        "error": node.error,
    }


def normalize(tags: list[str] | None) -> list[str]:
    return [] if tags is None else tags


def format_time(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond:06d}Z"


def parse_id(value: str) -> int:
    try:
        account_id = int(value)
    except ValueError:
        account_id = 0
    if account_id < 1 or account_id > 2**63 - 1:
        raise BadRequest("id must be a positive integer")
    return account_id


def parse_list_query(request: Request) -> tuple[int, int, Filter, str]:
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
    filt = Filter(name=q.get("name", ""), email=q.get("email", ""), tag=q.get("tag", ""))
    if q.get("status", "") != "":
        filt.status = status_text(q["status"])
    if q.get("created_after"):
        filt.created_after = parse_labeled_time("created_after", q["created_after"])
    if q.get("created_before"):
        filt.created_before = parse_labeled_time("created_before", q["created_before"])
    return page, size, filt, q.get("node", "")


def positive_int(value: str, message: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise BadRequest(message) from None
    if number < 1:
        raise BadRequest(message)
    return number


def status_text(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise BadRequest("status must be an integer from 0 to 127") from None
    if number < 0 or number > 127:
        raise BadRequest("status must be an integer from 0 to 127")
    return number


def account_from_create(body: dict) -> tuple[Account, str, str]:
    name, email = required_name_email(body)
    created = datetime.now(timezone.utc)
    if "created_at" in body and body["created_at"] is not None:
        created = parse_labeled_time("created_at", body["created_at"])
    if "note" not in body or body["note"] is None or not str(body["note"]).strip():
        raise BadRequest("note is required")
    if not isinstance(body["note"], str):
        raise BadRequest("note has the wrong type")
    status = 1 if body.get("status") is None else status_value(body["status"])
    account_id = 0
    if body.get("id") is not None:
        account_id = positive_int(str(body["id"]), "id must be a positive integer")
    return (
        Account(
            id=account_id,
            name=name,
            email=email,
            status=status,
            tags=normalize(tag_list(body.get("tags"), required=False)),
            created_at=created,
        ),
        body["note"].strip(),
        str(body.get("node") or ""),
    )


def account_from_update(account_id: int, body: dict) -> Account:
    name, email = required_name_email(body)
    if "status" not in body or body["status"] is None:
        raise BadRequest("status must be an integer from 0 to 127")
    if "tags" not in body or body["tags"] is None:
        raise BadRequest("tags is required")
    if "created_at" not in body or body["created_at"] is None:
        raise BadRequest("created_at is required")
    return Account(
        id=account_id,
        name=name,
        email=email,
        status=status_value(body["status"]),
        tags=tag_list(body["tags"], required=True),
        created_at=parse_labeled_time("created_at", body["created_at"]),
    )


def status_value(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 127:
        raise BadRequest("status must be an integer from 0 to 127")
    return value


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


def tag_list(value: object, required: bool) -> list[str]:
    if value is None:
        if required:
            raise BadRequest("tags is required")
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise BadRequest("tags has the wrong type")
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
    return str(Path(__file__).resolve().parent.parent / "certs" / "ca.crt")


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
    raw = env("HTTP_ADDR", ":8000")
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
