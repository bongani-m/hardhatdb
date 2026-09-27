"""Handler tests with an in-memory store. They do not need MySQL."""

from datetime import datetime, timezone

import pymysql
import pytest
from fastapi.testclient import TestClient

from main import create_app, mysql_addrs
from store import Account, Conflict, Filter, InsertResult, MySQLStore, NodeStatus, NotFound, Note, Page

CREATED = datetime.fromisoformat("2022-11-01T12:00:00.000001+00:00")
ADDRS = ["n1:3306", "n2:3306", "n3:3306"]


def note(note_id: int, body: str) -> Note:
    return Note(note_id, body, CREATED)


def seed() -> list[Account]:
    return [
        Account(1, "Ada Lovelace", "ada@example.com", 1, ["demo"], CREATED, [note(1, "Wrote the first program"), note(2, "Cluster demo")]),
        Account(2, "Grace Hopper", "grace@example.com", 1, ["demo", "compiler"], CREATED, [note(3, "A compiler is a program"), note(4, "Second note")]),
        Account(3, "Alan Turing", "alan@example.com", 0, [], CREATED, [note(5, "Can machines think"), note(6, "Second note")]),
        Account(4, "Katherine Johnson", "katherine@example.com", 1, ["orbit"], CREATED, [note(7, "Calculated the trajectory"), note(8, "Second note")]),
    ]


class MemoryStore:
    def __init__(self) -> None:
        self.accounts = seed()
        self.last = Filter()
        self.node = ""

    def addrs(self) -> list[str]:
        return list(ADDRS)

    def status(self) -> list[NodeStatus]:
        out = []
        for index, addr in enumerate(ADDRS):
            out.append(
                NodeStatus(
                    addr=addr,
                    role="leader" if index == 0 else "follower",
                    leader="10.116.0.2:7001",
                    commit_index="3",
                    applied_index="3",
                    lag="0",
                    suffrage="voter",
                )
            )
        return out

    def list(self, filt: Filter, page: int, size: int, node: str = "") -> Page:
        self.last = filt
        self.node = node
        matched = [account for account in self.accounts if matches(account, filt)]
        matched.sort(key=lambda account: (account.name, account.email))
        start = (page - 1) * size
        return Page(accounts=matched[start : start + size], total=len(matched))

    def get(self, account_id: int, node: str = "") -> Account:
        for account in self.accounts:
            if account.id == account_id:
                return account
        raise NotFound

    def insert(self, account: Account, note_body: str, node: str = "") -> InsertResult:
        for existing in self.accounts:
            if existing.email == account.email:
                raise Conflict
        account.id = max(item.id for item in self.accounts) + 1
        account.notes = [Note(account.id, note_body, account.created_at)]
        self.accounts.append(account)
        wrote = node if node in ADDRS else ADDRS[0]
        other = ADDRS[(ADDRS.index(wrote) + 1) % len(ADDRS)]
        return InsertResult(account=account, wrote_on=wrote, read_back=account, other_addr=other, other=None)

    def update(self, account: Account, node: str = "") -> None:
        for index, existing in enumerate(self.accounts):
            if existing.id == account.id:
                account.notes = existing.notes
                self.accounts[index] = account
                return
        raise NotFound

    def delete(self, account_id: int, node: str = "") -> None:
        before = len(self.accounts)
        self.accounts = [account for account in self.accounts if account.id != account_id]
        if len(self.accounts) == before:
            raise NotFound


def matches(account: Account, filt: Filter) -> bool:
    if filt.name and filt.name not in account.name:
        return False
    if filt.email and filt.email not in account.email:
        return False
    if filt.tag and filt.tag not in " ".join(account.tags):
        return False
    if filt.status is not None and account.status != filt.status:
        return False
    if filt.created_after and account.created_at < filt.created_after:
        return False
    if filt.created_before and account.created_at > filt.created_before:
        return False
    return True


def client(store: MemoryStore | None = None) -> TestClient:
    return TestClient(create_app(store or MemoryStore()))


def test_list_pagination() -> None:
    with client() as http:
        res = http.get("/accounts", params={"size": 2})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")
    body = res.json()
    assert (body["page"], body["size"], body["total"]) == (1, 2, 4)
    assert [account["name"] for account in body["accounts"]] == ["Ada Lovelace", "Alan Turing"]
    assert body["accounts"][0]["created_at"] == "2022-11-01T12:00:00.000001Z"
    assert body["accounts"][0]["notes"][0]["body"] == "Wrote the first program"

    with client() as http:
        page2 = http.get("/accounts", params={"page": 2, "size": 2}).json()
    assert [account["id"] for account in page2["accounts"]] == [2, 4]


def test_list_filters() -> None:
    store = MemoryStore()
    with client(store) as http:
        res = http.get(
            "/accounts",
            params={
                "name": "Ada",
                "email": "example.com",
                "tag": "demo",
                "status": 1,
                "created_after": "2022-01-01T00:00:00Z",
                "created_before": "2023-01-01 00:00:00",
                "size": 1,
                "node": "n2:3306",
            },
        )
    assert res.status_code == 200
    body = res.json()
    assert body["total"] == 1
    assert body["accounts"][0]["email"] == "ada@example.com"
    assert store.last.name == "Ada"
    assert store.last.status == 1
    assert store.node == "n2:3306"
    assert store.last.created_after == datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert store.last.created_before == datetime(2023, 1, 1, tzinfo=timezone.utc)


def test_create_get_update_delete() -> None:
    store = MemoryStore()
    with client(store) as http:
        created = http.post(
            "/accounts",
            json={
                "name": "Lin",
                "email": "lin@example.com",
                "status": 1,
                "tags": ["demo"],
                "note": "hello",
                "node": "n2:3306",
                "created_at": "2024-05-06T07:08:09.000010Z",
            },
        )
        assert created.status_code == 201
        body = created.json()
        assert body["account"]["id"] == 5
        assert body["wrote_on"] == "n2:3306"
        assert body["read_back"]["notes"][0]["body"] == "hello"
        assert body["other_node"] == {"addr": "n3:3306", "account": None}
        assert created.headers["location"] == "/accounts/5"

        got = http.get("/accounts/5")
        assert got.status_code == 200
        assert got.json()["notes"][0]["body"] == "hello"

        updated = http.put(
            "/accounts/5",
            json={
                "name": "Lin",
                "email": "lin@example.com",
                "status": 0,
                "tags": ["later"],
                "created_at": "2024-01-02 03:04:05",
            },
        )
        assert updated.status_code == 200
        assert updated.json()["created_at"] == "2024-01-02T03:04:05.000000Z"
        assert updated.json()["status"] == 0

        deleted = http.delete("/accounts/5")
        assert deleted.status_code == 204
        assert deleted.content == b""
        missing = http.get("/accounts/5")
        assert missing.status_code == 404
        assert missing.json() == {"error": "not found"}


def test_openapi_shows_body_and_responses() -> None:
    schema = client().get("/openapi.json").json()
    create = schema["paths"]["/accounts"]["post"]
    create_body = create["requestBody"]["content"]["application/json"]["schema"]
    assert create_body["properties"]["note"]["description"] == "First note. Required."
    assert "InsertDoc" in create["responses"]["201"]["content"]["application/json"]["schema"]["$ref"]
    assert "ErrorDoc" in create["responses"]["400"]["content"]["application/json"]["schema"]["$ref"]

    update = schema["paths"]["/accounts/{account_id}"]["put"]
    update_body = update["requestBody"]["content"]["application/json"]["schema"]
    assert "created_at" in update_body["required"]
    assert "AccountDoc" in update["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]

    listed = schema["paths"]["/accounts"]["get"]
    assert "AccountListDoc" in listed["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
    names = {param["name"] for param in listed["parameters"]}
    assert {"page", "size", "name", "status", "node"} <= names


def test_cluster() -> None:
    with client() as http:
        body = http.get("/cluster").json()
    assert body["nodes"][0]["role"] == "leader"
    assert body["nodes"][1]["addr"] == "n2:3306"
    assert body["nodes"][2]["suffrage"] == "voter"


def test_bad_input() -> None:
    with client() as http:
        assert http.get("/accounts", params={"size": 0}).status_code == 400
        assert http.get("/accounts", params={"created_after": "yesterday"}).status_code == 400
        assert http.post("/accounts", json={"email": "a@b.c"}).status_code == 400
        assert http.get("/accounts/nope").status_code == 400
        assert http.put(
            "/accounts/2",
            json={"tags": [], "created_at": "2024-01-02T03:04:05Z"},
        ).status_code == 400
        assert http.get("/accounts/999").status_code == 404


def test_conflict() -> None:
    with client() as http:
        res = http.post("/accounts", json={"name": "Ada Lovelace", "email": "ada@example.com", "note": "again"})
    assert res.status_code == 409
    assert res.json() == {"error": "account already exists"}


def _store(addrs: list[str]) -> MySQLStore:
    store = MySQLStore.__new__(MySQLStore)
    store._addrs = addrs
    return store


def test_run_prefers_node() -> None:
    store = _store(["a", "b"])
    assert store._run(lambda addr: addr, "b") == "b"
    assert store._run(lambda addr: addr) == "a"


def test_run_skips_broken_connection() -> None:
    store = _store(["down", "up"])
    seen: list[str] = []

    def attempt(addr: str) -> str:
        seen.append(addr)
        if addr == "down":
            raise pymysql.err.OperationalError(2003, "Can't connect")
        return addr

    assert store._run(attempt) == "up"
    assert seen == ["down", "up"]


def test_run_does_not_retry_sql_error() -> None:
    store = _store(["a", "b"])
    seen: list[str] = []

    def attempt(addr: str) -> str:
        seen.append(addr)
        raise pymysql.err.IntegrityError(1062, "Duplicate")

    with pytest.raises(pymysql.err.IntegrityError):
        store._run(attempt)
    assert seen == ["a"]


def test_mysql_addrs(monkeypatch) -> None:
    monkeypatch.delenv("MYSQL_ADDRS", raising=False)
    monkeypatch.setenv("MYSQL_HOST", "db.internal")
    monkeypatch.setenv("MYSQL_PORT", "3306")
    assert mysql_addrs() == ["db.internal:3306"]
    monkeypatch.setenv("MYSQL_ADDRS", " 127.0.0.1:3306, ,127.0.0.1:3307 ")
    assert mysql_addrs() == ["127.0.0.1:3306", "127.0.0.1:3307"]
