"""Handler tests with an in-memory store. They do not need MySQL."""

import threading
from datetime import datetime, timezone

import pymysql
import pytest
from fastapi.testclient import TestClient

from main import create_app, mysql_addrs
from store import Conflict, Filter, MySQLStore, NotFound, Page, Person

CREATED = datetime.fromisoformat("2022-11-01T12:00:00.000001+00:00")


def seed() -> list[Person]:
    return [
        Person(1, "Jane Deo", "janedeo@gmail.com", ["556-565-566", "777-777-777"], CREATED),
        Person(2, "Jane Doe", "jane@doe.com", [], CREATED),
        Person(3, "John Doe", "john@doe.com", ["555-555-555"], CREATED),
        Person(4, "John Doe", "johnalt@doe.com", [], CREATED),
    ]


class MemoryStore:
    def __init__(self) -> None:
        self.people = seed()
        self.last = Filter()

    def list(self, filt: Filter, page: int, size: int) -> Page:
        self.last = filt
        matched = [p for p in self.people if matches(p, filt)]
        matched.sort(key=lambda p: (p.name, p.email))
        start = (page - 1) * size
        return Page(people=matched[start : start + size], total=len(matched))

    def get(self, person_id: int) -> Person:
        for person in self.people:
            if person.id == person_id:
                return person
        raise NotFound

    def insert(self, person: Person) -> Person:
        person.id = max(p.id for p in self.people) + 1
        self.people.append(person)
        return person

    def update(self, person: Person) -> None:
        for i, existing in enumerate(self.people):
            if existing.id == person.id:
                self.people[i] = person
                return
        raise NotFound

    def delete(self, person_id: int) -> None:
        before = len(self.people)
        self.people = [p for p in self.people if p.id != person_id]
        if len(self.people) == before:
            raise NotFound


def matches(person: Person, filt: Filter) -> bool:
    if filt.name and filt.name not in person.name:
        return False
    if filt.email and filt.email not in person.email:
        return False
    if filt.phone and filt.phone not in " ".join(person.phone_numbers):
        return False
    if filt.created_after and person.created_at < filt.created_after:
        return False
    if filt.created_before and person.created_at > filt.created_before:
        return False
    return True


def client(store: MemoryStore | None = None) -> TestClient:
    return TestClient(create_app(store or MemoryStore()))


def test_list_pagination() -> None:
    with client() as http:
        res = http.get("/people")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")
    body = res.json()
    assert (body["page"], body["size"], body["total"]) == (1, 2, 4)
    assert [p["name"] for p in body["people"]] == ["Jane Deo", "Jane Doe"]
    assert body["people"][0]["created_at"] == "2022-11-01T12:00:00.000001Z"

    with client() as http:
        page2 = http.get("/people", params={"page": 2, "size": 2}).json()
    assert [p["id"] for p in page2["people"]] == [3, 4]


def test_list_filters() -> None:
    store = MemoryStore()
    with client(store) as http:
        res = http.get(
            "/people",
            params={
                "name": "Jane",
                "email": "doe.com",
                "phone": "555",
                "created_after": "2022-01-01T00:00:00Z",
                "created_before": "2023-01-01 00:00:00",
                "size": 1,
            },
        )
    assert res.status_code == 200
    assert res.json() == {"page": 1, "size": 1, "total": 0, "people": []}
    assert store.last.name == "Jane"
    assert store.last.created_after == datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert store.last.created_before == datetime(2023, 1, 1, tzinfo=timezone.utc)


def test_create_get_update_delete() -> None:
    store = MemoryStore()
    with client(store) as http:
        created = http.post(
            "/people",
            json={
                "name": "Ada Lovelace",
                "email": "ada@example.com",
                "phone_numbers": ["111-222-333"],
                "created_at": "2024-05-06T07:08:09.000010Z",
            },
        )
        assert created.status_code == 201
        body = created.json()
        assert body["id"] == 5
        assert created.headers["location"] == "/people/5"

        got = http.get("/people/5")
        assert got.status_code == 200
        assert got.json()["phone_numbers"] == ["111-222-333"]

        updated = http.put(
            "/people/5",
            json={
                "name": "Ada Lovelace",
                "email": "ada@example.com",
                "phone_numbers": ["999"],
                "created_at": "2024-01-02 03:04:05",
            },
        )
        assert updated.status_code == 200
        assert updated.json()["created_at"] == "2024-01-02T03:04:05.000000Z"
        assert updated.json()["phone_numbers"] == ["999"]

        deleted = http.delete("/people/5")
        assert deleted.status_code == 204
        assert deleted.content == b""
        missing = http.get("/people/5")
        assert missing.status_code == 404
        assert missing.json() == {"error": "not found"}


def test_bad_input() -> None:
    with client() as http:
        assert http.get("/people", params={"size": 0}).status_code == 400
        assert http.get("/people", params={"created_after": "yesterday"}).status_code == 400
        assert http.post("/people", json={"email": "a@b.c"}).status_code == 400
        assert http.get("/people/nope").status_code == 400
        assert http.put(
            "/people/2",
            json={"phone_numbers": [], "created_at": "2024-01-02T03:04:05Z"},
        ).status_code == 400
        assert http.get("/people/999").status_code == 404


def test_conflict(monkeypatch) -> None:
    class Boom(MemoryStore):
        def insert(self, person: Person) -> Person:
            raise Conflict

    with client(Boom()) as http:
        res = http.post("/people", json={"name": "A", "email": "a@b.c"})
    assert res.status_code == 409
    assert res.json() == {"error": "person already exists"}


def _store(addrs: list[str]) -> MySQLStore:
    store = MySQLStore.__new__(MySQLStore)
    store._addrs = addrs
    store._next = 0
    store._lock = threading.Lock()
    return store


def test_run_round_robin() -> None:
    store = _store(["a", "b"])
    assert store._run(lambda addr: addr) == "a"
    assert store._run(lambda addr: addr) == "b"
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
