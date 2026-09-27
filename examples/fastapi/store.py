"""MySQL access for mydb.mytable on the HardhatDB server."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TypeVar

import pymysql
from pymysql.constants.ER import DUP_ENTRY
from pymysql.cursors import DictCursor

TABLE = "mytable"
# Can't connect, server has gone away, lost connection.
_CONN_CODES = {2002, 2003, 2006, 2013}
log = logging.getLogger("example_fastapi")
T = TypeVar("T")


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


@dataclass
class Person:
    id: int
    name: str
    email: str
    phone_numbers: list[str]
    created_at: datetime


@dataclass
class Filter:
    name: str = ""
    email: str = ""
    phone: str = ""
    created_after: datetime | None = None
    created_before: datetime | None = None

    def where(self) -> tuple[str, list[Any]]:
        conds: list[str] = []
        args: list[Any] = []
        if self.name:
            conds.append("name LIKE %s")
            args.append(like_contains(self.name))
        if self.email:
            conds.append("email LIKE %s")
            args.append(like_contains(self.email))
        if self.phone:
            conds.append("CAST(phone_numbers AS CHAR) LIKE %s")
            args.append(like_contains(self.phone))
        if self.created_after is not None:
            conds.append("created_at >= %s")
            args.append(self.created_after)
        if self.created_before is not None:
            conds.append("created_at <= %s")
            args.append(self.created_before)
        if not conds:
            return "", args
        return " WHERE " + " AND ".join(conds), args


@dataclass
class Page:
    people: list[Person]
    total: int


class Store(Protocol):
    def list(self, filt: Filter, page: int, size: int) -> Page: ...
    def get(self, person_id: int) -> Person: ...
    def insert(self, person: Person) -> Person: ...
    def update(self, person: Person) -> None: ...
    def delete(self, person_id: int) -> None: ...


def like_contains(value: str) -> str:
    """Substring match. MySQL's default LIKE escape is backslash."""
    out = ["%"]
    for ch in value:
        if ch in "\\%_":
            out.append("\\")
        out.append(ch)
    out.append("%")
    return "".join(out)


def normalize_phones(phones: list[str] | None) -> list[str]:
    return [] if phones is None else phones


class MySQLStore:
    """Reads and writes round-robin across every address. A broken connection tries the next one."""

    def __init__(self, addrs: list[str], user: str, password: str, database: str, ssl_ca: str | None = None):
        if not addrs:
            raise ValueError("no MySQL addresses")
        self._addrs = list(addrs)
        self._user = user
        self._password = password
        self._database = database
        self._ssl = {"ca": ssl_ca} if ssl_ca else None
        self._next = 0
        self._lock = threading.Lock()
        alive = 0
        first: Exception | None = None
        for addr in self._addrs:
            try:
                self.ping(addr)
            except Exception as exc:
                log.warning("MySQL %s is down: %s", addr, exc)
                if first is None:
                    first = exc
                continue
            alive += 1
        if alive == 0:
            assert first is not None
            raise first

    def ping(self, addr: str) -> None:
        with self._connect(addr) as conn:
            conn.ping()

    def list(self, filt: Filter, page: int, size: int) -> Page:
        def run(addr: str) -> Page:
            where, args = filt.where()
            with self._cursor(addr) as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM {TABLE}{where}", args)
                total = int(cur.fetchone()["n"])
                offset = (page - 1) * size
                cur.execute(
                    f"SELECT id, name, email, phone_numbers, created_at FROM {TABLE}{where} "
                    "ORDER BY name, email LIMIT %s OFFSET %s",
                    [*args, size, offset],
                )
                return Page(people=[row_person(row) for row in cur.fetchall()], total=total)

        return self._run(run)

    def get(self, person_id: int) -> Person:
        def run(addr: str) -> Person:
            with self._cursor(addr) as cur:
                cur.execute(
                    f"SELECT id, name, email, phone_numbers, created_at FROM {TABLE} WHERE id = %s",
                    (person_id,),
                )
                row = cur.fetchone()
            if row is None:
                raise NotFound
            return row_person(row)

        return self._run(run)

    def insert(self, person: Person) -> Person:
        phones = json.dumps(normalize_phones(person.phone_numbers))

        def run(addr: str) -> Person:
            try:
                with self._cursor(addr) as cur:
                    cur.execute(
                        f"INSERT INTO {TABLE} (name, email, phone_numbers, created_at) VALUES (%s, %s, %s, %s)",
                        (person.name, person.email, phones, person.created_at),
                    )
                    person.id = int(cur.lastrowid)
            except pymysql.IntegrityError as exc:
                if exc.args and exc.args[0] == DUP_ENTRY:
                    raise Conflict from exc
                raise
            person.phone_numbers = normalize_phones(person.phone_numbers)
            return person

        return self._run(run)

    def update(self, person: Person) -> None:
        phones = json.dumps(normalize_phones(person.phone_numbers))

        def run(addr: str) -> None:
            with self._cursor(addr) as cur:
                cur.execute(
                    f"UPDATE {TABLE} SET name = %s, email = %s, phone_numbers = %s, created_at = %s WHERE id = %s",
                    (person.name, person.email, phones, person.created_at, person.id),
                )
                if cur.rowcount == 0:
                    raise NotFound

        self._run(run)

    def delete(self, person_id: int) -> None:
        def run(addr: str) -> None:
            with self._cursor(addr) as cur:
                cur.execute(f"DELETE FROM {TABLE} WHERE id = %s", (person_id,))
                if cur.rowcount == 0:
                    raise NotFound

        self._run(run)

    def _run(self, fn: Callable[[str], T]) -> T:
        count = len(self._addrs)
        with self._lock:
            start = self._next
            self._next += 1
        last: Exception | None = None
        for offset in range(count):
            addr = self._addrs[(start + offset) % count]
            try:
                return fn(addr)
            except Exception as exc:
                if not conn_err(exc):
                    raise
                last = exc
        assert last is not None
        raise last

    def _cursor(self, addr: str):
        return _Cursor(self._connect(addr))

    def _connect(self, addr: str) -> pymysql.Connection:
        host, port = split_host_port(addr)
        return pymysql.connect(
            host=host,
            port=port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=2,
            ssl=self._ssl,
        )


class _Cursor:
    def __init__(self, conn: pymysql.Connection):
        self._conn = conn
        self._cur: DictCursor | None = None

    def __enter__(self) -> DictCursor:
        self._cur = self._conn.cursor()
        return self._cur

    def __exit__(self, *exc: object) -> None:
        if self._cur is not None:
            self._cur.close()
        self._conn.close()


def row_person(row: dict[str, Any]) -> Person:
    created = row["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    else:
        created = created.astimezone(timezone.utc)
    return Person(
        id=int(row["id"]),
        name=row["name"],
        email=row["email"],
        phone_numbers=decode_phones(row["phone_numbers"]),
        created_at=created,
    )


def decode_phones(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise TypeError("phone_numbers is not a JSON array")
    return [str(item) for item in value]


def conn_err(exc: BaseException) -> bool:
    """A failure to reach the node. A SQL result is not a broken connection."""
    if isinstance(exc, pymysql.Error):
        if isinstance(exc, pymysql.err.InterfaceError):
            return True
        return bool(exc.args) and exc.args[0] in _CONN_CODES
    return isinstance(exc, OSError)


def split_host_port(addr: str) -> tuple[str, int]:
    host, sep, port = addr.rpartition(":")
    if not sep or not host:
        raise ValueError(f"address must be host:port, got {addr!r}")
    return host, int(port)
