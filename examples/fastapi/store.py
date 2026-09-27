"""MySQL access for mydb.accounts and mydb.notes on the HardhatDB server."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TypeVar

import pymysql
from pymysql.constants.ER import DUP_ENTRY
from pymysql.cursors import DictCursor

ACCOUNTS = "accounts"
NOTES = "notes"
# Can't connect, server has gone away, lost connection.
_CONN_CODES = {2002, 2003, 2006, 2013}
log = logging.getLogger("example_fastapi")
T = TypeVar("T")


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


@dataclass
class Note:
    id: int
    body: str
    created_at: datetime


@dataclass
class Account:
    id: int
    name: str
    email: str
    status: int
    tags: list[str]
    created_at: datetime
    notes: list[Note] = field(default_factory=list)


@dataclass
class Filter:
    name: str = ""
    email: str = ""
    tag: str = ""
    status: int | None = None
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
        if self.tag:
            conds.append("CAST(tags AS CHAR) LIKE %s")
            args.append(like_contains(self.tag))
        if self.status is not None:
            conds.append("status = %s")
            args.append(self.status)
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
    accounts: list[Account]
    total: int


@dataclass
class InsertResult:
    account: Account
    wrote_on: str
    read_back: Account | None
    other_addr: str
    other: Account | None


@dataclass
class NodeStatus:
    addr: str
    role: str = ""
    leader: str = ""
    commit_index: str = ""
    applied_index: str = ""
    lag: str = ""
    suffrage: str = ""
    error: str = ""


class Store(Protocol):
    def addrs(self) -> list[str]: ...
    def status(self) -> list[NodeStatus]: ...
    def list(self, filt: Filter, page: int, size: int, node: str = "") -> Page: ...
    def get(self, account_id: int, node: str = "") -> Account: ...
    def insert(self, account: Account, note: str, node: str = "") -> InsertResult: ...
    def update(self, account: Account, node: str = "") -> None: ...
    def delete(self, account_id: int, node: str = "") -> None: ...


def like_contains(value: str) -> str:
    """Substring match. MySQL's default LIKE escape is backslash."""
    out = ["%"]
    for ch in value:
        if ch in "\\%_":
            out.append("\\")
        out.append(ch)
    out.append("%")
    return "".join(out)


def normalize_tags(tags: list[str] | None) -> list[str]:
    return [] if tags is None else tags


class MySQLStore:
    """One request stays on one node. A broken connection tries the next address."""

    def __init__(self, addrs: list[str], user: str, password: str, database: str, ssl_ca: str | None = None):
        if not addrs:
            raise ValueError("no MySQL addresses")
        self._addrs = list(addrs)
        self._user = user
        self._password = password
        self._database = database
        self._ssl = {"ca": ssl_ca} if ssl_ca else None
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

    def addrs(self) -> list[str]:
        return list(self._addrs)

    def ping(self, addr: str) -> None:
        with self._connect(addr) as conn:
            conn.ping()

    def status(self) -> list[NodeStatus]:
        out: list[NodeStatus] = []
        for addr in self._addrs:
            node = NodeStatus(addr=addr)
            try:
                with self._cursor(addr) as cur:
                    cur.execute("SHOW RAFT STATUS")
                    row = cur.fetchone()
            except Exception as exc:
                if not conn_err(exc):
                    raise
                node.error = str(exc)
                out.append(node)
                continue
            if row:
                node.role = _text(row.get("role"))
                node.leader = _text(row.get("leader"))
                node.commit_index = _text(row.get("commit_index"))
                node.applied_index = _text(row.get("applied_index"))
                node.lag = _text(row.get("lag"))
                node.suffrage = _text(row.get("suffrage"))
            out.append(node)
        return out

    def list(self, filt: Filter, page: int, size: int, node: str = "") -> Page:
        def run(addr: str) -> Page:
            where, args = filt.where()
            with self._cursor(addr) as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM {ACCOUNTS}{where}", args)
                # A scatter returns one count per range. A single group returns one row.
                total = sum(int(row["n"]) for row in cur.fetchall())
                offset = (page - 1) * size
                cur.execute(
                    f"SELECT id, name, email, status, tags, created_at FROM {ACCOUNTS}{where} "
                    "ORDER BY name, email LIMIT %s OFFSET %s",
                    [*args, size, offset],
                )
                accounts = [row_account(row) for row in cur.fetchall()]
                _attach_notes(cur, accounts)
                return Page(accounts=accounts, total=total)

        return self._run(run, node)

    def get(self, account_id: int, node: str = "") -> Account:
        def run(addr: str) -> Account:
            with self._cursor(addr) as cur:
                account = _fetch(cur, account_id)
            if account is None:
                raise NotFound
            return account

        return self._run(run, node)

    def insert(self, account: Account, note: str, node: str = "") -> InsertResult:
        tags = json.dumps(normalize_tags(account.tags))

        def run(addr: str) -> InsertResult:
            try:
                with self._connect(addr, autocommit=False) as conn:
                    with conn.cursor() as cur:
                        if account.id > 0:
                            cur.execute(
                                f"INSERT INTO {ACCOUNTS} (id, name, email, status, tags, created_at) "
                                "VALUES (%s, %s, %s, %s, %s, %s)",
                                (account.id, account.name, account.email, account.status, tags, account.created_at),
                            )
                            account_id = account.id
                        else:
                            cur.execute(
                                f"INSERT INTO {ACCOUNTS} (name, email, status, tags, created_at) "
                                "VALUES (%s, %s, %s, %s, %s)",
                                (account.name, account.email, account.status, tags, account.created_at),
                            )
                            account_id = int(cur.lastrowid)
                        cur.execute(
                            f"INSERT INTO {NOTES} (account_id, body, created_at) VALUES (%s, %s, %s)",
                            (account_id, note, account.created_at),
                        )
                    conn.commit()
                    try:
                        with conn.cursor() as cur:
                            read_back = _fetch(cur, account_id)
                    except Exception as exc:
                        if not conn_err(exc):
                            raise
                        read_back = None
            except pymysql.IntegrityError as exc:
                if exc.args and exc.args[0] == DUP_ENTRY:
                    raise Conflict from exc
                raise
            other_addr = self._other(addr)
            other = None
            if other_addr:
                try:
                    with self._cursor(other_addr) as cur:
                        other = _fetch(cur, account_id)
                except Exception as exc:
                    if not conn_err(exc):
                        raise
            written = read_back if read_back is not None else Account(
                id=account_id,
                name=account.name,
                email=account.email,
                status=account.status,
                tags=normalize_tags(account.tags),
                created_at=account.created_at,
            )
            return InsertResult(
                account=written,
                wrote_on=addr,
                read_back=read_back,
                other_addr=other_addr,
                other=other,
            )

        return self._run(run, node)

    def update(self, account: Account, node: str = "") -> None:
        tags = json.dumps(normalize_tags(account.tags))

        def run(addr: str) -> None:
            try:
                with self._cursor(addr) as cur:
                    cur.execute(
                        f"UPDATE {ACCOUNTS} SET name = %s, email = %s, status = %s, tags = %s, created_at = %s "
                        "WHERE id = %s",
                        (account.name, account.email, account.status, tags, account.created_at, account.id),
                    )
                    if cur.rowcount == 0:
                        raise NotFound
            except pymysql.IntegrityError as exc:
                if exc.args and exc.args[0] == DUP_ENTRY:
                    raise Conflict from exc
                raise

        self._run(run, node)

    def delete(self, account_id: int, node: str = "") -> None:
        def run(addr: str) -> None:
            with self._connect(addr, autocommit=False) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {NOTES} WHERE account_id = %s", (account_id,))
                    cur.execute(f"DELETE FROM {ACCOUNTS} WHERE id = %s", (account_id,))
                    if cur.rowcount == 0:
                        conn.rollback()
                        raise NotFound
                conn.commit()

        self._run(run, node)

    def _other(self, addr: str) -> str:
        if len(self._addrs) < 2:
            return ""
        try:
            index = self._addrs.index(addr)
        except ValueError:
            return self._addrs[0]
        return self._addrs[(index + 1) % len(self._addrs)]

    def _run(self, fn: Callable[[str], T], prefer: str = "") -> T:
        start = 0
        if prefer:
            try:
                start = self._addrs.index(prefer)
            except ValueError:
                start = 0
        count = len(self._addrs)
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

    def _connect(self, addr: str, autocommit: bool = True) -> pymysql.Connection:
        host, port = split_host_port(addr)
        return pymysql.connect(
            host=host,
            port=port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            autocommit=autocommit,
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


def _fetch(cur: DictCursor, account_id: int) -> Account | None:
    cur.execute(
        f"SELECT id, name, email, status, tags, created_at FROM {ACCOUNTS} WHERE id = %s",
        (account_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    account = row_account(row)
    _attach_notes(cur, [account])
    return account


def _attach_notes(cur: DictCursor, accounts: list[Account]) -> None:
    if not accounts:
        return
    by_id = {account.id: account for account in accounts}
    for account in accounts:
        account.notes = []
    placeholders = ", ".join(["%s"] * len(accounts))
    cur.execute(
        f"SELECT id, account_id, body, created_at FROM {NOTES} "
        f"WHERE account_id IN ({placeholders}) ORDER BY id",
        [account.id for account in accounts],
    )
    for row in cur.fetchall():
        account = by_id.get(int(row["account_id"]))
        if account is None:
            continue
        created = _utc(row["created_at"])
        account.notes.append(Note(id=int(row["id"]), body=row["body"], created_at=created))


def row_account(row: dict[str, Any]) -> Account:
    return Account(
        id=int(row["id"]),
        name=row["name"],
        email=row["email"],
        status=int(row["status"]),
        tags=decode_tags(row["tags"]),
        created_at=_utc(row["created_at"]),
        notes=[],
    )


def decode_tags(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise TypeError("tags is not a JSON array")
    return [str(item) for item in value]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return str(value)


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
