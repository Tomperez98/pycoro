from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from random import Random
from typing import TYPE_CHECKING

from pycoro import Pycoro
from pycoro.aio import AIODst
from pycoro.subsystems.store import StoreCompletion, StoreSubmission, Transaction
from pycoro.subsystems.store.sqlite import StoreSqliteSubsystem

if TYPE_CHECKING:
    from sqlite3 import Connection

    from pycoro.scheduler import Computation


MAX_COROS = 1000
NUM_ACCOUNTS = 5


@dataclass(frozen=True)
class ReadBalance:
    account_id: int


def read_balance(conn: Connection, cmd: ReadBalance) -> tuple[int, int]:
    balance, version = conn.execute(
        "SELECT balance, version FROM accounts WHERE account_id = ?",
        (cmd.account_id,),
    ).fetchone()
    return balance, version


@dataclass(frozen=True)
class UpdateBalanceEnsureVersion:
    account_id: int
    amount: int
    version: int


def update_balance_ensure_version(conn: Connection, cmd: UpdateBalanceEnsureVersion) -> None:
    cur = conn.execute(
        """
                    UPDATE accounts
                    SET
                        balance = balance + ?,
                        version = version + 1
                    WHERE account_id = ? and version = ?
                    """,
        (cmd.amount, cmd.account_id, cmd.version),
    )
    assert cur.rowcount == 1


@dataclass(frozen=True)
class UpdateBalance:
    account_id: int
    amount: int


def update_balance(conn: Connection, cmd: UpdateBalance) -> None:
    cur = conn.execute(
        """
                UPDATE accounts
                SET
                    balance = balance + ?,
                    version = version + 1
                WHERE account_id = ?
                """,
        (cmd.amount, cmd.account_id),
    )
    assert cur.rowcount == 1


@dataclass(frozen=True)
class CheckNegativeBalanceAccounts: ...


def check_negative_balance_accounts(conn: Connection, cmd: CheckNegativeBalanceAccounts) -> bool:  # noqa: ARG001
    accounts_in_negative: int = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE balance < 0",
    ).fetchone()[0]
    return accounts_in_negative == 0


@dataclass(frozen=True)
class CheckNoMoneyDestroyed:
    money_in_the_system: int


def check_no_money_destroyed(conn: Connection, cmd: CheckNoMoneyDestroyed) -> bool:
    total_money_in_system: int = conn.execute(
        "SELECT SUM(balance) FROM accounts",
    ).fetchone()[0]
    return total_money_in_system == cmd.money_in_the_system


def transfer(source: int, target: int, amount: int) -> Computation[StoreSubmission, None]:
    if source == target:
        msg = "same accoun transfer"
        raise Exception(msg)  # noqa: TRY002

    source_balance, version = (
        yield (yield StoreSubmission(Transaction([ReadBalance(source)])))
    ).results[0]
    if source_balance - amount < 0:
        msg = "not enough founds."
        raise Exception(msg)  # noqa: TRY002
    yield (
        yield StoreSubmission(
            Transaction(
                [
                    UpdateBalanceEnsureVersion(source, -amount, version),
                    UpdateBalance(target, amount),
                ]
            )
        )
    )


def test_dst() -> None:
    Path().joinpath("dst.db").unlink(missing_ok=True)
    stmt = [
        "CREATE TABLE accounts(account_id INTEGER PRIMARY KEY, balance INTEGER, version INTEGER)"
    ]
    stmt.extend(f"INSERT INTO accounts VALUES ({i}, 100, 0)" for i in range(1, NUM_ACCOUNTS))  # noqa: S608

    r = Random()
    aio = AIODst(r, r.random())
    store_sqlite_subsystem = StoreSqliteSubsystem(
        aio, "dst.db", stmt, MAX_COROS, r.randint(1, MAX_COROS // 10)
    )
    store_sqlite_subsystem.add_command_handler(ReadBalance, read_balance)
    store_sqlite_subsystem.add_command_handler(UpdateBalance, update_balance)
    store_sqlite_subsystem.add_command_handler(
        UpdateBalanceEnsureVersion, update_balance_ensure_version
    )
    store_sqlite_subsystem.add_command_handler(
        CheckNegativeBalanceAccounts, check_negative_balance_accounts
    )
    store_sqlite_subsystem.add_command_handler(CheckNoMoneyDestroyed, check_no_money_destroyed)
    store_sqlite_subsystem.migrate()

    aio.attach_subsystem(store_sqlite_subsystem)

    s = Pycoro(aio, MAX_COROS, r.randint(1, MAX_COROS // 10))
    money_in_system = 100 * max(range(1, NUM_ACCOUNTS))
    futures = [
        s.add(
            transfer(
                r.choice(range(1, NUM_ACCOUNTS)),
                r.choice(range(1, NUM_ACCOUNTS)),
                r.randint(0, money_in_system * 2),
            )
        )
        for _ in range(MAX_COROS)
    ]

    for i in range(MAX_COROS * 5):
        s.tick(i)
        completion: StoreCompletion = aio.check(
            StoreSubmission(
                Transaction(
                    [CheckNegativeBalanceAccounts(), CheckNoMoneyDestroyed(money_in_system)]
                )
            )
        )
        no_negative_balance, no_money_destroyed = completion.results
        assert no_negative_balance
        assert no_money_destroyed

    completion = aio.check(StoreSubmission(Transaction([CheckNoMoneyDestroyed(money_in_system)])))
    no_money_destroyed = completion.results[0]
    assert no_money_destroyed
    Path().joinpath("dst.db").unlink(missing_ok=True)
    for f in futures:
        assert f.done()
