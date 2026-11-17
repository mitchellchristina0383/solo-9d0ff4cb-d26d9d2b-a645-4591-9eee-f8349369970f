"""回执封存与复核（发送端审计能力）。

凭据保存在与业务库完全独立的 SQLite 库（默认 SEAL_DB_PATH，区别于 DB_PATH）：
- 封存：拉取接收端「同一只读事务」的已提交快照，校验读取完整后，
  以封存时的最大回执号 max_id 为界，逐行保存不可变凭据（seal_entries）；
- 复核：重新拉取接收端当前快照，仅针对 <= max_id 的旧号比对，
  报告缺失（missing）/ 内容改变（changed）/ 后来插入的旧号（inserted）；
  封存后正常新增（id > max_id）的回执不算异常。

安全底线：
- 接收端失联、HTTP 非 2xx、快照读取不完整（行数/最大号/快照哈希对不上）、
  或本地凭据保存失败 → 一律抛 SealError，调用方不得显示封存成功或核验通过；
- 封存凭据只插入、不更新：seal_no 唯一，新封存绝不会覆盖旧凭据，
  发送端重启后仍可对旧封存执行复核。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone

import httpx

SEAL_DB_PATH = os.environ.get("SEAL_DB_PATH", "/data/seals.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))


class SealError(Exception):
    """封存/复核过程中任何无法确认成功的情况（调用方应返回错误，绝不显示成功）。"""

    def __init__(self, message: str, http_status: int = 502):
        super().__init__(message)
        self.http_status = http_status


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_seal_no() -> str:
    return "SEAL-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]


# ---------------- 独立凭据库 ----------------

def seal_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SEAL_DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_seal_db() -> None:
    os.makedirs(os.path.dirname(SEAL_DB_PATH) or ".", exist_ok=True)
    conn = seal_connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seals (
                seal_no TEXT PRIMARY KEY,              -- 唯一封存编号，INSERT 重复即失败，绝不覆盖
                receiver_snapshot_id TEXT NOT NULL,    -- 封存时接收端快照哈希
                max_id INTEGER NOT NULL,               -- 封存边界：当时最大回执号
                row_count INTEGER NOT NULL,
                taken_at TEXT NOT NULL,                -- 接收端快照时间
                sealed_at TEXT NOT NULL,               -- 本端封存时间
                note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seal_entries (
                seal_no TEXT NOT NULL,
                receipt_id INTEGER NOT NULL,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                row_hash TEXT NOT NULL,                -- 整行规范化哈希，任何字段改动都可发现
                PRIMARY KEY (seal_no, receipt_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seal_no TEXT NOT NULL,
                verified_at TEXT NOT NULL,
                passed INTEGER NOT NULL,               -- 1 通过 / 0 发现异常
                max_id INTEGER NOT NULL,
                current_snapshot_id TEXT,
                missing TEXT,                          -- JSON：封存时有、现在没有的回执号
                changed TEXT,                          -- JSON：内容改变的明细
                inserted TEXT,                         -- JSON：后来插入的旧号明细
                new_after_seal INTEGER NOT NULL,       -- 封存后正常新增（id > max_id）数量，不算异常
                error TEXT                             -- 复核过程中的通信/完整性错误
            )
            """
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


# ---------------- 接收端快照 ----------------

def _row_hash(item: dict) -> str:
    canonical = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fetch_snapshot(simulate: str | None = None) -> dict:
    """拉取并严格校验接收端快照。任何不完整/不一致都抛 SealError。"""
    url = f"{RECEIVER_URL}/api/snapshot"
    try:
        resp = httpx.get(url, params={"simulate": simulate} if simulate else None,
                         timeout=HTTP_TIMEOUT)
    except httpx.HTTPError as exc:
        raise SealError(f"接收端不可达，无法取得快照（{exc.__class__.__name__}）") from exc
    if resp.status_code != 200:
        raise SealError(f"接收端快照接口返回 {resp.status_code}，本次操作无效")
    try:
        snap = resp.json()
    except ValueError as exc:
        raise SealError("接收端快照响应无法解析，读取不完整") from exc

    rows = snap.get("rows")
    required = {"snapshot_id", "max_id", "count", "taken_at", "rows"}
    if not isinstance(rows, list) or not required.issubset(snap):
        raise SealError("接收端快照结构不完整，本次操作无效")

    # 1) 行数与声明一致（故障注入的截断快照在此即被拦下）
    if len(rows) != snap["count"]:
        raise SealError(
            f"快照读取不完整：声明 {snap['count']} 行，实际收到 {len(rows)} 行"
        )

    ids = [r.get("id") for r in rows]
    if any(i is None for i in ids) or len(set(ids)) != len(ids):
        raise SealError("快照中回执号缺失或重复，数据不可信")

    # 2) 最大号与声明一致，且不存在越界 id
    expect_max = max(ids) if ids else 0
    if expect_max != snap["max_id"]:
        raise SealError(
            f"快照最大回执号不一致：声明 {snap['max_id']}，实际 {expect_max}"
        )

    # 3) 重算快照哈希，确认传输内容与接收端事务快照逐字节一致
    canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
    calc_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if calc_id != snap["snapshot_id"]:
        raise SealError("快照哈希校验失败，读取内容可能被截断或篡改")

    # 4) 每行必备字段齐全
    entry_required = {"id", "env", "release_no", "result", "content_hash", "received_at"}
    for r in rows:
        if not entry_required.issubset(r):
            raise SealError(f"回执 #{r.get('id')} 字段不完整，数据不可信")
    return snap


# ---------------- 封存 ----------------

def create_seal(note: str | None = None, simulate: str | None = None) -> dict:
    """封存接收端当前回执。失败必抛 SealError，绝不返回成功。

    演示故障注入：
    - simulate=truncated：接收端返回被截断的快照（读取不完整）；
    - simulate=save_fail：快照校验通过后，本地凭据保存失败（不落任何凭据）。
    """
    snap = fetch_snapshot(simulate="truncated" if simulate == "truncated" else None)
    seal_no = new_seal_no()
    sealed_at = now_iso()
    rows = snap["rows"]

    conn = seal_connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if simulate == "save_fail":
                # 故意写入非法列触发 sqlite3.Error，随后回滚：不留半条封存凭据
                conn.execute(
                    "INSERT INTO seals (seal_no, no_such_column) VALUES (?, ?)",
                    (seal_no, "x"),
                )
            # 主键冲突会直接抛 IntegrityError：新封存不能覆盖旧凭据
            conn.execute(
                """INSERT INTO seals
                   (seal_no, receiver_snapshot_id, max_id, row_count, taken_at, sealed_at, note)
                   VALUES (?,?,?,?,?,?,?)""",
                (seal_no, snap["snapshot_id"], snap["max_id"], snap["count"],
                 snap["taken_at"], sealed_at, note),
            )
            conn.executemany(
                """INSERT INTO seal_entries
                   (seal_no, receipt_id, env, release_no, result, content_hash, received_at, row_hash)
                   VALUES (?,?,?,?,?,?,?,?)""",
                [
                    (
                        seal_no, r["id"], r["env"], r["release_no"], r["result"],
                        r["content_hash"], r["received_at"], _row_hash(r),
                    )
                    for r in rows
                ],
            )
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
    except sqlite3.Error as exc:
        # 保存失败：调用方必须显示封存失败
        raise SealError(f"封存凭据保存失败：{exc}", http_status=500) from exc
    finally:
        conn.close()

    return {
        "seal_no": seal_no,
        "receiver_snapshot_id": snap["snapshot_id"],
        "max_id": snap["max_id"],
        "row_count": snap["count"],
        "taken_at": snap["taken_at"],
        "sealed_at": sealed_at,
        "note": note,
    }


# ---------------- 复核 ----------------

def _load_seal(conn: sqlite3.Connection, seal_no: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM seals WHERE seal_no=?", (seal_no,)).fetchone()


def verify_seal(seal_no: str) -> dict:
    """对既有封存重新核验。任何通信/完整性/保存失败都不得给出「通过」。"""
    conn = seal_connect()
    try:
        seal = _load_seal(conn, seal_no)
        if seal is None:
            raise SealError(f"封存 {seal_no} 不存在", http_status=404)
        boundary = seal["max_id"]
        entries = conn.execute(
            "SELECT * FROM seal_entries WHERE seal_no=? ORDER BY receipt_id ASC", (seal_no,)
        ).fetchall()
    finally:
        conn.close()

    error_text: str | None = None
    snap: dict | None = None
    try:
        snap = fetch_snapshot()
    except SealError as exc:
        error_text = str(exc)

    missing, changed, inserted, new_after = [], [], [], 0
    current_snapshot_id = snap["snapshot_id"] if snap else None
    passed = 0

    if snap is not None:
        # 只看封存边界内（id <= max_id）的旧号；id > max_id 属于封存后正常新增
        current_old = {r["id"]: r for r in snap["rows"] if r["id"] <= boundary}
        new_after = sum(1 for r in snap["rows"] if r["id"] > boundary)
        sealed_ids = {e["receipt_id"] for e in entries}

        for e in entries:
            cur = current_old.get(e["receipt_id"])
            if cur is None:
                missing.append(e["receipt_id"])
                continue
            if _row_hash(cur) != e["row_hash"] or cur.get("content_hash") != e["content_hash"]:
                changed.append({
                    "receipt_id": e["receipt_id"],
                    "sealed": {
                        "env": e["env"], "release_no": e["release_no"],
                        "result": e["result"], "content_hash": e["content_hash"],
                        "received_at": e["received_at"],
                    },
                    "current": {
                        "env": cur.get("env"), "release_no": cur.get("release_no"),
                        "result": cur.get("result"),
                        "content_hash": cur.get("content_hash"),
                        "received_at": cur.get("received_at"),
                    },
                })
        for rid, r in current_old.items():
            if rid not in sealed_ids:
                inserted.append({
                    "receipt_id": rid,
                    "env": r.get("env"),
                    "release_no": r.get("release_no"),
                    "result": r.get("result"),
                    "received_at": r.get("received_at"),
                })

        passed = 1 if not (missing or changed or inserted) else 0

    # 复核记录同样落库；保存失败则整体按错误处理，不显示通过
    conn = seal_connect()
    try:
        conn.execute(
            """INSERT INTO verifications
               (seal_no, verified_at, passed, max_id, current_snapshot_id,
                missing, changed, inserted, new_after_seal, error)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                seal_no, now_iso(), passed, boundary, current_snapshot_id,
                json.dumps(missing, ensure_ascii=False),
                json.dumps(changed, ensure_ascii=False),
                json.dumps(inserted, ensure_ascii=False),
                new_after, error_text,
            ),
        )
    except sqlite3.Error as exc:
        raise SealError(f"复核记录保存失败：{exc}", http_status=500) from exc
    finally:
        conn.close()

    if error_text is not None:
        raise SealError(error_text)

    return {
        "seal_no": seal_no,
        "verified_at": now_iso(),
        "passed": bool(passed),
        "boundary_max_id": boundary,
        "current_snapshot_id": current_snapshot_id,
        "missing": missing,
        "changed": changed,
        "inserted": inserted,
        "new_after_seal": new_after,
    }


def list_seals() -> list[dict]:
    conn = seal_connect()
    try:
        rows = conn.execute(
            """
            SELECT s.*,
                   (SELECT COUNT(*) FROM verifications v WHERE v.seal_no = s.seal_no) AS verify_count,
                   (SELECT passed FROM verifications v WHERE v.seal_no = s.seal_no
                      ORDER BY id DESC LIMIT 1) AS last_passed
            FROM seals s ORDER BY s.sealed_at DESC
            """
        ).fetchall()
        return [row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_seal_detail(seal_no: str) -> dict | None:
    conn = seal_connect()
    try:
        seal = _load_seal(conn, seal_no)
        if seal is None:
            return None
        entries = conn.execute(
            "SELECT receipt_id, env, release_no, result, content_hash, received_at, row_hash "
            "FROM seal_entries WHERE seal_no=? ORDER BY receipt_id ASC",
            (seal_no,),
        ).fetchall()
        verifs = conn.execute(
            "SELECT * FROM verifications WHERE seal_no=? ORDER BY id DESC", (seal_no,)
        ).fetchall()
        out = row_to_dict(seal)
        out["entries"] = [row_to_dict(e) for e in entries]
        out["verifications"] = []
        for v in verifs:
            vd = row_to_dict(v)
            for k in ("missing", "changed", "inserted"):
                vd[k] = json.loads(vd[k]) if vd[k] else ([] if k == "missing" else [])
            out["verifications"].append(vd)
        return out
    finally:
        conn.close()
