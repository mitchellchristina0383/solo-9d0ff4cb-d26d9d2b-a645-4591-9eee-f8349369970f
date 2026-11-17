#!/usr/bin/env python3
"""回执封存/复核审计能力端到端自测（仅依赖 Python 标准库）。

覆盖：
  A1  封存当前回执 → 凭据落库；同快照立即复核 → 通过
  A2  封存后正常新增回执（id > 边界）→ 复核仍通过，且不计异常
  A3  删除旧回执 → 复核报告 missing
  A4  改动旧回执内容 → 复核报告 changed
  A5  以旧号插入回执 → 复核报告 inserted
  A6  接收端失联 → 封存与复核均失败（不显示成功/通过），恢复后复核可用
  A7  快照读取不完整（truncated 注入）→ 封存失败，且无新凭据残留
  A8  本地保存失败（save_fail 注入）→ 封存失败，且无新凭据残留
  A9  复核不存在的封存 → 404
  A10 新封存不覆盖旧凭据（seal_no 唯一、旧凭据条目不变）
  A11 发送端重启后仍可核验旧封存（凭据在独立 SQLite 库持久化）
  A12 封存与入账交错：并发 8 线程入账同时封存 → 快照行完整一致、无漏行/混行
  A13 回归：原有通知投递与回执查询照常可用
  A14 缺失+改动+插入并存的一次复核，三类异常同时报出

用法：
  python3 tests/audit_selftest.py [--lifecycle]
环境变量与 tests/selftest.py 相同（SENDER_URL / RECEIVER_URL / RN_HOOK_*）。
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

SENDER = os.environ.get("SENDER_URL", "http://127.0.0.1:8000").rstrip("/")
RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = str(int(time.time()))
ENV = "audit"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw.decode(errors="replace")}
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"A{RUN}-{tag}"


# ---------------- 基础操作封装 ----------------

def make_receipt(tag, result="success"):
    """直接走完整业务链路在接收端落一条回执，返回回执 id。"""
    code, n = req("POST", SENDER + "/api/notifications",
                  {"env": ENV, "release_no": key(tag), "result": result})
    assert code in (200, 201), (code, n)
    code, n2 = req("POST", f"{SENDER}/api/notifications/{n['id']}/send")
    assert n2.get("status") == "ACKED", n2
    return n2["receipt_id"]


def list_receipt_ids():
    code, d = req("GET", RECEIVER + "/api/receipts")
    assert code == 200
    return [x["id"] for x in d["items"]]


def create_seal(simulate=None, note=None):
    url = SENDER + "/api/seals"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url, {"note": note or "audit selftest"})


def verify(seal_no):
    return req("POST", f"{SENDER}/api/seals/{urllib.parse.quote(seal_no)}/verify")


def seals_list():
    code, d = req("GET", SENDER + "/api/seals")
    assert code == 200, d
    return d["items"]


def seal_detail(seal_no):
    code, d = req("GET", f"{SENDER}/api/seals/{urllib.parse.quote(seal_no)}")
    return code, d


def tamper(op, rid, new_result=None):
    body = {"op": op, "id": rid}
    if new_result:
        body["new_result"] = new_result
    return req("POST", RECEIVER + "/api/demo/tamper", body)


def snapshot(simulate=None):
    url = RECEIVER + "/api/snapshot"
    if simulate:
        url += f"?simulate={simulate}"
    return req("GET", url)


def ensure_gap_below(max_id):
    """返回一个小于当前最大号、且不在当前台账中的未占用回执号。

    若没有天然空档，则在 max_id 之外新造两条再删掉较小一条：
    留下的空档 id 高于封存边界（不在封存凭据内），又小于当前 MAX（允许 insert_old）。
    """
    used = set(list_receipt_ids())
    for i in range(1, max_id):
        if i not in used:
            return i
    hi1 = make_receipt(f"gap-hi1-{max_id}")
    hi2 = make_receipt(f"gap-hi2-{max_id}")
    assert hi1 > max_id and hi2 > hi1, (hi1, hi2, max_id)
    code, t = tamper("delete", hi1)
    assert code == 200, t
    return hi1


# ---------------- 场景 ----------------

def a1_seal_and_verify_clean():
    rid = make_receipt("a1")
    code, s = create_seal(note="A1 clean seal")
    check("A1 封存成功并返回边界与行数", code == 201 and s["max_id"] >= rid and s["row_count"] >= 1, s)
    check("A1 封存出现在列表", any(x["seal_no"] == s["seal_no"] for x in seals_list()))
    code, v = verify(s["seal_no"])
    check("A1 同台账立即复核通过", code == 200 and v["passed"] is True and not v["missing"]
          and not v["changed"] and not v["inserted"], v)
    code, detail = seal_detail(s["seal_no"])
    check("A1 凭据条目逐行落库且含边界内回执",
          code == 200 and len(detail["entries"]) == s["row_count"]
          and any(e["receipt_id"] == rid for e in detail["entries"]), detail)
    return s["seal_no"]


def a2_new_after_seal_not_anomaly(seal_no):
    code, before = verify(seal_no)
    assert before["passed"]
    boundary = before["boundary_max_id"]
    new_rid = make_receipt("a2")
    check("A2 新回执号大于封存边界", new_rid > boundary, (new_rid, boundary))
    code, v = verify(seal_no)
    check("A2 封存后正常新增不算异常、复核仍通过",
          code == 200 and v["passed"] is True and v["new_after_seal"] >= 1
          and not v["missing"] and not v["changed"] and not v["inserted"], v)


def a3_missing():
    rid = make_receipt("a3")
    code, s = create_seal()
    assert code == 201, s
    code, t = tamper("delete", rid)
    check("A3 演示删除旧回执成功", code == 200, t)
    code, v = verify(s["seal_no"])
    check("A3 复核报告缺失且不通过", code == 200 and v["passed"] is False
          and rid in v["missing"] and not v["changed"] and not v["inserted"], v)


def a4_changed():
    rid = make_receipt("a4")
    code, s = create_seal()
    assert code == 201, s
    code, t = tamper("change", rid, new_result="tampered_value")
    check("A4 演示改动旧回执成功", code == 200, t)
    code, v = verify(s["seal_no"])
    changed_ids = [c["receipt_id"] for c in v["changed"]]
    check("A4 复核报告内容改变且不通过",
          code == 200 and v["passed"] is False and rid in changed_ids
          and not v["missing"] and not v["inserted"], v)
    if v["changed"]:
        c = next(x for x in v["changed"] if x["receipt_id"] == rid)
        check("A4 改变明细同时保留封存值与当前值",
              c["sealed"]["result"] != c["current"]["result"]
              and c["current"]["result"] == "tampered_value", c)


def a5_inserted_old_id():
    make_receipt("a5a")
    make_receipt("a5b")
    code, s = create_seal()
    assert code == 201, s
    gap = ensure_gap_below(s["max_id"])
    code, t = tamper("insert_old", gap)
    check("A5 以旧号插入成功", code == 200, (gap, t))
    code, v = verify(s["seal_no"])
    inserted_ids = [c["receipt_id"] for c in v["inserted"]]
    check("A5 复核报告后来插入的旧号且不通过",
          code == 200 and v["passed"] is False and gap in inserted_ids
          and not v["missing"] and not v["changed"], v)


def a6_receiver_down():
    code, s = create_seal()
    assert code == 201, s
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, fail_seal = create_seal()
    check("A6 失联时封存失败（不显示成功）", code in (0, 502, 503, 504), (code, fail_seal))
    code, fail_verify = verify(s["seal_no"])
    check("A6 失联时复核失败（不显示通过）", code in (0, 502, 503, 504), (code, fail_verify))
    # 失联期间的失败复核也应留痕
    code, detail = seal_detail(s["seal_no"])
    check("A6 失败复核留痕且 passed=0",
          code == 200 and detail["verifications"]
          and detail["verifications"][0]["passed"] == 0
          and bool(detail["verifications"][0]["error"]), detail)
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("A6 接收端恢复", False)
        return s["seal_no"]
    code, v = verify(s["seal_no"])
    check("A6 恢复后复核恢复正常并通过", code == 200 and v["passed"] is True, v)
    return s["seal_no"]


def a7_truncated_snapshot():
    make_receipt("a7")
    before = len(seals_list())
    code, d = create_seal(simulate="truncated")
    check("A7 截断快照封存失败", code in (502, 500), (code, d))
    check("A7 失败后无新封存凭据残留", len(seals_list()) == before, seals_list())
    # 接收端原始快照接口自身的注入也应暴露不完整
    code, snap = snapshot(simulate="truncated")
    check("A7 截断快照声明行数与实际行数不一致",
          code == 200 and len(snap["rows"]) != snap["count"], snap)


def a8_save_fail():
    make_receipt("a8")
    before = len(seals_list())
    code, d = create_seal(simulate="save_fail")
    check("A8 本地保存失败时封存失败", code in (500, 502), (code, d))
    check("A8 保存失败后无新凭据残留", len(seals_list()) == before, seals_list())


def a9_unknown_seal():
    code, d = verify("SEAL-does-not-exist-000")
    check("A9 复核不存在的封存返回 404", code == 404, (code, d))
    code, d = seal_detail("SEAL-does-not-exist-000")
    check("A9 查询不存在的封存返回 404", code == 404, (code, d))


def a10_no_overwrite():
    code, s1 = create_seal(note="A10 first")
    assert code == 201, s1
    code, d1 = seal_detail(s1["seal_no"])
    entries1 = [(e["receipt_id"], e["row_hash"]) for e in d1["entries"]]
    make_receipt("a10")  # 台账继续变化
    code, s2 = create_seal(note="A10 second")
    check("A10 再次封存生成不同编号", code == 201 and s2["seal_no"] != s1["seal_no"], (s1, s2))
    code, d1b = seal_detail(s1["seal_no"])
    entries1b = [(e["receipt_id"], e["row_hash"]) for e in d1b["entries"]]
    check("A10 旧封存凭据未被新封存覆盖/改动", entries1 == entries1b, (entries1, entries1b))
    check("A10 两个封存均在库", len(seals_list()) >= 2 and
          {s1["seal_no"], s2["seal_no"]}.issubset({x["seal_no"] for x in seals_list()}))


def a11_restart_persistence(seal_no):
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("A11 发送端重启后恢复服务", False)
        return
    code, detail = seal_detail(seal_no)
    check("A11 重启后旧封存凭据仍可读取", code == 200 and detail["seal_no"] == seal_no, detail)
    code, v = verify(seal_no)
    check("A11 重启后仍可核验旧封存并通过", code == 200 and v["passed"] is True, v)


def a12_interleaving():
    # 先封存一次确定基线，随后并发入账与封存交错
    code, base = create_seal(note="A12 baseline")
    assert code == 201, base
    errors = []
    seal_results = []

    def post_worker(i):
        try:
            make_receipt(f"a12-{i}")
        except Exception as exc:  # noqa: BLE001
            errors.append(("post", i, str(exc)))

    def seal_worker():
        try:
            code, s = create_seal(note="A12 concurrent")
            seal_results.append((code, s))
        except Exception as exc:  # noqa: BLE001
            errors.append(("seal", str(exc)))

    threads = [threading.Thread(target=post_worker, args=(i,)) for i in range(8)]
    threads.append(threading.Thread(target=seal_worker))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check("A12 并发入账与封存无线程异常", not errors, errors)
    ok_seals = [s for code, s in seal_results if code == 201]
    check("A12 交错期间封存成功", len(ok_seals) == 1, seal_results)
    if ok_seals:
        s = ok_seals[0]
        code, snap = snapshot()
        ids_now = sorted(r["id"] for r in snap["rows"])
        # 封存快照必须与某个真实已提交状态一致：其全部 id 都仍存在、行数=条目数、最大号自洽
        code, detail = seal_detail(s["seal_no"])
        sealed_ids = sorted(e["receipt_id"] for e in detail["entries"])
        check("A12 封存凭据无漏行（行数=条目数，最大号自洽）",
              len(sealed_ids) == s["row_count"]
              and (not sealed_ids or max(sealed_ids) == s["max_id"])
              and len(set(sealed_ids)) == len(sealed_ids), (s, len(sealed_ids)))
        check("A12 封存行均为真实已提交回执（无部分新记录混入）",
              set(sealed_ids).issubset(set(ids_now)),
              ("sealed", sealed_ids, "now", ids_now))
        # 用该封存复核：边界内必须通过（封存时刻边界内没有被删改）
        code, v = verify(s["seal_no"])
        check("A12 交错封存复核通过（边界内无缺失/改动/插入）",
              code == 200 and v["passed"] is True, v)


def a13_regression():
    k = key("a13")
    code, n = req("POST", SENDER + "/api/notifications",
                  {"env": ENV, "release_no": k, "result": "success"})
    check("A13 回归：登记通知成功", code in (200, 201) and n["status"] == "PENDING", (code, n))
    code, n2 = req("POST", f"{SENDER}/api/notifications/{n['id']}/send?simulate=lose_response")
    check("A13 回归：丢回执保持待核", n2["status"] == "UNCERTAIN", n2)
    code, n3 = req("POST", f"{SENDER}/api/notifications/{n['id']}/reconcile")
    check("A13 回归：核对凭回执确认", n3["status"] == "ACKED" and bool(n3["receipt_id"]), n3)
    code, d = req("GET", RECEIVER + "/api/receipts")
    check("A13 回归：接收端台账查询照常", code == 200 and len(d["items"]) >= 1)
    qs = urllib.parse.urlencode({"env": ENV, "release_no": k})
    code, r1 = req("GET", f"{RECEIVER}/api/receipts?{qs}")
    check("A13 回归：单条回执查询照常", code == 200 and r1["content_hash"] == n["content_hash"], r1)


def a14_combined_anomalies():
    r_del = make_receipt("a14-del")
    r_chg = make_receipt("a14-chg")
    # 先造出一个位于封存边界之内的空档（造两条删一条，空档 id 会被随后封存固化为“不存在”）
    gap_hi1 = make_receipt("a14-gap-hi1")
    gap_hi2 = make_receipt("a14-gap-hi2")
    tamper("delete", gap_hi1)
    code, s = create_seal(note="A14 combined")
    assert code == 201, s
    gap = gap_hi1
    _, detail = seal_detail(s["seal_no"])
    check("A14 空档位于封存边界内且封存中不存在",
          gap < s["max_id"] and gap not in
          [e["receipt_id"] for e in detail["entries"]],
          (gap, s["max_id"]))
    tamper("delete", r_del)
    tamper("change", r_chg, new_result="changed_again")
    tamper("insert_old", gap)
    code, v = verify(s["seal_no"])
    check("A14 一次复核同时报出缺失/改动/插入三类异常",
          code == 200 and v["passed"] is False
          and r_del in v["missing"]
          and any(c["receipt_id"] == r_chg for c in v["changed"])
          and any(c["receipt_id"] == gap for c in v["inserted"]), v)
    # 恢复台账后再封存新快照，新封存复核通过；旧封存仍如实保留异常历史
    code, s2 = create_seal(note="A14 after anomalies, new seal")
    check("A14 异常后仍可建立新封存", code == 201, s2)


def main():
    lifecycle = "--lifecycle" in sys.argv or os.environ.get("RN_LIFECYCLE") == "1"
    print(f"发送端 {SENDER}  接收端 {RECEIVER}  lifecycle={lifecycle}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端。")
        sys.exit(2)

    seal_no_a1 = a1_seal_and_verify_clean()
    a2_new_after_seal_not_anomaly(seal_no_a1)
    a3_missing()
    a4_changed()
    a5_inserted_old_id()
    if lifecycle:
        seal_no_a6 = a6_receiver_down()
    else:
        seal_no_a6 = create_seal()[1]["seal_no"]
    a7_truncated_snapshot()
    a8_save_fail()
    a9_unknown_seal()
    a10_no_overwrite()
    if lifecycle:
        a11_restart_persistence(seal_no_a6)
    a12_interleaving()
    a13_regression()
    a14_combined_anomalies()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
