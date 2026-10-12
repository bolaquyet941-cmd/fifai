"""
runner.py — Orchestrator cho br.py
- accounts.json: KHÔNG BAO GIỜ BỊ SỬA
- accounts_done.json: chỉ chứa {uid, password} — append khi acc đủ level
- Chạy MAX_CONCURRENT acc song song
- Đủ target → next acc kế tiếp từ queue
- Kiểm tra cuối: mọi UID trong accounts.json đều có trong accounts_done.json
  → dừng
"""

import asyncio
import os
import sys
import json
import time
from typing import List, Optional, Dict, Set

try:
    import lw as Main
except ImportError:
    print("[-] Không tìm thấy lw.py")
    sys.exit(1)

try:
    from dashboard_server import bot_state, start_web_dashboard
except ImportError:
    bot_state = None
    start_web_dashboard = None

if bot_state:
    try:
        bot_state.log("Runner khởi động — bắt đầu farm", "info")
    except Exception:
        pass

# ============ CONFIG ============
ACC_FILE = "accounts_done.json"
DONE_FILE = "acc.json"
MAX_CONCURRENT = 5
TARGET_LEVEL = 21
LEVEL_CHECK_INTERVAL = 15
LOGIN_RETRY_DELAY = 20
VERIFY_INTERVAL = 30


# ============ ĐỌC / GHI FILE ============
def load_accounts(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        out = []
        for acc in data:
            if not isinstance(acc, dict):
                continue
            uid = str(acc.get("uid", "")).strip()
            pw = str(acc.get("password", "")).strip()
            if uid and pw:
                out.append({"uid": uid, "password": pw})
        return out
    except Exception as e:
        print(f"[-] Lỗi đọc {path}: {e}")
        return []


def load_done() -> List[Dict]:
    """Đọc accounts_done.json → list dict {uid, password}."""
    if not os.path.exists(DONE_FILE):
        return []
    try:
        with open(DONE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        return [a for a in data if isinstance(a, dict) and a.get("uid")]
    except Exception as e:
        print(f"[-] Lỗi đọc {DONE_FILE}: {e}")
        return []


def load_done_uids() -> Set[str]:
    """Đọc accounts_done.json → set uid đã done."""
    return {str(a.get("uid", "")).strip() for a in load_done() if a.get("uid")}


def append_done(uid: str, password: str):
    """
    Append acc vào accounts_done.json — CHỈ uid + password.
    KHÔNG sửa accounts.json.
    Nếu uid đã có trong done → bỏ qua (không thêm trùng).
    """
    existing = load_done()

    # Kiểm tra trùng
    for e in existing:
        if str(e.get("uid")) == str(uid):
            return  # đã có → bỏ qua

    # Append entry mới — chỉ uid + password
    existing.append({
        "uid": uid,
        "password": password,
    })

    try:
        tmp = DONE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DONE_FILE)
    except Exception as e:
        print(f"[-] Lỗi ghi {DONE_FILE}: {e}")


def build_queue() -> List[Dict]:
    """
    Queue = accounts.json trừ acc đã có trong accounts_done.json.
    KHÔNG sửa accounts.json.
    """
    all_acc = load_accounts(ACC_FILE)
    done_uids = load_done_uids()

    queue = []
    skipped = 0
    for acc in all_acc:
        if str(acc["uid"]) in done_uids:
            skipped += 1
            continue
        queue.append(acc)

    print(f"[i] {ACC_FILE}: {len(all_acc)} acc")
    print(f"[i] {DONE_FILE}: {len(done_uids)} acc đã done")
    print(f"[i] Đã bỏ qua {skipped} acc")
    print(f"[i] Queue cần chạy: {len(queue)} acc")
    return queue


def verify_all_done() -> bool:
    """
    Kiểm tra: mọi UID trong accounts.json đều có trong accounts_done.json.
    Trả True nếu tất cả OK.
    """
    all_acc = load_accounts(ACC_FILE)
    done_uids = load_done_uids()

    if not all_acc:
        print(f"[!] {ACC_FILE} rỗng")
        return False

    missing = [str(a["uid"]) for a in all_acc if str(a["uid"]) not in done_uids]

    total = len(all_acc)
    done_count = total - len(missing)

    print("=" * 62)
    print(f"  VERIFY: {done_count}/{total} acc đã done")
    if missing:
        print(f"  - Chưa có trong {DONE_FILE}: {len(missing)} acc")
        for u in missing[:10]:
            print(f"      {u}")
        if len(missing) > 10:
            print(f"      ... và {len(missing) - 10} acc khác")
    print("=" * 62)

    return len(missing) == 0


# ============ WORKER ============
class AccountWorker:
    def __init__(self, uid: str, password: str, slot: int):
        self.uid = uid
        self.password = password
        self.slot = slot
        self.task: Optional[asyncio.Task] = None
        self.monitor_task: Optional[asyncio.Task] = None
        self.done = False
        self.reason = ""
        self.final_level = 1
        self.stop_event = asyncio.Event()
        self.bot_task: Optional[asyncio.Task] = None
        self._cancelled = False

    def log(self, msg: str):
        print(f"[Slot {self.slot} | {self.uid}] {msg}", flush=True)

    async def _monitor_level(self, account_data: dict):
        while not self._cancelled and not self.stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=LEVEL_CHECK_INTERVAL
                )
                break
            except asyncio.TimeoutError:
                pass

            if self._cancelled:
                break

            try:
                res = await Main.send_getlogin(
                    account_data["login_payload_data"],
                    account_data["server_url"],
                    account_data["token"],
                    account_data["release_version"],
                )
                if res:
                    _, dict_res = res
                    level = int(Main.get_proto_field(dict_res, 6, 1))
                    account_data["level"] = level
                    self.final_level = level
                    self.log(f"level={level}")

                    if level >= TARGET_LEVEL:
                        self.log(f"🎯 ĐỦ TARGET lv{level} — DỪNG NGAY")
                        self.done = True
                        self.reason = f"lv{level}"
                        self.stop_event.set()
                        if self.bot_task and not self.bot_task.done():
                            self.bot_task.cancel()
                        break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log(f"check level error: {e}")

    async def run(self):
        if self._cancelled:
            return

        try:
            self.log("login...")
            account_data = await Main.process_account_uid_pass(self.uid, self.password)
            if not account_data:
                self.log("login FAIL")
                return

            if self._cancelled:
                return

            acc_id = str(account_data.get("account_id", self.uid))
            level = int(account_data.get("level", 1))
            self.final_level = level
            self.log(f"OK | acc_id={acc_id} | lv={level}")

            if level >= TARGET_LEVEL:
                self.log(f"đã đủ lv{level} → skip")
                self.done = True
                self.reason = f"already_lv{level}"
                return

            self.log("farm...")
            self.monitor_task = asyncio.create_task(self._monitor_level(account_data))
            self.bot_task = asyncio.create_task(
                Main.run_account_worker(account_data, self.uid)
            )

            done, pending = await asyncio.wait(
                [self.bot_task, self.monitor_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

            for t in done:
                try:
                    t.result()
                except (asyncio.CancelledError, Exception):
                    pass

            if self.done:
                self.log(f"DỪNG — {self.reason}")
            else:
                self.log("bot task kết thúc, chưa đủ level")

        except asyncio.CancelledError:
            self.log("cancel")
            self._cancelled = True
            raise
        except Exception as e:
            self.log(f"error: {e}")


# ============ RUNNER ============
async def runner():
    if start_web_dashboard:
        try:
            await start_web_dashboard(host="0.0.0.0", port=20335)
            print(f"[+] Web Dashboard: http://localhost:20335")
        except Exception as e:
            print(f"[!] Dashboard error: {e}")

    print("=" * 62)
    print(f"  FF Auto Level | {MAX_CONCURRENT} acc song song | target lv {TARGET_LEVEL}")
    print("=" * 62)

    if verify_all_done():
        print("[✓] Tất cả acc đã đủ target — không cần chạy")
        return

    queue = build_queue()
    if not queue:
        print(f"[-] Không có acc nào cần chạy")
        verify_all_done()
        return

    running: List[AccountWorker] = []
    slot_counter = 0
    done_count = 0
    total_to_run = len(queue)

    def new_slot() -> int:
        nonlocal slot_counter
        slot_counter += 1
        return slot_counter

    def launch(uid: str, pw: str) -> AccountWorker:
        w = AccountWorker(uid, pw, new_slot())
        w.task = asyncio.create_task(w.run())
        running.append(w)
        msg = f"[Slot {w.slot}] Launch | {uid} | running: {len(running)}/{MAX_CONCURRENT}"
        print(f"[+] {msg}", flush=True)
        if bot_state:
            try:
                bot_state.log(msg, "info")
                bot_state.update_status(uid, "CONNECTING", 0)
            except Exception:
                pass
        return w

    def fill_slots():
        while len(running) < MAX_CONCURRENT and queue:
            acc = queue.pop(0)
            launch(acc["uid"], acc["password"])

    fill_slots()

    if not running:
        print("[-] Không có acc nào để chạy")
        return

    last_verify = time.time()

    while True:
        done_workers = [w for w in running if w.task and w.task.done()]

        now = time.time()
        if now - last_verify > VERIFY_INTERVAL:
            verify_all_done()
            last_verify = now

        if not done_workers:
            await asyncio.sleep(2)
            continue

        for w in done_workers:
            running.remove(w)

            try:
                w.task.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[-] Slot {w.slot} ({w.uid}) crashed: {e}", flush=True)

            for t in (w.bot_task, w.monitor_task):
                if t and not t.done():
                    t.cancel()
            if w.bot_task:
                try:
                    await w.bot_task
                except (asyncio.CancelledError, Exception):
                    pass
            if w.monitor_task:
                try:
                    await w.monitor_task
                except (asyncio.CancelledError, Exception):
                    pass

            if w.done:
                done_count += 1
                # 🔥 Append UID + password vào accounts_done.json
                # KHÔNG sửa accounts.json
                append_done(w.uid, w.password)

                msg = f"[DONE] {w.uid} | Lvl {w.final_level} | {w.reason} | {done_count}/{total_to_run}"
                print(f"[✓] {msg}", flush=True)
                if bot_state:
                    try:
                        bot_state.log(msg, "success")
                        bot_state.update_status(w.uid, "DONE", 0)
                        bot_state.update_exp(w.uid, w.final_level, w.final_level)
                    except Exception as e:
                        print(f"[!] bot_state error: {e}")

                # Next acc kế tiếp
                if queue:
                    acc = queue.pop(0)
                    launch(acc["uid"], acc["password"])
                    print(f"    → còn {len(queue)} acc trong queue", flush=True)
                else:
                    print(f"    → queue rỗng, còn {len(running)} slot đang chạy", flush=True)
            else:
                msg = f"[RETRY] {w.uid} — retry sau {LOGIN_RETRY_DELAY}s"
                print(f"[!] {msg}", flush=True)
                if bot_state:
                    try:
                        bot_state.log(msg, "warning")
                        bot_state.update_status(w.uid, "RETRY", 0)
                    except Exception:
                        pass
                await asyncio.sleep(LOGIN_RETRY_DELAY)
                if w._cancelled:
                    continue
                w._cancelled = False
                w.done = False
                w.stop_event = asyncio.Event()
                w.task = asyncio.create_task(w.run())
                running.append(w)

        # Verify điều kiện dừng
        if not queue and not running:
            print()
            print("[i] Queue và running đều rỗng → verify lần cuối...")
            if verify_all_done():
                print("[✓] OK — tất cả acc trong accounts.json đều có trong accounts_done.json")
                break
            else:
                print("[!] Vẫn còn acc chưa done → rebuild queue")
                queue = build_queue()
                if not queue:
                    print("[-] Không tìm được acc nào cần chạy → dừng")
                    break
                fill_slots()
                if not running:
                    break

    # Verify cuối
    print()
    if verify_all_done():
        print(f"[✓] HOÀN TẤT — tất cả acc trong {ACC_FILE} đã có trong {DONE_FILE}")
    else:
        print(f"[!] KẾT THÚC nhưng còn acc chưa done")

    print(f"  - Lần chạy này done: {done_count}/{total_to_run}")
    print(f"  - {ACC_FILE}: {len(load_accounts(ACC_FILE))} acc (KHÔNG ĐỔI)")
    print(f"  - {DONE_FILE}: {len(load_done())} acc")
    print("=" * 62)


if __name__ == "__main__":
    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        print("\n[STOP] Ctrl+C")