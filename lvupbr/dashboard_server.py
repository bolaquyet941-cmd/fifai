# dashboard_server.py
import time
import asyncio
from aiohttp import web

class BotState:
    def __init__(self):
        self.accounts = {}              # {uid: {...}}
        self.account_credentials = {}   # {uid: account_data}
        self.account_workers = {}       # {uid: task}
        self.logs = []
        self.total_matches = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.refresh_callbacks = {}
        self._lock = asyncio.Lock()

    def log(self, message, level="info"):
        self.logs.append({
            "time": time.strftime("%H:%M:%S"),
            "message": str(message),
            "level": level,
        })
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]

    def register_account(self, uid, nickname=None, region=None, level=1, exp=0, likes=0):
        uid = str(uid)
        if uid not in self.accounts:
            self.accounts[uid] = {
                "uid": uid,
                "nickname": nickname or f"Player_{uid}",
                "region": region or "--",
                "level": level,
                "initial_exp": exp,          # 🔥 EXP ban đầu
                "current_exp": exp,
                "gained_exp": 0,
                "matches_played": 0,
                "active_matches": 0,
                "status": "ONLINE",
                "last_match_time": "-",
                "last_updated": time.strftime("%H:%M:%S"),
            }
        else:
            # Cập nhật nếu đã có
            if nickname:
                self.accounts[uid]["nickname"] = nickname
            if region:
                self.accounts[uid]["region"] = region

    def update_status(self, uid, status, active_matches=None):
        uid = str(uid)
        if uid not in self.accounts:
            self.register_account(uid)
        self.accounts[uid]["status"] = status
        if active_matches is not None:
            self.accounts[uid]["active_matches"] = active_matches
        self.accounts[uid]["last_updated"] = time.strftime("%H:%M:%S")

    def update_exp(self, uid, exp, level):
        uid = str(uid)
        if uid not in self.accounts:
            self.register_account(uid, level=level, exp=exp)
            return
        old_exp = self.accounts[uid].get("current_exp", 0)
        delta = max(0, exp - old_exp)
        self.accounts[uid]["gained_exp"] += delta
        self.total_gained_exp += delta
        self.accounts[uid]["current_exp"] = exp
        self.accounts[uid]["level"] = level
        self.accounts[uid]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid):
        uid = str(uid)
        if uid not in self.accounts:
            self.register_account(uid)
        self.accounts[uid]["matches_played"] += 1
        self.accounts[uid]["last_match_time"] = time.strftime("%H:%M:%S")
        self.total_matches += 1

    @property
    def uptime(self):
        return int(time.time() - self.start_time)

    def get_stats(self):
        return {
            "total_accounts": len(self.accounts),
            "total_matches": self.total_matches,
            "total_gained_exp": self.total_gained_exp,
            "uptime": self.uptime,
            "accounts": list(self.accounts.values()),
            "logs": self.logs[-150:],
        }


bot_state = BotState()


# ============ API HANDLERS ============
async def handle_stats(request):
    return web.json_response(bot_state.get_stats())


async def handle_add_account(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)

    uid = str(data.get("uid", "")).strip()
    password = str(data.get("password", "")).strip()
    token = str(data.get("token", "")).strip()

    # Ghi vào accounts.json
    import json, os
    ACC_FILE = "accounts.json"
    accounts = []
    if os.path.exists(ACC_FILE):
        try:
            with open(ACC_FILE, "r", encoding="utf-8") as f:
                accounts = json.load(f)
        except Exception:
            accounts = []

    if uid and password:
        accounts.append({"uid": uid, "password": password})
    elif token:
        accounts.append({"token": token})
    else:
        return web.json_response({"status": "error", "error": "missing uid/password"})

    with open(ACC_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

    bot_state.log(f"New account added: {uid or 'token'}", "success")

    # Gọi callback để bot spawn worker
    cb = bot_state.refresh_callbacks.get("on_account_added")
    if cb:
        try:
            await cb(data)
        except Exception as e:
            bot_state.log(f"callback error: {e}", "error")

    return web.json_response({"status": "ok"})


async def handle_delete_account(request):
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)

    if not uid:
        return web.json_response({"status": "error", "error": "missing uid"}, status=400)

    import json, os
    ACC_FILE = "accounts.json"
    accounts = []
    if os.path.exists(ACC_FILE):
        try:
            with open(ACC_FILE, "r", encoding="utf-8") as f:
                accounts = json.load(f)
        except Exception:
            accounts = []
    
    # Xóa acc khỏi accounts.json — match cả uid gốc lẫn acc_id
    new_accounts = []
    removed = 0
    for a in accounts:
        a_uid = str(a.get("uid", "")).strip()
        if a_uid == uid:
            removed += 1
            continue
        new_accounts.append(a)
    
    with open(ACC_FILE, "w", encoding="utf-8") as f:
        json.dump(new_accounts, f, indent=2, ensure_ascii=False)

    # 🔥 Cancel worker — tìm trong cả account_workers theo uid
    keys_to_remove = []
    for key in list(bot_state.account_workers.keys()):
        # Match nếu key trùng uid hoặc nếu account trong bot_state.accounts[key].uid == uid
        acc_info = bot_state.accounts.get(key)
        if key == uid or (acc_info and str(acc_info.get("uid")) == uid):
            keys_to_remove.append(key)
    
    # Cũng cần tìm theo account_credentials (chứa cả UID gốc và acc_id)
    for key in list(bot_state.account_credentials.keys()):
        cred = bot_state.account_credentials.get(key)
        if not cred:
            continue
        acc_id = str(cred.get("account_id", ""))
        auth_uid = str(cred.get("auth_uid", ""))
        if acc_id == uid or auth_uid == uid:
            # Cancel worker tương ứng
            for wk in [acc_id, auth_uid]:
                if wk and wk in bot_state.account_workers:
                    task = bot_state.account_workers[wk]
                    try:
                        task.cancel()
                    except Exception:
                        pass
                    keys_to_remove.append(wk)
    
    # Xóa worker khỏi dict
    for k in set(keys_to_remove):
        bot_state.account_workers.pop(k, None)
    
    # Xóa account khỏi bot_state.accounts (cả uid gốc lẫn acc_id)
    for key in list(bot_state.accounts.keys()):
        acc_info = bot_state.accounts[key]
        if key == uid or str(acc_info.get("uid")) == uid:
            del bot_state.accounts[key]

    bot_state.log(f"Account removed: {uid} (removed={removed})", "warning")
    return web.json_response({"status": "ok", "removed": removed})


async def handle_refresh_account(request):
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
    except Exception:
        return web.json_response({"status": "error"}, status=400)

    cb = bot_state.refresh_callbacks.get("on_refresh_account")
    if cb:
        try:
            await cb(uid)
        except Exception as e:
            bot_state.log(f"refresh error: {e}", "error")

    return web.json_response({"status": "ok"})


async def handle_index(request):
    import os
    path = os.path.join("templates", "index.html")
    if not os.path.exists(path):
        return web.Response(text="templates/index.html not found!", status=404)
    return web.FileResponse(path)


def create_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    # Static files (cho CSS/JS nếu cần)
    return app


async def start_web_dashboard(host="0.0.0.0", port=20335):
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"[+] Dashboard running at http://{host}:{port}")
    return runner