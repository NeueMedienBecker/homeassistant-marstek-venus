#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Marstek Venus Steuerung - Einrichtung / Setup
=============================================

Wird von install.sh im Home-Assistant-Terminal-Add-on aufgerufen und erledigt alles:
  * Add-ons Mosquitto (MQTT) und AppDaemon pruefen / installieren / einrichten
  * Python-Pakete fuer AppDaemon eintragen (pymodbus, paho-mqtt)
  * Skript-Dateien kopieren (Neuinstallation oder Update mit Uebernahme der Einstellungen)
  * Batterien und Netz-Zaehler abfragen (mit automatischer Suche im Netzwerk)
  * MQTT-Integration einrichten, Dashboard "Marstek" anlegen
  * AppDaemon neu starten und pruefen, ob die Batterien antworten

Runs inside the Home Assistant terminal add-on (needs SUPERVISOR_TOKEN).
Only the Python standard library is used.
"""

import argparse
import ast
import atexit
import base64
import json
import os
import re
import secrets
import shutil
import socket
import struct
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_SRC = os.path.join(REPO_DIR, "apps", "marstek_venus")
DASH_TEMPLATE = os.path.join(REPO_DIR, "dashboard", "marstek_dashboard.json")
VERSION_FILE = os.path.join(REPO_DIR, "VERSION")

SUP = os.environ.get("MARSTEK_SUPERVISOR_URL", "http://supervisor")
TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
ADDON_APPDAEMON = "a0d7b954_appdaemon"
ADDON_MOSQUITTO = "core_mosquitto"
APP_FOLDER_NAME = "marstek_venus"
APP_NAME = "marstek_venus"
DASH_URL = "marstek-venus"
DASH_URL_OLD = ("dashboard-marstek",)
MQTT_USER = "marstek"
FOLD_ROW_URL = "https://raw.githubusercontent.com/thomasloven/lovelace-fold-entity-row/v.2.3.2/fold-entity-row.js"
FOLD_ROW_REL = os.path.join("www", "marstek-venus", "fold-entity-row.js")
GRID_MISSING = "sensor.netz_zaehler_fehlt"
# AppDaemon-Konfigurationsordner, wie ihn das Terminal-Add-on sieht (neu: /app_configs, bis 2026.6: /addon_configs)
AD_DIRS = ("/app_configs/a0d7b954_appdaemon", "/addon_configs/a0d7b954_appdaemon", "/config/appdaemon",
           "/homeassistant/appdaemon")
FOLD_ROW_RES = "/local/marstek-venus/fold-entity-row.js?v=2.3.2"
PY_PACKAGES = ["pymodbus==3.9.2", "paho-mqtt"]

# Dateien, die dem Nutzer gehoeren und bei einem Update nie ueberschrieben werden
USER_FILES = ("pv_mqtt_login.json", "pv_mqtt_overrides.json", "pv_batteries.json")
# Programmdateien (werden bei jedem Update ersetzt)
PROGRAM_FILES = ("pv_control.py", "pv_mqtt.py")

# Einstellungen, die bei einem Update aus der alten pv_config.py uebernommen werden,
# wenn keine pv_config.default der alten Version vorliegt (Erst-Update alter Installationen).
DASH_KEYS = (
    "SOC_SAFETY_ENABLE", "SOC_SAFETY_MIN_SOC", "SOC_SAFETY_TARGET_SOC", "SOC_SAFETY_CHARGE_W",
    "GRID_DEADBAND_W", "TOLERANCE_W", "ENABLE_BMS_CARE", "FULL_CHARGE_SOC", "FULL_CHARGE_INTERVAL_DAYS",
    "CONTROL_ENABLE", "SOC_SAFETY_REQUIRE_PV", "SOC_SAFETY_PV_WAIT_H", "BMS_CARE_REQUIRE_PV",
    "BMS_CARE_PV_WAIT_H", "PV_CHARGE_ALLOW_W", "PV_SURPLUS_MIN_TIME", "STABLE_TIMER_S",
    "GRID_STOP_AFTER_S", "PV_BUCKET_ENABLE", "PV_BUCKET_1_TOTAL_W", "PV_BUCKET_2_TOTAL_W",
    "PV_MIN_PER_BAT_W", "PV_ROTATE_ENABLE", "READ_ONLY",
)
USER_KEYS = (
    "BATTERY_CONFIG", "POWER_SENSOR", "HA_ENTITY_ID_MAP", "_BATTERY_META_OVERRIDE", "AC_SIGN_INVERT",
    "COMM_MODE_FALLBACK_ENABLE", "HA_EXPORT_PREFIX", "SOC_SAFETY_AFTER_HOURS", "BMS_FORCE_CHARGE_W",
    "MAX_BMS_BATTERIES_PER_DAY", "FULL_CHARGE_MIN_HOLD_S", "ALLOW_DISCHARGE_AFTER_S",
)
COMM_CHOICES = ("Modbus", "API", "Modbus, sonst API", "API, sonst Modbus")

LANG = "de"
DRY = False
_BRIDGE_DIR = None          # Austauschordner der AppDaemon-Hilfs-App (wenn die HA-API nicht direkt geht)
NEED_HA_RESTART = False     # www-Ordner neu angelegt -> /local erst nach Neustart verfuegbar
BRIDGE_APP = "marstek_bridge"
BRIDGE_BLOCK = "\n" + BRIDGE_APP + ":\n  module: " + BRIDGE_APP + "\n  class: MarstekBridge\n"
BRIDGE_CODE = '''# -*- coding: utf-8 -*-
# Marstek Venus - Hilfs-App fuer den Installer (wird nach der Einrichtung automatisch entfernt).
# Das Terminal-Add-on darf die Home-Assistant-API nicht direkt benutzen; diese App fuehrt die
# Anfragen des Installers aus. Austausch ueber Dateien im Unterordner "io".
import importlib.machinery
import importlib.util
import json
import os
import threading
import time

from appdaemon.plugins.hass.hassapi import Hass

_DIR = os.path.dirname(os.path.abspath(__file__))
_IO = os.path.join(_DIR, "io")


def _load_setup():
    loader = importlib.machinery.SourceFileLoader("marstek_ha_setup", os.path.join(_DIR, "ha_setup.job"))
    spec = importlib.util.spec_from_loader("marstek_ha_setup", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


class MarstekBridge(Hass):
    def initialize(self):
        os.makedirs(_IO, exist_ok=True)
        self._stop = False
        threading.Thread(target=self._loop, name="marstek_bridge", daemon=True).start()
        self.log("Marstek-Installer: Hilfs-App aktiv")

    def terminate(self):
        self._stop = True

    def _loop(self):
        mod = None
        while not self._stop:
            if not os.path.isdir(_IO):
                return  # Installer ist fertig und hat die Hilfs-App entfernt
            try:
                with open(os.path.join(_IO, "alive"), "w") as f:
                    f.write(str(time.time()))
                for fn in sorted(os.listdir(_IO)):
                    if not (fn.startswith("req_") and fn.endswith(".json")):
                        continue
                    path = os.path.join(_IO, fn)
                    try:
                        with open(path, encoding="utf-8") as f:
                            req = json.load(f)
                    except ValueError:
                        continue
                    os.remove(path)
                    if mod is None:
                        mod = _load_setup()
                    out = self._do(mod, req)
                    rp = os.path.join(_IO, "resp_" + fn[4:])
                    with open(rp + ".tmp", "w", encoding="utf-8") as f:
                        json.dump(out, f)
                    os.replace(rp + ".tmp", rp)
            except Exception as e:  # noqa: BLE001
                self.log("Hilfs-App: %r" % (e,), level="WARNING")
            time.sleep(0.4)

    def _do(self, mod, req):
        try:
            if req.get("kind") == "ws":
                ws = mod.WS()
                try:
                    return {"ok": True, "result": ws.call(req["type"], **(req.get("data") or {}))}
                finally:
                    ws.close()
            return {"ok": True, "result": mod.http(req["method"], mod.SUP + "/core/api" + req["path"],
                                                   req.get("data"), timeout=req.get("timeout", 30))}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)}
'''


def L(de, en):
    """Text in der gewaehlten Sprache / text in the selected language."""
    return de if LANG == "de" else en


# ---------------------------------------------------------------------------
# Ausgabe / Eingabe
# ---------------------------------------------------------------------------
C_OK, C_WARN, C_ERR, C_HEAD, C_DIM, C_RST = "\033[32m", "\033[33m", "\033[31m", "\033[1;36m", "\033[2m", "\033[0m"
if not sys.stdout.isatty() and not os.environ.get("FORCE_COLOR"):
    C_OK = C_WARN = C_ERR = C_HEAD = C_DIM = C_RST = ""


def say(msg=""):
    print(msg, flush=True)


def head(msg):
    say()
    say(f"{C_HEAD}== {msg} =={C_RST}")


def ok(msg):
    say(f"  {C_OK}✔{C_RST} {msg}")


def warn(msg):
    say(f"  {C_WARN}!{C_RST} {msg}")


def err(msg):
    say(f"  {C_ERR}✘{C_RST} {msg}")


def hint(msg):
    say(f"    {C_DIM}{msg}{C_RST}")


class Abort(Exception):
    pass


_TTY = None
AUTO_YES = False


def _tty():
    global _TTY
    if _TTY is None:
        try:
            _TTY = open("/dev/tty", "r")
        except OSError:
            _TTY = sys.stdin
    return _TTY


def ask(question, default=None):
    """Frage stellen; Enter uebernimmt den Vorschlag in [Klammern]."""
    if AUTO_YES and default is not None:
        say(f"  {question} [{default}]: {default}")
        return str(default)
    while True:
        sfx = f" [{default}]" if default not in (None, "") else ""
        print(f"  {question}{sfx}: ", end="", flush=True)
        line = _tty().readline()
        if not line:
            raise Abort(L("Eingabe beendet", "input closed"))
        line = line.strip()
        if line:
            return line
        if default is not None:
            return str(default)


def ask_yes(question, default=True):
    d = L("J/n", "Y/n") if default else L("j/N", "y/N")
    if AUTO_YES:
        say(f"  {question} ({d}): {L('ja', 'yes') if default else L('nein', 'no')}")
        return default
    while True:
        a = ask(f"{question} ({d})", "").strip().lower()
        if a == "":
            return default
        if a in ("j", "ja", "y", "yes"):
            return True
        if a in ("n", "nein", "no"):
            return False


def ask_int(question, default, lo, hi):
    while True:
        a = ask(question, default)
        try:
            v = int(float(a.replace(",", ".")))
            if lo <= v <= hi:
                return v
        except ValueError:
            pass
        warn(L(f"Bitte eine Zahl von {lo} bis {hi} eingeben.", f"Please enter a number from {lo} to {hi}."))


def ask_float(question, default, lo, hi):
    while True:
        a = ask(question, default)
        try:
            v = float(a.replace(",", "."))
            if lo <= v <= hi:
                return v
        except ValueError:
            pass
        warn(L(f"Bitte eine Zahl von {lo} bis {hi} eingeben.", f"Please enter a number from {lo} to {hi}."))


_IP_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")


def valid_ip(s):
    m = _IP_RE.match(str(s or "").strip())
    return bool(m) and all(0 <= int(x) <= 255 for x in m.groups())


def ask_ip(question, default=None):
    while True:
        a = ask(question, default).strip()
        if valid_ip(a):
            return a
        warn(L("Das ist keine gueltige IP-Adresse (Beispiel: 192.168.1.50).",
               "That is not a valid IP address (example: 192.168.1.50)."))


def choose(question, options, default=1):
    """options: Liste von Texten. Rueckgabe: Index (0-basiert)."""
    say(f"  {question}")
    for i, o in enumerate(options, 1):
        say(f"    {i}) {o}")
    return ask_int(L("Auswahl", "Choice"), default, 1, len(options)) - 1


# ---------------------------------------------------------------------------
# Supervisor- und Home-Assistant-API
# ---------------------------------------------------------------------------
class ApiError(Exception):
    pass


def http(method, url, data=None, timeout=30, raw=False):
    body = None
    headers = {"Authorization": f"Bearer {TOKEN}"}
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="replace")[:300]
        raise ApiError(f"{method} {url}: HTTP {e.code} {msg}")
    except (urllib.error.URLError, OSError) as e:
        raise ApiError(f"{method} {url}: {e}")
    if raw:
        return txt
    try:
        return json.loads(txt) if txt.strip() else None
    except ValueError:
        return txt


def sup(method, path, data=None, timeout=60):
    """Supervisor-API: liefert data-Feld."""
    r = http(method, SUP + path, data, timeout)
    if isinstance(r, dict) and r.get("result") == "error":
        raise ApiError(f"{path}: {r.get('message')}")
    return r.get("data") if isinstance(r, dict) else r


def bridge_call(req, timeout=90):
    """Anfrage an die AppDaemon-Hilfs-App (Datei rein, Antwort-Datei raus)."""
    rid = "%d_%s" % (int(time.time() * 1000), secrets.token_hex(3))
    tmp = os.path.join(_BRIDGE_DIR, ".req_" + rid + ".tmp")
    dst = os.path.join(_BRIDGE_DIR, "req_" + rid + ".json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(req, f)
    os.replace(tmp, dst)
    rp = os.path.join(_BRIDGE_DIR, "resp_" + rid + ".json")
    t0 = time.time()
    while time.time() - t0 < timeout:
        if os.path.exists(rp):
            try:
                with open(rp, encoding="utf-8") as f:
                    r = json.load(f)
            except ValueError:
                time.sleep(0.2)
                continue
            os.remove(rp)
            if r.get("ok"):
                return r.get("result")
            raise ApiError(r.get("error") or "Hilfs-App: Fehler")
        time.sleep(0.3)
    try:
        os.remove(dst)
    except OSError:
        pass
    raise ApiError(L("keine Antwort von der AppDaemon-Hilfs-App", "no answer from the AppDaemon helper app"))


def core(method, path, data=None, timeout=30):
    """Home-Assistant-REST-API ueber den Supervisor-Proxy (oder ueber die AppDaemon-Hilfs-App)."""
    if _BRIDGE_DIR:
        return bridge_call({"kind": "rest", "method": method, "path": path, "data": data, "timeout": timeout},
                           timeout + 60)
    return http(method, SUP + "/core/api" + path, data, timeout)


class WS:
    """Sehr kleiner WebSocket-Client fuer die Home-Assistant-API (nur Standardbibliothek)."""

    def __init__(self):
        self.bridge = bool(_BRIDGE_DIR)
        if self.bridge:
            return
        m = re.match(r"^https?://([^/:]+)(?::(\d+))?", SUP)
        host, port = m.group(1), int(m.group(2) or 80)
        self.s = socket.create_connection((host, port), timeout=30)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET /core/websocket HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
               f"Authorization: Bearer {TOKEN}\r\n\r\n")
        self.s.sendall(req.encode())
        self.buf = b""
        while b"\r\n\r\n" not in self.buf:
            chunk = self.s.recv(4096)
            if not chunk:
                raise ApiError("WebSocket: connection closed during handshake")
            self.buf += chunk
        hdr, self.buf = self.buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in hdr.split(b"\r\n")[0]:
            raise ApiError("WebSocket handshake failed: " + hdr.split(b"\r\n")[0].decode(errors="replace"))
        self.mid = 0
        first = self.recv()
        if first.get("type") == "auth_required":
            self.send({"type": "auth", "access_token": TOKEN})
            res = self.recv()
            if res.get("type") != "auth_ok":
                raise ApiError("WebSocket auth failed: " + str(res))

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.s.recv(65536)
            if not chunk:
                raise ApiError("WebSocket closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _frame(self, opcode, payload):
        hdr = bytes([0x80 | opcode])
        n = len(payload)
        if n < 126:
            hdr += bytes([0x80 | n])
        elif n < 65536:
            hdr += bytes([0x80 | 126]) + struct.pack("!H", n)
        else:
            hdr += bytes([0x80 | 127]) + struct.pack("!Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.s.sendall(hdr + mask + masked)

    def send(self, obj):
        self._frame(0x1, json.dumps(obj).encode("utf-8"))

    def recv(self):
        data = b""
        while True:
            b1, b2 = self._read(2)
            fin, op = b1 & 0x80, b1 & 0x0F
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack("!H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack("!Q", self._read(8))[0]
            mask = self._read(4) if b2 & 0x80 else None
            payload = self._read(n)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if op == 0x9:
                self._frame(0xA, payload)
                continue
            if op == 0x8:
                raise ApiError("WebSocket closed by server")
            if op in (0x1, 0x0, 0x2):
                data += payload
                if fin:
                    return json.loads(data.decode("utf-8"))

    def call(self, typ, **kw):
        if self.bridge:
            return bridge_call({"kind": "ws", "type": typ, "data": kw})
        self.mid += 1
        msg = {"id": self.mid, "type": typ}
        msg.update(kw)
        self.send(msg)
        while True:
            r = self.recv()
            if r.get("id") == self.mid and r.get("type") == "result":
                if not r.get("success"):
                    raise ApiError(f"{typ}: {r.get('error')}")
                return r.get("result")

    def close(self):
        if self.bridge:
            return
        try:
            self.s.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# pv_config.py: Werte lesen / setzen / zusammenfuehren
# ---------------------------------------------------------------------------
def _assign_nodes(text):
    out = {}
    for node in ast.parse(text).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            out[node.targets[0].id] = node
    return out


def config_values(text):
    vals = {}
    for name, node in _assign_nodes(text).items():
        try:
            vals[name] = ast.literal_eval(node.value)
        except (ValueError, SyntaxError, TypeError):
            pass
    return vals


def _lit(v, level=0):
    """Python-Literal im Stil der pv_config.py (doppelte Anfuehrungszeichen, 4 Leerzeichen Einzug)."""
    pad, end = "    " * (level + 1), "    " * level
    if isinstance(v, bool) or v is None:
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, tuple):
        inner = ", ".join(_lit(x, level + 1) for x in v)
        return "(" + inner + ("," if len(v) == 1 else "") + ")"
    if isinstance(v, (list, set)):
        items = [_lit(x, level + 1) for x in v]
        one = "[" + ", ".join(items) + "]"
        if len(one) <= 100 and not any(isinstance(x, (dict, list)) and x for x in v):
            return one
        return "[\n" + "".join(f"{pad}{x},\n" for x in items) + end + "]"
    if isinstance(v, dict):
        if not v:
            return "{}"
        items = [f"{_lit(k, level + 1)}: {_lit(x, level + 1)}" for k, x in v.items()]
        one = "{" + ", ".join(items) + "}"
        if len(one) <= 100 and not any(isinstance(x, (dict, list)) and x for x in v.values()):
            return one
        return "{\n" + "".join(f"{pad}{x},\n" for x in items) + end + "}"
    return repr(v)


def _fmt(name, value):
    return f"{name} = " + _lit(value)


def config_set(text, values, note=None):
    """Setzt Werte in pv_config.py (Kommentare und Aufbau bleiben erhalten)."""
    lines = text.split("\n")
    nodes = _assign_nodes(text)
    for name, node in sorted(((n, nd) for n, nd in nodes.items() if n in values),
                             key=lambda x: x[1].lineno, reverse=True):
        start, end = node.lineno - 1, node.end_lineno - 1
        tail = lines[end][node.end_col_offset:]
        new = _fmt(name, values[name]).split("\n")
        if tail.strip().startswith("#"):
            new[-1] += "  " + tail.strip()
        lines[start:end + 1] = new
    missing = [k for k in values if k not in nodes]
    if missing:
        lines.append("")
        lines.append("# " + (note or "Ergaenzt durch die Einrichtung"))
        for k in missing:
            lines.extend(_fmt(k, values[k]).split("\n"))
        lines.append("")
    out = "\n".join(lines)
    compile(out, "pv_config.py", "exec")
    return out


def is_user_key(k):
    return k in USER_KEYS or k in DASH_KEYS or bool(re.match(r"^BATTERY\d+_", k))


def config_merge(old_text, new_text, old_default_text=None):
    """Neue Vorlage + Einstellungen des Nutzers aus der alten Datei."""
    old = config_values(old_text)
    new = config_values(new_text)
    if old_default_text:
        base = config_values(old_default_text)
        carry = {k: v for k, v in old.items() if k not in base or base[k] != v}
    else:
        carry = {k: v for k, v in old.items() if is_user_key(k)}
    if isinstance(carry.get("POWER_SENSOR"), dict) and isinstance(new.get("POWER_SENSOR"), dict):
        merged = dict(new["POWER_SENSOR"])
        merged.update(carry["POWER_SENSOR"])
        carry["POWER_SENSOR"] = merged
    carry = {k: v for k, v in carry.items() if k not in new or new[k] != v}
    return config_set(new_text, carry, "Uebernommen aus der vorherigen Version"), carry


def batteries_from_config(text, app_dir=None):
    """Batterie-Namen aus BATTERY_CONFIG + pv_batteries.json (Dashboard-Verwaltung)."""
    bats = dict(config_values(text).get("BATTERY_CONFIG") or {})
    if app_dir:
        p = os.path.join(app_dir, "pv_batteries.json")
        if os.path.exists(p):
            try:
                with open(p, encoding="utf-8") as f:
                    bo = json.load(f)
                for k in bo.get("remove", []):
                    bats.pop(k, None)
                for k, v in (bo.get("add") or {}).items():
                    bats[k] = v
            except (OSError, ValueError):
                pass
    return bats


def bat_index(name):
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else 0


# ---------------------------------------------------------------------------
# Dashboard erzeugen
# ---------------------------------------------------------------------------
_NUM_RE = re.compile(r"marstekvenus_(\d+)_|battery(\d+)(?![0-9])|Venus (\d+)(?![0-9])")


def _bat_ids(obj):
    s = json.dumps(obj, ensure_ascii=False)
    return {int(next(g for g in m.groups() if g)) for m in _NUM_RE.finditer(s)}


def _renum(obj, src, dst):
    s = json.dumps(obj, ensure_ascii=False)
    s = re.sub(rf"marstekvenus_{src}_", f"marstekvenus_{dst}_", s)
    s = re.sub(rf"battery{src}(?![0-9])", f"battery{dst}", s)
    s = re.sub(rf"BATTERY{src}_", f"BATTERY{dst}_", s)
    s = re.sub(rf"Venus {src}(?![0-9])", f"Venus {dst}", s)
    return json.loads(s)


def _vis(n):
    return [{"condition": "numeric_state", "entity": f"sensor.marstekvenus_{n}_battery_soc", "above": -1}]


def _expand(items, kind, bats, slots):
    """Wiederholte Batterie-Elemente (Venus 1, Venus 2, ...) fuer die eigenen Batterien neu erzeugen."""
    info = [(_bat_ids(it) if isinstance(it, (dict, str)) else set()) for it in items]
    out, i = [], 0
    while i < len(items):
        j = i
        seen = []
        while j < len(items) and len(info[j]) == 1 and next(iter(info[j])) not in seen:
            seen.append(next(iter(info[j])))
            j += 1
        if j - i >= 2:
            run = items[i:j]
            ids = [next(iter(x)) for x in info[i:j]]
            proto_n = min(ids)
            proto = run[ids.index(proto_n)]
            cond_proto = None
            for it, n in zip(run, ids):
                if isinstance(it, dict) and it.get("type") == "conditional":
                    cond_proto = (it, n)
                    break
            if kind in ("cards", "sections"):
                for n in slots:
                    c = _renum(proto, proto_n, n)
                    if isinstance(c, dict):
                        c["visibility"] = _vis(n)
                    out.append(c)
            else:
                for n in bats:
                    out.append(_renum(proto, proto_n, n))
                if cond_proto:
                    for n in slots:
                        if n not in bats:
                            out.append(_renum(cond_proto[0], cond_proto[1], n))
            i = j
        else:
            out.append(_walk(items[i], bats, slots))
            i += 1
    return out


def _walk(obj, bats, slots):
    if isinstance(obj, dict):
        res = {}
        for k, v in obj.items():
            if isinstance(v, list) and k in ("views", "sections", "cards", "entities"):
                kind = "cards" if k == "cards" else ("sections" if k == "sections" else ("rows" if k == "entities" else k))
                res[k] = _expand(v, kind, bats, slots) if k != "views" else [_walk(x, bats, slots) for x in v]
            else:
                res[k] = _walk(v, bats, slots) if isinstance(v, (dict, list)) else v
        return res
    if isinstance(obj, list):
        return [_walk(x, bats, slots) for x in obj]
    return obj


def build_dashboard(template, bat_indexes, grid_sensor, entity_map=None, prefix="marstekvenus"):
    bats = sorted(set(bat_indexes)) or [1]
    slots = list(range(1, max(6, max(bats)) + 1))
    cfg = _walk(template, bats, slots)
    s = json.dumps(cfg, ensure_ascii=False)
    s = s.replace("__GRID_SENSOR__", grid_sensor or "sensor.unknown")
    for (idx, key), ent in (entity_map or {}).items():
        s = re.sub(rf"sensor\.{prefix}_{idx}_{key}(?![a-z0-9_])", ent, s)
    cfg = json.loads(s)
    if grid_sensor == GRID_MISSING:
        _grid_missing_cards(cfg)
    return cfg


GRID_MISSING_CARD = {"type": "markdown", "content": (
    "⚠️ **Netz-Zähler fehlt.** Die Steuerung braucht einen Sensor, der die aktuelle Leistung am Hausanschluss "
    "zeigt (z. B. Lesekopf am Stromzähler, Shelly 3EM). Sensor in Home Assistant einrichten, dann im Terminal "
    "den Installer mit der Option --reconfigure ausführen und den Sensor auswählen.")}


def _grid_missing_cards(o):
    """Kachel / Zeile des fehlenden Zaehlers durch einen Hinweis ersetzen."""
    if isinstance(o, dict):
        for k, v in list(o.items()):
            if isinstance(v, list):
                new = []
                for it in v:
                    if isinstance(it, dict) and it.get("entity") == GRID_MISSING:
                        if k == "cards":
                            new.append(json.loads(json.dumps(GRID_MISSING_CARD)))
                        elif k == "entities":
                            new.append({"type": "section", "label": "Netz-Zähler fehlt (Installer mit --reconfigure)"})
                        else:
                            new.append(it)
                    elif isinstance(it, str) and it == GRID_MISSING and k == "entities":
                        new.append({"type": "section", "label": "Netz-Zähler fehlt (Installer mit --reconfigure)"})
                    else:
                        new.append(it)
                o[k] = new
            _grid_missing_cards(o[k])
    elif isinstance(o, list):
        for it in o:
            _grid_missing_cards(it)


# ---------------------------------------------------------------------------
# Add-ons
# ---------------------------------------------------------------------------
def addon_info(slug):
    try:
        return sup("GET", f"/addons/{slug}/info")
    except ApiError:
        return None


def wait_addon_state(slug, want="started", timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        info = addon_info(slug) or {}
        if info.get("state") == want:
            return True
        time.sleep(3)
    return False


def ensure_addon(slug, name):
    info = addon_info(slug)
    if not info or not info.get("version"):
        say(L(f"  {name} wird installiert (das kann einige Minuten dauern) ...",
              f"  Installing {name} (this can take a few minutes) ..."))
        if DRY:
            ok(L("(Testlauf) Installation uebersprungen", "(dry run) installation skipped"))
            return {}
        try:
            sup("POST", f"/store/addons/{slug}/install", timeout=900)
        except ApiError:
            sup("POST", f"/addons/{slug}/install", timeout=900)
        info = addon_info(slug) or {}
        ok(L(f"{name} installiert", f"{name} installed"))
    else:
        ok(L(f"{name} ist installiert (Version {info.get('version')})",
             f"{name} is installed (version {info.get('version')})"))
    if not DRY:
        if not info.get("boot") == "auto":
            try:
                sup("POST", f"/addons/{slug}/options", {"boot": "auto"})
            except ApiError:
                pass
        try:
            sup("POST", f"/addons/{slug}/options", {"watchdog": True})
        except ApiError:
            pass
    return info


def start_or_restart(slug, name, restart=False):
    if DRY:
        ok(L(f"(Testlauf) {name} wuerde gestartet", f"(dry run) {name} would be started"))
        return True
    info = addon_info(slug) or {}
    if info.get("state") == "started" and restart:
        say(L(f"  {name} wird neu gestartet ...", f"  Restarting {name} ..."))
        sup("POST", f"/addons/{slug}/restart", timeout=300)
    elif info.get("state") != "started":
        say(L(f"  {name} wird gestartet ...", f"  Starting {name} ..."))
        sup("POST", f"/addons/{slug}/start", timeout=300)
    if wait_addon_state(slug):
        ok(L(f"{name} laeuft", f"{name} is running"))
        return True
    err(L(f"{name} startet nicht - bitte das Protokoll des Add-ons ansehen.",
          f"{name} does not start - please check the add-on log."))
    return False


def setup_mosquitto(login):
    info = ensure_addon(ADDON_MOSQUITTO, "Mosquitto broker")
    opts = dict((info or {}).get("options") or {})
    logins = list(opts.get("logins") or [])
    mine = [x for x in logins if x.get("username") == login["user"]]
    changed = False
    if not mine:
        logins.append({"username": login["user"], "password": login["password"]})
        changed = True
    elif mine[0].get("password") != login["password"]:
        mine[0]["password"] = login["password"]
        changed = True
    if changed and not DRY:
        opts["logins"] = logins
        sup("POST", f"/addons/{ADDON_MOSQUITTO}/options", {"options": opts})
        ok(L(f"MQTT-Benutzer '{login['user']}' eingerichtet", f"MQTT user '{login['user']}' configured"))
        start_or_restart(ADDON_MOSQUITTO, "Mosquitto", restart=True)
    elif changed:
        ok(L(f"(Testlauf) MQTT-Benutzer '{login['user']}' wuerde eingerichtet",
             f"(dry run) MQTT user '{login['user']}' would be configured"))
    else:
        ok(L(f"MQTT-Benutzer '{login['user']}' vorhanden", f"MQTT user '{login['user']}' present"))
        start_or_restart(ADDON_MOSQUITTO, "Mosquitto")


def setup_appdaemon():
    info = ensure_addon(ADDON_APPDAEMON, "AppDaemon")
    opts = dict((info or {}).get("options") or {})
    pk = list(opts.get("python_packages") or [])
    names = {re.split(r"[<>=!~ \[]", p, maxsplit=1)[0].lower() for p in pk}
    changed = False
    for p in PY_PACKAGES:
        n = re.split(r"[<>=!~ \[]", p, maxsplit=1)[0].lower()
        if n not in names:
            pk.append(p)
            changed = True
    if changed and not DRY:
        opts["python_packages"] = pk
        opts.setdefault("system_packages", [])
        opts.setdefault("init_commands", [])
        sup("POST", f"/addons/{ADDON_APPDAEMON}/options", {"options": opts})
        ok(L("Python-Pakete fuer AppDaemon eingetragen: ", "Python packages for AppDaemon added: ") + ", ".join(pk))
    else:
        ok(L("Python-Pakete fuer AppDaemon vorhanden", "Python packages for AppDaemon present"))
    # Ein frisch installiertes AppDaemon legt seinen Konfigurationsordner erst beim ersten Start an
    if not DRY and not _ad_config_exists():
        start_or_restart(ADDON_APPDAEMON, "AppDaemon")
        for _ in range(60):
            if _ad_config_exists():
                break
            time.sleep(3)


def ha_config_dir():
    """Konfigurationsordner von Home Assistant im Terminal-Add-on (/config, neuere Versionen /homeassistant)."""
    for p in ("/config", "/homeassistant"):
        if os.path.exists(os.path.join(p, "configuration.yaml")) or os.path.isdir(os.path.join(p, ".storage")):
            return p
    return "/config"


def _ad_config_exists():
    return any(os.path.exists(os.path.join(p, "appdaemon.yaml")) for p in AD_DIRS)


def _app_configs_base():
    for b in ("/app_configs", "/addon_configs"):
        if os.path.isdir(b):
            return b
    return None


def appdaemon_root():
    env = os.environ.get("MARSTEK_AD_ROOT")
    if env:
        os.makedirs(os.path.join(env, "apps"), exist_ok=True)
        return env
    for p in AD_DIRS:
        if os.path.isdir(os.path.join(p, "apps")) or os.path.exists(os.path.join(p, "appdaemon.yaml")):
            return p
    base = _app_configs_base()
    if base:
        p = os.path.join(base, "a0d7b954_appdaemon")
        os.makedirs(os.path.join(p, "apps"), exist_ok=True)
        return p
    raise Abort(L("Der AppDaemon-Konfigurationsordner wurde nicht gefunden. Bitte das Add-on einmal starten "
                  "und den Installer erneut aufrufen.",
                  "AppDaemon config folder not found. Please start the add-on once and run the installer again."))


def find_existing_app(apps_dir):
    for root, dirs, files in os.walk(apps_dir):
        dirs[:] = [d for d in dirs if d not in ("backup", "__pycache__")]
        if "pv_control.py" in files and "pv_config.py" in files:
            return root
    return None


def ensure_apps_yaml(apps_dir):
    p = os.path.join(apps_dir, "apps.yaml")
    txt = ""
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            txt = f.read()
    if re.search(r"^\s*module:\s*pv_control\s*$", txt, re.M):
        ok(L("apps.yaml enthaelt die App bereits", "apps.yaml already contains the app"))
        return
    block = (f"\n{APP_NAME}:\n  module: pv_control\n  class: PVControlApp\n")
    if not DRY:
        with open(p, "a", encoding="utf-8") as f:
            f.write(block)
    ok(L("(Testlauf) App wuerde in apps.yaml eingetragen", "(dry run) app would be added to apps.yaml") if DRY
       else L("App in apps.yaml eingetragen", "App added to apps.yaml"))


def remove_from_apps_yaml(apps_dir, module="pv_control"):
    p = os.path.join(apps_dir, "apps.yaml")
    if not os.path.exists(p):
        return
    with open(p, encoding="utf-8", newline="") as f:
        lines = f.read().split("\n")
    out, skip = [], False
    for i, ln in enumerate(lines):
        if re.match(r"^[A-Za-z0-9_]+:\s*$", ln):
            nxt = "\n".join(lines[i + 1:i + 4])
            skip = bool(re.search(r"module:\s*" + module + r"\b", nxt))
        if not skip:
            out.append(ln)
    with open(p, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(out))


# ---------------------------------------------------------------------------
# Zugriff auf die Home-Assistant-API (direkt oder ueber AppDaemon)
# ---------------------------------------------------------------------------
def _bridge_alive(io_dir, max_age=10):
    try:
        with open(os.path.join(io_dir, "alive")) as f:
            return time.time() - float(f.read().strip() or 0) < max_age
    except (OSError, ValueError):
        return False


def remove_core_bridge(apps_dir):
    global _BRIDGE_DIR
    _BRIDGE_DIR = None
    d = os.path.join(apps_dir, BRIDGE_APP)
    try:
        p = os.path.join(apps_dir, "apps.yaml")
        if os.path.exists(p):
            with open(p, encoding="utf-8", newline="") as f:
                txt = f.read()
            if BRIDGE_BLOCK in txt:
                with open(p, "w", encoding="utf-8", newline="") as f:
                    f.write(txt.replace(BRIDGE_BLOCK, "", 1))
            elif re.search(r"module:\s*" + BRIDGE_APP + r"\b", txt):
                remove_from_apps_yaml(apps_dir, BRIDGE_APP)
        shutil.rmtree(d, ignore_errors=True)
    except OSError:
        pass


def ensure_core_access(apps_dir):
    """Das Add-on 'Terminal & SSH' darf die Home-Assistant-API nicht benutzen (HTTP 401). Dann richtet der
    Installer voruebergehend eine kleine Hilfs-App in AppDaemon ein, die die Anfragen ausfuehrt."""
    global _BRIDGE_DIR
    if not os.environ.get("MARSTEK_FORCE_BRIDGE"):
        try:
            http("GET", SUP + "/core/api/config", timeout=20)
            return True
        except ApiError as e:
            if " 401" not in str(e) and " 403" not in str(e):
                warn(L(f"Home Assistant antwortet nicht ({e}).", f"Home Assistant does not answer ({e})."))
                return False
    if DRY:
        ok(L("(Testlauf) Zugriff auf Home Assistant wuerde ueber eine AppDaemon-Hilfs-App laufen",
             "(dry run) access to Home Assistant would go through an AppDaemon helper app"))
        return False
    say(L("  Dieses Terminal darf Home Assistant nicht direkt einrichten - das uebernimmt kurz eine Hilfs-App "
          "in AppDaemon ...", "  This terminal may not configure Home Assistant directly - an AppDaemon helper "
          "app does it for a moment ..."))
    d = os.path.join(apps_dir, BRIDGE_APP)
    io_dir = os.path.join(d, "io")
    os.makedirs(io_dir, exist_ok=True)
    for fn in os.listdir(io_dir):
        try:
            os.remove(os.path.join(io_dir, fn))
        except OSError:
            pass
    with open(os.path.join(d, BRIDGE_APP + ".py"), "w", encoding="utf-8") as f:
        f.write(BRIDGE_CODE)
    shutil.copyfile(os.path.abspath(__file__), os.path.join(d, "ha_setup.job"))
    p = os.path.join(apps_dir, "apps.yaml")
    txt = read(p) if os.path.exists(p) else ""
    if not re.search(r"module:\s*" + BRIDGE_APP + r"\b", txt):
        with open(p, "a", encoding="utf-8", newline="") as f:
            f.write(BRIDGE_BLOCK)
    atexit.register(remove_core_bridge, apps_dir)
    start_or_restart(ADDON_APPDAEMON, "AppDaemon")
    t0 = time.time()
    restarted = False
    while not _bridge_alive(io_dir):
        if not restarted and time.time() - t0 > 45:
            start_or_restart(ADDON_APPDAEMON, "AppDaemon", restart=True)
            restarted = True
        if time.time() - t0 > 240:
            warn(L("Die AppDaemon-Hilfs-App startet nicht - Home Assistant kann nicht automatisch eingerichtet werden.",
                   "The AppDaemon helper app does not start - Home Assistant cannot be configured automatically."))
            return False
        time.sleep(2)
    _BRIDGE_DIR = io_dir
    try:
        core("GET", "/config")
    except ApiError as e:
        warn(L(f"Hilfs-App: kein Zugriff auf Home Assistant ({e}).", f"Helper app: no access to Home Assistant ({e})."))
        _BRIDGE_DIR = None
        return False
    ok(L("Zugriff auf Home Assistant ueber AppDaemon", "Access to Home Assistant via AppDaemon"))
    return True


def _mqtt_flows():
    """Laufende Einrichtungs-Ablaeufe der MQTT-Integration (z. B. von Mosquitto entdeckt)."""
    try:
        ws = WS()
        try:
            flows = ws.call("config_entries/flow/progress") or []
        finally:
            ws.close()
    except (ApiError, OSError):
        try:
            flows = core("GET", "/config/config_entries/flow") or []
        except ApiError:
            flows = []
    return [f for f in flows if isinstance(f, dict) and f.get("handler") == "mqtt"]


# ---------------------------------------------------------------------------
# MQTT-Integration in Home Assistant
# ---------------------------------------------------------------------------
def ensure_mqtt_integration(login):
    try:
        entries = core("GET", "/config/config_entries/entry") or []
    except ApiError as e:
        warn(L(f"Integrationen konnten nicht gelesen werden ({e}).", f"Could not read integrations ({e})."))
        entries = []
    if any(e.get("domain") == "mqtt" for e in entries):
        ok(L("MQTT-Integration ist eingerichtet", "MQTT integration is set up"))
        return True
    if DRY:
        ok(L("(Testlauf) MQTT-Integration wuerde eingerichtet", "(dry run) MQTT integration would be set up"))
        return True
    flow = None
    try:
        for f in _mqtt_flows():
            flow = core("POST", f"/config/config_entries/flow/{f['flow_id']}", {})
            break
    except ApiError:
        flow = None
    try:
        if flow is None:
            flow = core("POST", "/config/config_entries/flow", {"handler": "mqtt", "show_advanced_options": False})
        for _ in range(5):
            typ = (flow or {}).get("type")
            if typ == "create_entry":
                ok(L("MQTT-Integration eingerichtet", "MQTT integration set up"))
                return True
            if typ == "abort":
                break
            fid = flow["flow_id"]
            if typ == "menu":
                opts = flow.get("menu_options") or []
                pick = "broker" if "broker" in opts else (opts[0] if opts else None)
                flow = core("POST", f"/config/config_entries/flow/{fid}", {"next_step_id": pick})
                continue
            data = {}
            for fld in flow.get("data_schema") or []:
                n = fld.get("name")
                if n == "broker":
                    data[n] = login["host"]
                elif n == "port":
                    data[n] = int(login["port"])
                elif n == "username":
                    data[n] = login["user"]
                elif n == "password":
                    data[n] = login["password"]
            flow = core("POST", f"/config/config_entries/flow/{fid}", data)
    except ApiError as e:
        warn(str(e))
    warn(L("Die MQTT-Integration konnte nicht automatisch eingerichtet werden.",
           "The MQTT integration could not be set up automatically."))
    hint(L("Bitte in Home Assistant: Einstellungen -> Geraete & Dienste -> MQTT -> Konfigurieren "
           "(Server core-mosquitto, Benutzer und Passwort siehe pv_mqtt_login.json).",
           "Please in Home Assistant: Settings -> Devices & services -> MQTT -> Configure "
           "(broker core-mosquitto, user and password see pv_mqtt_login.json)."))
    return False


# ---------------------------------------------------------------------------
# Netzwerk: Batterien suchen / pruefen
# ---------------------------------------------------------------------------
def _udp_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    for port in (30000, 0):
        try:
            s.bind(("0.0.0.0", port))
            return s
        except OSError:
            continue
    return s


def _local_subnets():
    """IPv4-Netze des Home-Assistant-Rechners (aus der Supervisor-API), jeweils als /24."""
    nets = []
    try:
        for itf in (sup("GET", "/network/info") or {}).get("interfaces", []):
            for a in ((itf.get("ipv4") or {}).get("address") or []):
                ip = a.split("/")[0]
                if valid_ip(ip) and not ip.startswith(("127.", "172.30.", "172.17.")):
                    base = ".".join(ip.split(".")[:3])
                    if base not in nets:
                        nets.append(base)
    except ApiError:
        pass
    return nets


def _own_ips():
    ips = set()
    try:
        for itf in (sup("GET", "/network/info") or {}).get("interfaces", []):
            for a in ((itf.get("ipv4") or {}).get("address") or []):
                ips.add(a.split("/")[0])
    except ApiError:
        pass
    return ips


def discover_marstek(rounds=3):
    """Sucht Marstek-Batterien mit eingeschalteter Open API: Broadcast und Abfrage aller Adressen im
    eigenen Netz (UDP-Port 30000). Mehrere Durchgaenge, weil die Batterie die erste Anfrage oft ignoriert."""
    import select
    found = {}
    own = _own_ips()
    targets = ["255.255.255.255"]
    for base in _local_subnets()[:2]:
        targets += [f"{base}.{i}" for i in range(1, 255) if f"{base}.{i}" not in own]
    msg = json.dumps({"id": 1, "method": "Marstek.GetDevice", "params": {"ble_mac": "0"}}).encode()

    def collect(sock, seconds):
        t_end = time.time() + seconds
        while True:
            rem = t_end - time.time()
            if rem <= 0:
                return
            r, _, _ = select.select([sock], [], [], rem)
            if not r:
                return
            try:
                pkt, src = sock.recvfrom(4096)
            except OSError:
                return
            if src[0] in own:
                continue
            try:
                res = json.loads(pkt.decode("utf-8", errors="replace")).get("result") or {}
            except ValueError:
                continue
            if res.get("device"):
                found[src[0]] = {"ip": res.get("ip") or src[0], "device": res.get("device"),
                                 "ver": res.get("ver"), "mac": res.get("wifi_mac") or res.get("ble_mac")}

    try:
        sock = _udp_socket()
        sock.settimeout(None)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
        except OSError:
            pass
        for _ in range(rounds):
            for ip in targets:
                if ip in found:
                    continue
                try:
                    sock.sendto(msg, (ip, 30000))
                except OSError:
                    pass
                collect(sock, 0.002)
            collect(sock, 1.5)
        collect(sock, 1.0)
        sock.close()
    except OSError:
        pass
    return sorted(found.values(), key=lambda d: tuple(int(x) for x in d["ip"].split(".")))


def test_api(ip, port=30000):
    try:
        s = _udp_socket()
        s.settimeout(1.5)
        msg = json.dumps({"id": 7, "method": "Marstek.GetDevice", "params": {"ble_mac": "0"}}).encode()
        for _ in range(3):
            s.sendto(msg, (ip, int(port)))
            try:
                while True:
                    pkt, src = s.recvfrom(4096)
                    if src[0] == ip:
                        res = json.loads(pkt.decode("utf-8", errors="replace")).get("result") or {}
                        s.close()
                        return f"{res.get('device', '?')} FW {res.get('ver', '?')}"
            except socket.timeout:
                time.sleep(1.0)
        s.close()
    except (OSError, ValueError):
        pass
    return None


def test_tcp(ip, port=502):
    try:
        with socket.create_connection((ip, int(port)), timeout=2):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Netz-Zaehler auswaehlen
# ---------------------------------------------------------------------------
def power_sensors():
    try:
        states = core("GET", "/states") or []
    except ApiError:
        return []
    out = []
    for s in states:
        eid = s.get("entity_id", "")
        a = s.get("attributes") or {}
        if not eid.startswith("sensor.") or eid.startswith("sensor.marstekvenus_"):
            continue
        unit = a.get("unit_of_measurement")
        if unit not in ("W", "kW") and a.get("device_class") != "power":
            continue
        try:
            float(s.get("state"))
        except (TypeError, ValueError):
            continue
        name = (a.get("friendly_name") or eid)
        txt = (eid + " " + name).lower()
        score = 0
        for w, pts in (("netz", 5), ("grid", 5), ("zaehler", 4), ("zähler", 4), ("meter", 4), ("sml", 4),
                       ("bezug", 3), ("einspeis", 3), ("wirkleistung", 3), ("import", 2), ("total", 1),
                       ("shelly", 2), ("em", 1), ("haus", 1), ("house", 1), ("power", 1)):
            if w in txt:
                score += pts
        for w in ("pv", "solar", "battery", "batterie", "inverter", "wechselrichter", "charge", "laden"):
            if w in txt:
                score -= 3
        if re.search(r"(^|[^a-z0-9])(l[123]|phase(_?[abc123])?)([^a-z0-9]|$)", txt):
            score -= 3  # einzelne Phase, nicht der ganze Hausanschluss
        out.append((score, eid, name, s.get("state"), unit))
    out.sort(key=lambda x: (-x[0], x[1]))
    return out


def ask_grid_sensor(current=None):
    head(L("Netz-Zaehler", "Grid meter"))
    hint(L("Die Steuerung braucht einen Sensor, der die aktuelle Leistung am Hausanschluss zeigt "
           "(z. B. Lesekopf am Stromzaehler, Shelly 3EM).",
           "The control needs a sensor that shows the current power at the grid connection "
           "(e.g. IR reader on the electricity meter, Shelly 3EM)."))
    sensors = power_sensors()
    if sensors:
        shown = sensors[:12]
        opts = [f"{eid}  ({name}, {state} {unit or ''})" for _, eid, name, state, unit in shown]
        opts.append(L("andere Entitaet von Hand eingeben", "enter another entity manually"))
        dflt = 1
        if current:
            for i, x in enumerate(shown, 1):
                if x[1] == current:
                    dflt = i
        i = choose(L("Welcher Sensor ist dein Netz-Zaehler?", "Which sensor is your grid meter?"), opts, dflt)
        if i < len(shown):
            eid = shown[i][1]
        else:
            eid = ask(L("Entitaet (z. B. sensor.stromzaehler_leistung)", "Entity (e.g. sensor.grid_power)"), current)
    else:
        warn(L("Keine passenden Leistungs-Sensoren gefunden.", "No suitable power sensors found."))
        eid = ask(L("Entitaet des Netz-Zaehlers", "Entity of the grid meter"), current or "sensor.")
    unit, val = "W", None
    try:
        st = core("GET", f"/states/{eid}") or {}
        unit = (st.get("attributes") or {}).get("unit_of_measurement") or "W"
        val = st.get("state")
    except ApiError:
        warn(L("Diese Entitaet gibt es (noch) nicht.", "This entity does not exist (yet)."))
    if val is not None:
        say(L(f"  Aktueller Wert: {val} {unit}", f"  Current value: {val} {unit}"))
    say(L("  Wichtig: Der Wert muss POSITIV sein, wenn Strom aus dem Netz BEZOGEN wird,",
          "  Important: the value must be POSITIVE while power is DRAWN from the grid,"))
    say(L("  und NEGATIV, wenn ins Netz eingespeist wird.", "  and NEGATIVE while feeding into the grid."))
    pos = ask_yes(L("Ist das bei deinem Zaehler so?", "Is that the case for your meter?"), True)
    return {"enabled": True, "entity": eid, "invert": not pos}


# ---------------------------------------------------------------------------
# Batterien abfragen
# ---------------------------------------------------------------------------
def ask_batteries(template_vals, found=None):
    if found is None:
        head(L("Batterien", "Batteries"))
        say(L("  Suche Marstek-Batterien mit eingeschalteter Open API im Netzwerk (bis zu 30 Sekunden) ...",
              "  Searching the network for Marstek batteries with Open API enabled (up to 30 seconds) ..."))
        found = discover_marstek()
    if found:
        for d in found:
            ok(f"{d['ip']}  {d['device']}  FW {d['ver']}")
    else:
        hint(L("Keine gefunden. Das ist normal, wenn die Open API in der Marstek-App aus ist "
               "oder du RS485-LAN-Adapter (Modbus) nutzt.",
               "None found. That is normal if the Open API is switched off in the Marstek app "
               "or you use RS485-to-LAN adapters (Modbus)."))
    n = ask_int(L("Wie viele Marstek-Batterien hast du?", "How many Marstek batteries do you have?"),
                max(1, len(found)), 1, 8)
    proto = dict(next(iter((template_vals.get("BATTERY_CONFIG") or {}).values()), {}))
    cfg, comm = {}, {}
    used_api = set()
    for i in range(1, n + 1):
        say()
        say(f"  {C_HEAD}Venus {i}{C_RST}")
        c = choose(L("Wie ist diese Batterie mit dem Netzwerk verbunden?", "How is this battery connected?"), [
            L("WLAN - Open API (kein Zusatzgeraet noetig)", "Wi-Fi - Open API (no extra hardware)"),
            L("RS485-LAN-Adapter - Modbus TCP", "RS485-to-LAN adapter - Modbus TCP"),
            L("Beides: Modbus, bei Ausfall automatisch WLAN-API", "Both: Modbus, automatic fallback to Wi-Fi API"),
        ], 1)
        b = dict(proto)
        b.update({"enabled": True, "modbus_host": "", "api_host": "", "port": 502, "unit": 1, "api_port": 30000})
        if c in (1, 2):
            b["modbus_host"] = ask_ip(L("IP-Adresse des RS485-LAN-Adapters", "IP address of the RS485-to-LAN adapter"))
            b["port"] = ask_int(L("Modbus-Port", "Modbus port"), 502, 1, 65535)
            b["unit"] = ask_int(L("Modbus Slave-ID (meist 1)", "Modbus slave ID (usually 1)"), 1, 1, 247)
            if test_tcp(b["modbus_host"], b["port"]):
                ok(L("Adapter antwortet", "Adapter answers"))
            else:
                warn(L("Adapter antwortet nicht - IP/Port pruefen (kann spaeter im Dashboard geaendert werden).",
                       "Adapter does not answer - check IP/port (can be changed later in the dashboard)."))
        if c in (0, 2):
            dflt = next((d["ip"] for d in found if d["ip"] not in used_api), None)
            hint(L("Die WLAN-IP steht in der Marstek-App (Geraet -> Einstellungen -> Open API / Netzwerk). "
                   "Tipp: im Router eine feste IP vergeben.",
                   "The Wi-Fi IP is shown in the Marstek app (device -> settings -> Open API / network). "
                   "Tip: give the battery a fixed IP in your router."))
            b["api_host"] = ask_ip(L("WLAN-IP-Adresse der Batterie", "Wi-Fi IP address of the battery"), dflt)
            used_api.add(b["api_host"])
            b["api_port"] = ask_int(L("Open-API-Port (Marstek-App, Standard 30000)", "Open API port (Marstek app, default 30000)"),
                                    30000, 1, 65535)
            r = test_api(b["api_host"], b["api_port"])
            if r:
                ok(L(f"Batterie antwortet: {r}", f"Battery answers: {r}"))
            else:
                warn(L("Keine Antwort. Ist die Open API in der Marstek-App eingeschaltet und der Port richtig?",
                       "No answer. Is the Open API enabled in the Marstek app and the port correct?"))
        b["comm_mode"] = "api" if c == 0 else "modbus"
        comm[f"BATTERY{i}_COMM"] = ("API", "Modbus", "Modbus, sonst API")[c]
        b["capacity_kwh"] = ask_float(L("Nutzbare Kapazitaet in kWh (Venus E: 5.12)", "Usable capacity in kWh (Venus E: 5.12)"),
                                      b.get("capacity_kwh", 5.12) if i > 1 else 5.12, 0.5, 100)
        b["max_charge_w"] = ask_int(L("Max. Ladeleistung in W", "Max. charge power in W"), b.get("max_charge_w", 2500), 100, 10000)
        hint(L("Steckerspeicher in Deutschland: meist 800 W Einspeisegrenze.",
               "Plug-in storage in Germany: usually limited to 800 W feed-in."))
        b["max_discharge_w"] = ask_int(L("Max. Entladeleistung in W", "Max. discharge power in W"),
                                       b.get("max_discharge_w", 2500), 100, 10000)
        cfg[f"battery{i}"] = b
        proto = dict(b)
    return cfg, comm


# ---------------------------------------------------------------------------
# Automatische Einrichtung (Standard bei Neuinstallation)
# ---------------------------------------------------------------------------
def api_get_mode(ip, port=30000):
    """ES.GetMode einer Batterie lesen (dict oder None)."""
    try:
        s = _udp_socket()
        s.settimeout(1.5)
        msg = json.dumps({"id": 9, "method": "ES.GetMode", "params": {"id": 0}}).encode()
        for _ in range(3):
            s.sendto(msg, (ip, int(port)))
            try:
                while True:
                    pkt, src = s.recvfrom(4096)
                    if src[0] != ip:
                        continue
                    d = json.loads(pkt.decode("utf-8", errors="replace"))
                    if d.get("id") == 9 and isinstance(d.get("result"), dict):
                        s.close()
                        return d["result"]
            except socket.timeout:
                time.sleep(1.1)
        s.close()
    except (OSError, ValueError):
        pass
    return None


def auto_grid_sensor():
    """Netz-Zaehler automatisch erkennen. Rueckgabe (Treffer oder None, Kandidaten)."""
    cands = [x for x in power_sensors() if x[0] >= 4]
    if not cands:
        return None, []
    if len(cands) == 1 or cands[0][0] - cands[1][0] >= 2:
        return cands[0], cands
    return None, cands


def auto_setup(tv):
    """Batterien und Netz-Zaehler ohne Rueckfragen einrichten; gefragt wird nur, was sich nicht
    erkennen laesst. Rueckgabe: (BATTERY_CONFIG, BATTERYn_COMM, POWER_SENSOR, READ_ONLY)."""
    head(L("Batterien", "Batteries"))
    say(L("  Suche Marstek-Batterien mit eingeschalteter Open API im Netzwerk (bis zu 30 Sekunden) ...",
          "  Searching the network for Marstek batteries with Open API enabled (up to 30 seconds) ..."))
    found = discover_marstek()
    busy = []
    if found:
        proto = dict(next(iter((tv.get("BATTERY_CONFIG") or {}).values()), {}))
        bcfg, comm = {}, {}
        time.sleep(1.2)  # die Batterie ignoriert Anfragen, die zu schnell aufeinander folgen
        for i, d in enumerate(found, 1):
            b = dict(proto)
            b.update({"enabled": True, "comm_mode": "api", "modbus_host": "", "api_host": d["ip"], "port": 502,
                      "unit": 1, "api_port": 30000, "capacity_kwh": 5.12, "max_charge_w": 2500,
                      "max_discharge_w": 2500})
            bcfg[f"battery{i}"] = b
            comm[f"BATTERY{i}_COMM"] = "API"
            mode = api_get_mode(d["ip"])
            if mode:
                m = str(mode.get("mode") or "").strip()
                if m.lower() == "passive":
                    # Passive = Sollwert kommt gerade per Open API von einer anderen Steuerung
                    busy.append(i)
                    m = L("fremdgesteuert (Passive)", "externally controlled (Passive)")
                elif not m:
                    m = L("unbekannt", "unknown")
                state = L(f"Ladestand {mode.get('bat_soc', '?')} %, Modus {m}", f"charge {mode.get('bat_soc', '?')} %, mode {m}")
            else:
                state = L("Status gerade nicht lesbar", "status not readable right now")
            ok(f"Venus {i}: {d['ip']}  {d['device']}  FW {d['ver']}  ({state})")
            if "venuse" not in str(d.get("device") or "").lower().replace(" ", ""):
                warn(L("Unbekanntes Modell - Kapazitaet 5.12 kWh und 2500 W angenommen. Abweichend? "
                       "Installer spaeter mit --reconfigure starten.",
                       "Unknown model - assuming 5.12 kWh and 2500 W. Different? Run the installer with --reconfigure later."))
            time.sleep(1.2)
        hint(L("Alle gefundenen Batterien werden ueber WLAN (Open API) angebunden. Batterien mit RS485-Adapter "
               "oder ohne Open API: Installer mit --reconfigure starten.",
               "All batteries found are connected via Wi-Fi (Open API). Batteries with RS485 adapter or without "
               "Open API: run the installer with --reconfigure."))
    else:
        hint(L("Keine Batterie mit eingeschalteter Open API gefunden. Bitte die Batterien einmal von Hand angeben "
               "(oder in der Marstek-App die Open API einschalten und den Installer erneut starten).",
               "No battery with Open API enabled found. Please enter the batteries manually once "
               "(or enable the Open API in the Marstek app and run the installer again)."))
        bcfg, comm = ask_batteries(tv, found=[])

    head(L("Netz-Zaehler", "Grid meter"))
    pick, cands = auto_grid_sensor()
    if pick:
        _, eid, name, state, unit = pick
        ok(L(f"Netz-Zaehler erkannt: {eid} ({name}, gerade {state} {unit or ''})",
             f"Grid meter detected: {eid} ({name}, now {state} {unit or ''})"))
        hint(L("Angenommen: positiver Wert = Strom aus dem Netz, negativer Wert = Einspeisung. Laden die Batterien "
               "spaeter, wenn sie entladen sollten: Installer mit --reconfigure starten.",
               "Assumed: positive value = power drawn from the grid, negative = feed-in. If the batteries charge "
               "when they should discharge: run the installer with --reconfigure."))
        ps = {"enabled": True, "entity": eid, "invert": False}
    elif cands:
        ps = ask_grid_sensor()
    else:
        warn(L("Kein Netz-Zaehler in Home Assistant gefunden (Sensor fuer die Leistung am Hausanschluss).",
               "No grid meter found in Home Assistant (sensor for the power at the grid connection)."))
        ps = {"enabled": True, "entity": GRID_MISSING, "invert": False}

    read_only = False
    if busy:
        read_only = True
        warn(L(f"Venus {', '.join(map(str, busy))} wird gerade von einer anderen Steuerung geregelt.",
               f"Venus {', '.join(map(str, busy))} is currently controlled by another control (mode Passive)."))
        hint(L("Laeuft hier noch eine andere Steuerung (z. B. ein zweites Home Assistant)? Es darf immer nur EINE "
               "Steuerung die Batterien regeln.",
               "Is another control still running (e.g. a second Home Assistant)? Only ONE control may drive the batteries."))
    if ps["entity"] == GRID_MISSING:
        read_only = True
    if read_only:
        hint(L("Deshalb startet die Steuerung im BEOBACHTUNGSMODUS: sie liest und zeigt alle Werte, sendet aber "
               "keine Befehle an die Batterien. Ausschalten im Dashboard: Konfiguration -> Hauptschalter & "
               "Batterien -> 'Nur beobachten'" + (" (vorher den Netz-Zaehler einrichten und den Installer mit "
               "--reconfigure starten)." if ps["entity"] == GRID_MISSING else "."),
               "So the control starts in OBSERVE MODE: it reads and shows all values but sends no commands to the "
               "batteries. Switch it off in the dashboard: Konfiguration -> Hauptschalter & Batterien -> "
               "'Nur beobachten'" + (" (set up the grid meter first and run the installer with --reconfigure)."
                                      if ps["entity"] == GRID_MISSING else ".")))
    return bcfg, comm, ps, read_only


# ---------------------------------------------------------------------------
# Dateien schreiben
# ---------------------------------------------------------------------------
def backup_file(path, tag):
    if not os.path.exists(path):
        return None
    bdir = os.path.join(os.path.dirname(path), "backup")
    os.makedirs(bdir, exist_ok=True)
    dst = os.path.join(bdir, os.path.basename(path) + ".bak_" + time.strftime("%Y%m%d_%H%M%S") + "_" + tag)
    shutil.copy2(path, dst)
    return dst


def write_file(path, text):
    if DRY:
        outdir = os.environ.get("MARSTEK_DRY_OUT")
        if outdir:
            os.makedirs(outdir, exist_ok=True)
            with open(os.path.join(outdir, os.path.basename(path)), "w", encoding="utf-8") as f:
                f.write(text)
        ok(L(f"(Testlauf) wuerde schreiben: {path}", f"(dry run) would write: {path}"))
        return
    tmp = path + ".tmp_install"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Dashboard + fold-entity-row
# ---------------------------------------------------------------------------
def ensure_fold_row(ws):
    try:
        res = ws.call("lovelace/resources") or []
    except ApiError as e:
        warn(L(f"Dashboard-Ressourcen nicht lesbar ({e}).", f"Dashboard resources not readable ({e})."))
        return
    if any("fold-entity-row" in (r.get("url") or "") for r in res):
        ok(L("fold-entity-row ist vorhanden", "fold-entity-row is present"))
        return
    if DRY:
        ok(L("(Testlauf) fold-entity-row wuerde installiert", "(dry run) fold-entity-row would be installed"))
        return
    try:
        global NEED_HA_RESTART
        dst = os.path.join(ha_config_dir(), FOLD_ROW_REL)
        if not os.path.isdir(os.path.join(ha_config_dir(), "www")):
            NEED_HA_RESTART = True  # /local gibt es erst nach einem Neustart von Home Assistant
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with urllib.request.urlopen(FOLD_ROW_URL, timeout=60) as r:
            data = r.read()
        if len(data) < 10000:
            raise OSError("download too small")
        with open(dst, "wb") as f:
            f.write(data)
        ws.call("lovelace/resources/create", res_type="module", url=FOLD_ROW_RES)
        ok(L("fold-entity-row installiert (aufklappbare Bereiche im Dashboard)",
             "fold-entity-row installed (collapsible sections in the dashboard)"))
    except (OSError, ApiError, urllib.error.URLError) as e:
        warn(L(f"fold-entity-row konnte nicht installiert werden ({e}). Die Konfigurationsseite zeigt dann "
               f"rote Kaesten - abhilfe: HACS -> fold-entity-row installieren.",
               f"fold-entity-row could not be installed ({e}). The configuration page will show red boxes - "
               f"fix: install fold-entity-row via HACS."))


def find_dashboard(ws):
    try:
        for d in ws.call("lovelace/dashboards/list") or []:
            if d.get("url_path") in (DASH_URL,) + DASH_URL_OLD:
                return d
    except ApiError:
        pass
    return None


def install_dashboard(ws, cfg_text, app_dir, update):
    with open(DASH_TEMPLATE, encoding="utf-8") as f:
        template = json.load(f)
    vals = config_values(cfg_text)
    bats = [bat_index(n) for n in batteries_from_config(cfg_text, app_dir)]
    grid = (vals.get("POWER_SENSOR") or {}).get("entity") or GRID_MISSING
    emap = vals.get("HA_ENTITY_ID_MAP") or {}
    prefix = vals.get("HA_EXPORT_PREFIX") or "marstekvenus"
    dash = build_dashboard(template, bats, grid, emap, prefix)
    existing = find_dashboard(ws)
    url = DASH_URL
    if existing:
        url = existing["url_path"]
        if update and not ask_yes(L(f"Dashboard '{existing.get('title')}' durch die neue Version ersetzen? "
                                    f"(eigene Aenderungen daran gehen verloren, eine Sicherung wird angelegt)",
                                    f"Replace dashboard '{existing.get('title')}' with the new version? "
                                    f"(your own changes to it are lost, a backup is made)"), False):
            ok(L("Dashboard bleibt unveraendert", "Dashboard left unchanged"))
            return url
        try:
            old = ws.call("lovelace/config", url_path=url)
            bdir = os.path.join(app_dir, "backup")
            os.makedirs(bdir, exist_ok=True)
            if not DRY:
                with open(os.path.join(bdir, f"dashboard_{url}_{time.strftime('%Y%m%d_%H%M%S')}.json"), "w",
                          encoding="utf-8") as f:
                    json.dump(old, f, ensure_ascii=False, indent=1)
        except ApiError:
            pass
    elif not DRY:
        ws.call("lovelace/dashboards/create", url_path=DASH_URL, title="Marstek", icon="mdi:home-battery",
                show_in_sidebar=True, require_admin=False, mode="storage")
    if not DRY:
        ws.call("lovelace/config/save", url_path=url, config=dash)
    else:
        write_file(os.path.join(app_dir, "dashboard_preview.json"), json.dumps(dash, ensure_ascii=False, indent=1))
    ok(L(f"(Testlauf) Dashboard 'Marstek' wuerde angelegt (Adresse /{url})",
         f"(dry run) dashboard 'Marstek' would be created (address /{url})") if DRY
       else L(f"Dashboard 'Marstek' angelegt (Seitenleiste, Adresse /{url})",
              f"Dashboard 'Marstek' created (sidebar, address /{url})"))
    return url


# ---------------------------------------------------------------------------
# Pruefung nach dem Start
# ---------------------------------------------------------------------------
def verify(bats, prefix="marstekvenus", timeout=150):
    head(L("Pruefung", "Check"))
    say(L("  Warte, bis die Batterien die ersten Werte liefern (bis zu 2,5 Minuten) ...",
          "  Waiting for the first values from the batteries (up to 2.5 minutes) ..."))
    t0 = time.time()
    seen = {}
    while time.time() - t0 < timeout and len(seen) < len(bats):
        for i in bats:
            if i in seen:
                continue
            try:
                st = core("GET", f"/states/sensor.{prefix}_{i}_battery_soc")
                if st and st.get("state") not in (None, "unknown", "unavailable"):
                    seen[i] = st.get("state")
                    ok(L(f"Venus {i}: Ladestand {seen[i]} %", f"Venus {i}: state of charge {seen[i]} %"))
            except ApiError:
                pass
        time.sleep(5)
    for i in bats:
        if i not in seen:
            warn(L(f"Venus {i}: noch keine Werte - Verbindung im Dashboard (Konfiguration -> Venus {i}) pruefen.",
                   f"Venus {i}: no values yet - check the connection in the dashboard (Konfiguration -> Venus {i})."))
    try:
        log = sup("GET", f"/addons/{ADDON_APPDAEMON}/logs", timeout=30)
        log = log if isinstance(log, str) else ""
    except ApiError:
        log = ""
    if not log:
        try:
            log = http("GET", SUP + f"/addons/{ADDON_APPDAEMON}/logs", raw=True)
        except ApiError:
            log = ""
    tail = log[-20000:]
    if "Traceback" in tail or "ModuleNotFoundError" in tail:
        warn(L("Im AppDaemon-Protokoll stehen Fehler (Einstellungen -> Add-ons -> AppDaemon -> Protokoll).",
               "There are errors in the AppDaemon log (Settings -> Add-ons -> AppDaemon -> Log)."))
    return len(seen) == len(bats)


# ---------------------------------------------------------------------------
# Hauptablauf
# ---------------------------------------------------------------------------
def detect_language():
    try:
        lang = (core("GET", "/config") or {}).get("language") or "de"
    except ApiError:
        lang = "de"
    return "de" if lang.lower().startswith("de") else "en"


def check_environment():
    if not TOKEN:
        raise Abort(L("Kein SUPERVISOR_TOKEN gefunden. Bitte den Installer im Home-Assistant-Add-on "
                      "'Terminal & SSH' oder 'Advanced SSH & Web Terminal' starten.",
                      "No SUPERVISOR_TOKEN found. Please run the installer in the Home Assistant add-on "
                      "'Terminal & SSH' or 'Advanced SSH & Web Terminal'."))
    try:
        sup("GET", "/supervisor/ping", timeout=10)
    except ApiError as e:
        raise Abort(L(f"Supervisor nicht erreichbar ({e}). Der Installer braucht Home Assistant OS oder Supervised.",
                      f"Supervisor not reachable ({e}). The installer needs Home Assistant OS or Supervised."))


def do_uninstall():
    head(L("Deinstallation", "Uninstall"))
    root = appdaemon_root()
    apps = os.path.join(root, "apps")
    app_dir = find_existing_app(apps)
    if not app_dir:
        warn(L("Keine Installation gefunden.", "No installation found."))
        return
    if not ask_yes(L(f"Marstek-Steuerung aus {app_dir} entfernen? Die Batterien bekommen danach keine "
                     f"Befehle mehr (sie bleiben im zuletzt gesetzten Zustand bzw. fallen nach 5 min auf Stopp).",
                     f"Remove Marstek control from {app_dir}? The batteries will not get commands any more."), False):
        return
    ensure_core_access(apps)
    arc = shutil.make_archive(os.path.join(ha_config_dir(), "marstek-venus-backup-" + time.strftime("%Y%m%d_%H%M%S")),
                              "gztar", app_dir)
    ok(L(f"Sicherung: {arc}", f"Backup: {arc}"))
    remove_from_apps_yaml(apps)
    shutil.rmtree(app_dir)
    ok(L("Dateien entfernt", "Files removed"))
    try:
        ws = WS()
        d = find_dashboard(ws)
        if d and ask_yes(L("Dashboard 'Marstek' ebenfalls loeschen?", "Delete the 'Marstek' dashboard too?"), True):
            ws.call("lovelace/dashboards/delete", dashboard_id=d["id"])
            ok(L("Dashboard geloescht", "Dashboard deleted"))
        ws.close()
    except ApiError as e:
        warn(str(e))
    start_or_restart(ADDON_APPDAEMON, "AppDaemon", restart=True)
    say()
    say(L("Fertig. Mosquitto und AppDaemon bleiben installiert.", "Done. Mosquitto and AppDaemon stay installed."))


def main():
    global LANG, DRY, AUTO_YES
    ap = argparse.ArgumentParser(description="Marstek Venus Steuerung - Einrichtung")
    ap.add_argument("--lang", choices=("de", "en"))
    ap.add_argument("--dry-run", action="store_true", help="nichts veraendern, nur zeigen")
    ap.add_argument("--uninstall", action="store_true")
    ap.add_argument("--dashboard", action="store_true", help="nur das Dashboard neu erzeugen")
    ap.add_argument("--reconfigure", action="store_true", help="Batterien und Zaehler neu abfragen")
    ap.add_argument("--yes", action="store_true", help="alle Vorschlaege uebernehmen (Update ohne Rueckfragen)")
    ap.add_argument("--ask", action="store_true", help="Neuinstallation: alles einzeln abfragen statt automatisch")
    a = ap.parse_args()
    DRY, AUTO_YES = a.dry_run, a.yes
    version = read(VERSION_FILE).strip() if os.path.exists(VERSION_FILE) else "?"

    check_environment()
    LANG = a.lang or detect_language()
    say(f"{C_HEAD}Marstek Venus Steuerung {version} - {L('Einrichtung', 'Setup')}{C_RST}")
    if DRY:
        warn(L("TESTLAUF: es wird nichts veraendert.", "DRY RUN: nothing will be changed."))

    if a.uninstall:
        return do_uninstall()

    root = appdaemon_root() if not DRY or _app_configs_base() else "/tmp/marstek-dry"
    apps = os.path.join(root, "apps")
    os.makedirs(apps, exist_ok=True)
    existing = find_existing_app(apps)
    app_dir = existing or os.path.join(apps, APP_FOLDER_NAME)
    update = existing is not None
    template = read(os.path.join(APP_SRC, "pv_config.py"))

    if a.dashboard:
        ensure_core_access(apps)
        ws = WS()
        ensure_fold_row(ws)
        install_dashboard(ws, read(os.path.join(app_dir, "pv_config.py")), app_dir, update=False)
        ws.close()
        return

    # 1) Add-ons ---------------------------------------------------------
    head(L("Add-ons", "Add-ons"))
    login_path = os.path.join(app_dir, "pv_mqtt_login.json")
    login = None
    if os.path.exists(login_path):
        try:
            login = json.loads(read(login_path))
        except ValueError:
            login = None
    if not login or not login.get("user"):
        login = {"host": "core-mosquitto", "port": 1883, "user": MQTT_USER,
                 "password": secrets.token_urlsafe(18)}
    setup_mosquitto(login)
    setup_appdaemon()
    ensure_core_access(apps)

    # 2) Einstellungen ---------------------------------------------------
    if update:
        head(L("Update", "Update"))
        ok(L(f"Vorhandene Installation gefunden: {app_dir}", f"Existing installation found: {app_dir}"))
        old_cfg = read(os.path.join(app_dir, "pv_config.py"))
        dflt_path = os.path.join(app_dir, "pv_config.default")
        old_dflt = read(dflt_path) if os.path.exists(dflt_path) else None
        cfg_text, carried = config_merge(old_cfg, template, old_dflt)
        ok(L(f"{len(carried)} eigene Einstellungen uebernommen", f"{len(carried)} own settings kept"))
        if a.reconfigure:
            vals = config_values(cfg_text)
            bcfg, comm = ask_batteries(vals)
            ps = ask_grid_sensor((vals.get("POWER_SENSOR") or {}).get("entity"))
            ro = vals.get("READ_ONLY", False)
            if ro and ps.get("entity") and ps["entity"] != GRID_MISSING:
                ro = not ask_yes(L("Beobachtungsmodus ausschalten (Steuerung regelt die Batterien)?",
                                   "Switch off observe mode (the control drives the batteries)?"), True)
            read_only = bool(ro)
            cfg_text = config_set(cfg_text, dict(BATTERY_CONFIG=bcfg, POWER_SENSOR=ps, READ_ONLY=read_only, **comm))
    else:
        head(L("Neuinstallation", "New installation"))
        tv = config_values(template)
        if a.ask:
            bcfg, comm = ask_batteries(tv)
            ps = ask_grid_sensor()
            read_only = False
        else:
            bcfg, comm, ps, read_only = auto_setup(tv)
        values = dict(BATTERY_CONFIG=bcfg, POWER_SENSOR=ps, HA_ENTITY_ID_MAP={}, CONTROL_ENABLE=True,
                      READ_ONLY=read_only, **comm)
        cfg_text = config_set(template, values)
        say()
        say(L("  Zusammenfassung:", "  Summary:"))
        for n, b in bcfg.items():
            way = comm.get(f"BATTERY{bat_index(n)}_COMM")
            say(f"    Venus {bat_index(n)}: {way}, Modbus {b['modbus_host'] or '-'}, API {b['api_host'] or '-'}, "
                f"{b['capacity_kwh']} kWh, {b['max_charge_w']}/{b['max_discharge_w']} W")
        say(f"    {L('Netz-Zaehler', 'Grid meter')}: {ps['entity']}{' (' + L('Vorzeichen gedreht', 'sign inverted') + ')' if ps['invert'] else ''}")
        if read_only:
            say(f"    {L('Beobachtungsmodus: an (keine Befehle an die Batterien)', 'Observe mode: on (no commands to the batteries)')}")
        if a.ask and not ask_yes(L("So einrichten?", "Set up like this?"), True):
            raise Abort(L("Abgebrochen - es wurde nichts an der Steuerung veraendert.",
                          "Cancelled - nothing was changed."))

    # 3) Dateien ----------------------------------------------------------
    head(L("Dateien", "Files"))
    if not DRY:
        os.makedirs(app_dir, exist_ok=True)
    for fn in PROGRAM_FILES:
        dst = os.path.join(app_dir, fn)
        if update and not DRY:
            backup_file(dst, "vor_update_" + version)
        write_file(dst, read(os.path.join(APP_SRC, fn)))
    cfg_path = os.path.join(app_dir, "pv_config.py")
    if update and not DRY:
        backup_file(cfg_path, "vor_update_" + version)
    write_file(cfg_path, cfg_text)
    write_file(os.path.join(app_dir, "pv_config.default"), template)
    werk = os.path.join(app_dir, "pv_config.werk")
    if update and os.path.exists(werk):
        werk_text, _ = config_merge(read(werk), template, old_dflt)
        write_file(werk, werk_text)
    else:
        write_file(werk, cfg_text)
    if not os.path.exists(login_path) or DRY:
        write_file(login_path, json.dumps(login, indent=2))
    for fn in ("README.md",):
        src = os.path.join(APP_SRC, fn)
        if os.path.exists(src):
            write_file(os.path.join(app_dir, fn), read(src))
    if not DRY:
        shutil.rmtree(os.path.join(app_dir, "__pycache__"), ignore_errors=True)
    ok(L(f"(Testlauf) Dateien wuerden nach {app_dir} geschrieben", f"(dry run) files would be written to {app_dir}") if DRY
       else L(f"Dateien in {app_dir} geschrieben", f"Files written to {app_dir}"))
    ensure_apps_yaml(apps)

    # 4) Home Assistant ---------------------------------------------------
    head("Home Assistant")
    ensure_mqtt_integration(login)
    try:
        ws = WS()
        ensure_fold_row(ws)
        url = install_dashboard(ws, cfg_text, app_dir, update)
        ws.close()
    except (ApiError, OSError) as e:
        url = DASH_URL
        warn(L(f"Dashboard konnte nicht angelegt werden ({e}). Spaeter erneut versuchen mit: "
               f"bash install.sh --dashboard", f"Dashboard could not be created ({e}). Retry later with: "
               f"bash install.sh --dashboard"))

    # 5) Start + Pruefung -------------------------------------------------
    head("AppDaemon")
    if start_or_restart(ADDON_APPDAEMON, "AppDaemon", restart=True) and not DRY:
        vals = config_values(cfg_text)
        verify([bat_index(n) for n in batteries_from_config(cfg_text, app_dir)],
               vals.get("HA_EXPORT_PREFIX") or "marstekvenus")

    remove_core_bridge(apps)
    if NEED_HA_RESTART and not DRY:
        say(L("  Home Assistant wird einmal neu gestartet, damit das Dashboard alle Teile laden kann ...",
              "  Restarting Home Assistant once so the dashboard can load all parts ..."))
        try:
            sup("POST", "/core/restart", timeout=120)
            ok(L("Home Assistant startet neu (etwa 1 Minute)", "Home Assistant is restarting (about 1 minute)"))
        except ApiError as e:
            warn(L(f"Neustart nicht moeglich ({e}) - bitte Home Assistant einmal neu starten "
                   f"(Einstellungen -> System -> Neu starten).",
                   f"Restart not possible ({e}) - please restart Home Assistant once (Settings -> System -> Restart)."))

    head(L("Fertig", "Done"))
    if config_values(cfg_text).get("READ_ONLY"):
        warn(L("Beobachtungsmodus ist an: die Steuerung sendet noch keine Befehle an die Batterien.",
               "Observe mode is on: the control does not send commands to the batteries yet."))
    say(L(f"  Dashboard: in der Seitenleiste 'Marstek' (Adresse /{url}).",
          f"  Dashboard: 'Marstek' in the sidebar (address /{url})."))
    say(L("  Dort kannst du alles weitere einstellen: Hauptschalter, Batterien, IP-Adressen, Grenzen.",
          "  There you can adjust everything else: main switch, batteries, IP addresses, limits."))
    say(L("  Update spaeter: denselben Befehl erneut ausfuehren - deine Einstellungen bleiben erhalten.",
          "  Update later: run the same command again - your settings are kept."))


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Abort as e:
        say()
        err(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        say()
        err(L("Abgebrochen.", "Cancelled."))
        sys.exit(130)
    except ApiError as e:
        say()
        err(L(f"Fehler bei der Verbindung zu Home Assistant: {e}", f"Error talking to Home Assistant: {e}"))
        sys.exit(2)
