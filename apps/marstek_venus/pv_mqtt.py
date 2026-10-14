# =============================================================================
# pv_mqtt.py  -  MQTT-Parameterbruecke fuer pv_control.py (Marstek Venus)
#
# Meldet ausgewaehlte Werte aus pv_config.py per MQTT-Discovery als Geraet
# "Marstek Steuerung" in Home Assistant an (Schalter / Zahlen / Buttons).
# Aenderungen im Dashboard werden zum Beginn des naechsten Regelzyklus
# uebernommen - ohne AppDaemon-Neustart.
#
# Verwaltung (Dashboard):
#   - "Aenderungen verwerfen"      -> zurueck auf die Werte aus pv_config.py
#   - "Einstellungen uebernehmen"  -> aktuelle Werte fest in pv_config.py schreiben
#   - "Werksreset" (2x druecken)   -> pv_config.werk wiederherstellen
#   - Batterie anlegen / loeschen  -> pv_batteries.json, danach Neustart des Scripts
#     Modbus: nur IP (und Port) eintragen. Slave-ID 0 = automatisch: die Bruecke
#     probiert die IDs 1..32 am Adapter durch und nimmt die erste, die einen
#     gueltigen SoC liefert (bereits vergebene IDs am gleichen Adapter werden
#     uebersprungen). Eine feste Slave-ID wird vor dem Anlegen geprueft.
#
# Sicherheit:
#   - Ohne Login-Datei / ohne Broker laeuft pv_control ganz normal weiter.
#   - Alle Werte werden auf min/max begrenzt.
#   - Vor jeder Datei-Aenderung wird eine Sicherung in backup/ angelegt.
#   - Datei-Aktionen laufen im Regel-Thread (apply_pending), nie parallel.
#
# Login: pv_mqtt_login.json im gleichen Ordner:
#   {"host": "...", "port": 1883, "user": "...", "password": "..."}
# =============================================================================

import json
import os
import re
import shutil
import threading
import time

try:
    import paho.mqtt.client as mqtt
except Exception:  # pragma: no cover
    mqtt = None

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
LOGIN_FILE = os.path.join(_THIS_DIR, "pv_mqtt_login.json")
OVERRIDE_FILE = os.path.join(_THIS_DIR, "pv_mqtt_overrides.json")
CONFIG_FILE = os.path.join(_THIS_DIR, "pv_config.py")
WERK_FILE = os.path.join(_THIS_DIR, "pv_config.werk")
BATTERY_FILE = os.path.join(_THIS_DIR, "pv_batteries.json")
CONTROL_FILE = os.path.join(_THIS_DIR, "pv_control.py")
BACKUP_DIR = os.path.join(_THIS_DIR, "backup")

BASE = "marstek_ctrl"
DISCOVERY = "homeassistant"
DEVICE = {
    "identifiers": ["marstek_pv_control"],
    "name": "Marstek Steuerung",
    "manufacturer": "AppDaemon",
    "model": "pv_control.py",
    "sw_version": "2.0.18",
}
MAX_BATTERY_SLOTS = 8
# FIX 31: Anbindung je Batterie - Texte muessen zu COMM_CHOICES in pv_control.py passen
COMM_OPTIONS = ["Modbus", "API", "Modbus, sonst API", "API, sonst Modbus"]
# FIX 36: Verbindungsdaten je Batterie (Suffix, Typ, Name, min, max, step, Icon, Schluessel in BATTERY_CONFIG)
CONN_FIELDS = [
    ("_MODBUS_HOST", "text", "IP LAN-Adapter (Modbus)", None, None, None, "mdi:ip-network", "modbus_host"),
    ("_MODBUS_PORT", "number", "Modbus-Port", 1, 65535, 1, "mdi:numeric", "port"),
    ("_MODBUS_UNIT", "number", "Modbus Slave-ID", 1, 247, 1, "mdi:numeric", "unit"),
    ("_API_HOST", "text", "WLAN-IP Batterie (API)", None, None, None, "mdi:wifi", "api_host"),
    ("_API_PORT", "number", "API UDP-Port", 1, 65535, 1, "mdi:numeric", "api_port"),
]
# FIX 48: Grenzwerte je Batterie (Suffix, Typ, Name, min, max, step, Einheit, Icon, Schluessel in BATTERY_CONFIG)
LIMIT_FIELDS = [
    ("_MAX_CHARGE_W", "number", "max. Ladeleistung", 0, 10000, 50, "W", "mdi:battery-arrow-up", "max_charge_w"),
    ("_MAX_DISCHARGE_W", "number", "max. Entladeleistung", 0, 10000, 50, "W", "mdi:battery-arrow-down", "max_discharge_w"),
    ("_MIN_SOC", "number", "Min-SoC", 0, 50, 1, "%", "mdi:battery-10", "min_soc"),
    ("_MAX_SOC", "number", "Max-SoC", 50, 100, 1, "%", "mdi:battery", "max_soc"),
    ("_CAPACITY_KWH", "number", "Kapazitaet", 0.5, 50, 0.1, "kWh", "mdi:battery-high", "capacity_kwh"),
]

# (Key in pv_config, Typ, Name, min, max, step, Einheit, Icon, entity_category)
BASE_PARAMS = [
    # ---- Wichtig --------------------------------------------------------
    ("SOC_SAFETY_ENABLE",         "switch", "SoC-Schutz aktiv",            None, None, None, None,  "mdi:shield-check",       None),
    ("SOC_SAFETY_MIN_SOC",        "number", "Min-SoC (Notladung ab)",      5,    50,   1,    "%",   "mdi:battery-alert",      None),
    ("SOC_SAFETY_TARGET_SOC",     "number", "Ziel-SoC Notladung",          5,    60,   1,    "%",   "mdi:battery-arrow-up",   None),
    ("SOC_SAFETY_CHARGE_W",       "number", "Notladeleistung",             100,  2500, 50,   "W",   "mdi:flash",              None),
    ("GRID_DEADBAND_W",           "number", "Netz-Totband",                0,    300,  5,    "W",   "mdi:arrow-collapse-horizontal", None),
    ("TOLERANCE_W",               "number", "Regel-Toleranz",              0,    200,  5,    "W",   "mdi:tune-variant",       None),
    ("ENABLE_BMS_CARE",           "switch", "BMS-Pflege (Vollladung)",     None, None, None, None,  "mdi:battery-heart",      None),
    ("FULL_CHARGE_SOC",           "number", "Vollladung bis",              90,   100,  1,    "%",   "mdi:battery-charging-100", None),
    ("FULL_CHARGE_INTERVAL_DAYS", "number", "Vollladung alle",             1,    60,   1,    "d",   "mdi:calendar-refresh",   None),
    # ---- Schalter --------------------------------------------------------
    ("CONTROL_ENABLE",            "switch", "Regelung aktiv (Hauptschalter)", None, None, None, None, "mdi:power",          None),
    ("READ_ONLY",                 "switch", "Nur beobachten (keine Befehle an Batterien)", None, None, None, None, "mdi:eye-outline", None),  # FIX 40
    # FIX 46: Zaehler & zweiter Speicher
    ("GRID_SENSOR",                "text",   "Netz-Zaehler (Sensor)",                       None, None, None, None, "mdi:transmission-tower", "config"),
    ("GRID_SENSOR_INVERT",         "switch", "Netz-Zaehler: Vorzeichen drehen",             None, None, None, None, "mdi:swap-vertical",      "config"),
    ("METER_MAX_AGE_S",            "number", "STOP wenn Zaehlerwert aelter als",           0, 900, 5, "s", "mdi:timer-alert-outline", "config"),  # FIX 47
    ("EXT_STORAGE_DISCHARGE_SENSOR", "text", "Zweiter Speicher: Entladeleistung (Sensor)",  None, None, None, None, "mdi:battery-arrow-down-outline", "config"),
    ("EXT_STORAGE_CHARGE_SENSOR",  "text",   "Zweiter Speicher: Ladeleistung (Sensor)",     None, None, None, None, "mdi:battery-arrow-up-outline",   "config"),
    ("EXT_PV_SENSOR",              "text",   "Zweiter Speicher: PV-Leistung (Sensor)",      None, None, None, None, "mdi:solar-power",        "config"),
    # (BATTERYn_ACTIVE werden dynamisch je Batterie erzeugt)
    # ---- PV-Vorrang Notladung / BMS-Pflege -------------------------------
    ("SOC_SAFETY_REQUIRE_PV",     "switch", "Notladung: Sonne bevorzugen", None, None, None, None,    "mdi:solar-power",    None),
    ("SOC_SAFETY_PV_WAIT_H",      "number", "Notladung: max. Warten auf Sonne", 0, 48, 1, "h",        "mdi:timer-sand",     None),
    ("BMS_CARE_REQUIRE_PV",       "switch", "BMS-Pflege: Sonne bevorzugen", None, None, None, None,   "mdi:solar-power",    None),
    ("BMS_CARE_PV_WAIT_H",        "number", "BMS-Pflege: max. Warten auf Sonne", 0, 168, 1, "h",      "mdi:timer-sand",     None),
    ("BMS_CARE_CELL_DELTA_MV",    "number", "BMS-Pflege vorziehen ab Zell-Unterschied", 0, 500, 5, "mV", "mdi:battery-heart-variant", None),  # FIX 50
    ("BMS_CARE_CELL_MIN_DAYS",    "number", "BMS-Pflege vorziehen fruehestens nach", 1, 30, 1, "d",  "mdi:calendar-clock",       None),  # FIX 50
    # ---- Erweitert: PV-Laden / Regelverhalten ----------------------------
    ("PV_CHARGE_ALLOW_W",         "number", "Laden ab Einspeisung",        -1000, 0,    10,  "W",     "mdi:transmission-tower-export", None),
    ("PV_SURPLUS_MIN_TIME",       "number", "PV-Ueberschuss Mindestdauer", 0,     300,  5,   "s",     "mdi:timer-outline",  None),
    ("STABLE_TIMER_S",            "number", "Moduswechsel fruehestens nach", 0,   300,  5,   "s",     "mdi:timer-lock-outline", None),
    ("GRID_STOP_AFTER_S",         "number", "Stopp nach Ausgleich",        0,     600,  10,  "s",     "mdi:timer-stop-outline", None),
    ("PV_BUCKET_ENABLE",          "switch", "PV-Stufen aktiv",             None, None, None, None,    "mdi:stairs",         None),
    ("PV_BUCKET_1_TOTAL_W",       "number", "Nur 1 Batterie bis",          0,     3000, 50,  "W",     "mdi:numeric-1-box",  None),
    ("PV_BUCKET_2_TOTAL_W",       "number", "Nur 2 Batterien bis",         0,     6000, 50,  "W",     "mdi:numeric-2-box",  None),
    ("PV_MIN_PER_BAT_W",          "number", "Mindestens je Batterie",      0,     1000, 10,  "W",     "mdi:battery-arrow-up-outline", None),
    ("PV_ROTATE_ENABLE",          "switch", "Batterien abwechseln",        None, None, None, None,    "mdi:rotate-3d-variant", None),
]

# Formular "Neue Batterie": (key, typ, name, min, max, step, unit, icon, default-Quelle)
FORM_FIELDS = [
    ("new_comm_mode",       "select", "Neu: Protokoll",             None, None, None, None, "mdi:lan", ["modbus", "api"]),
    ("new_modbus_host",     "text",   "Neu: IP LAN-Adapter (Modbus)", None, None, None, None, "mdi:ip-network", ""),
    ("new_api_host",        "text",   "Neu: WLAN-IP Batterie (API)",  None, None, None, None, "mdi:wifi", ""),
    ("new_port",            "number", "Neu: Modbus-Port",           1,    65535, 1,  None, "mdi:numeric", 502),
    ("new_unit",            "number", "Neu: Modbus Slave-ID (0 = automatisch)", 0, 247, 1, None, "mdi:numeric", 0),
    ("new_api_port",        "number", "Neu: API UDP-Port",          1,    65535, 1,  None, "mdi:numeric", 30000),
    ("new_capacity_kwh",    "number", "Neu: Kapazitaet",            0.5,  50,    0.1, "kWh", "mdi:battery", 5.6),
    ("new_max_charge_w",    "number", "Neu: Max. Ladeleistung",     100,  10000, 50, "W",   "mdi:battery-arrow-up", 2500),
    ("new_max_discharge_w", "number", "Neu: Max. Entladeleistung",  100,  10000, 50, "W",   "mdi:battery-arrow-down", 2500),
    ("new_min_soc",         "number", "Neu: Min-SoC",               0,    50,    1,  "%",   "mdi:battery-10", 11),
    ("new_max_soc",         "number", "Neu: Max-SoC",               50,   100,   1,  "%",   "mdi:battery", 99),
]

BUTTONS = [
    ("reset_overrides",  "Aenderungen verwerfen",            "mdi:restore",           "config"),
    ("apply_config",     "Einstellungen fest uebernehmen",   "mdi:content-save",      "config"),
    ("factory_reset",    "Werksreset (2x druecken)",         "mdi:factory",           "config"),
    ("add_battery",      "Neue Batterie anlegen",            "mdi:battery-plus",      "config"),
    ("delete_battery",   "Batterie loeschen (2x druecken)",  "mdi:battery-remove",    "config"),
]
RESET_KEY = "reset_overrides"
CONFIRM_S = 15


def _to_bool(v):
    if isinstance(v, bool):
        return v
    return str(v).strip().upper() in ("ON", "TRUE", "1", "YES")


def _py_literal(v):
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return repr(v)


try:
    from pymodbus.client import ModbusTcpClient
except Exception:  # pragma: no cover
    ModbusTcpClient = None
import socket

# Automatische Slave-ID-Suche: diese IDs werden der Reihe nach probiert
PROBE_UNITS = list(range(1, 33))
PROBE_TIMEOUT_S = 0.4
PROBE_BUDGET_S = 20


def _mb_read(client, addr, unit, count=1):
    """read_holding_registers fuer alle pymodbus-Versionen (device_id / slave / unit)."""
    for kw in ("device_id", "slave", "unit"):
        try:
            return client.read_holding_registers(addr, count=count, **{kw: unit})
        except TypeError:
            continue
    return None


def _mb_ok(rr):
    return rr is not None and not (hasattr(rr, "isError") and rr.isError()) and getattr(rr, "registers", None)


def _tcp_open(host, port, timeout=1.5):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


_IP_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")


def _valid_ip(s):
    m = _IP_RE.match(str(s or "").strip())
    return bool(m) and all(0 <= int(x) <= 255 for x in m.groups())


class ParamBridge:
    def __init__(self, app, target_globals):
        self.app = app
        self.g = target_globals
        # Batterie-Schalter dynamisch je vorhandener Batterie
        self.bat_names = sorted((self.g.get("BATTERY_CONFIG") or {}).keys(),
                                key=lambda n: (len(n), n))
        params = list(BASE_PARAMS)
        k = [p[0] for p in params].index("CONTROL_ENABLE") + 1
        for n in self.bat_names:
            key = n.upper() + "_ACTIVE"
            if key not in self.g:
                self.g[key] = True
            idx = self._idx(n)
            params.insert(k, (key, "switch", f"Venus {idx} vom Script gesteuert",
                              None, None, None, None, "mdi:battery-sync", None))
            k += 1
            # FIX 31: Auswahl der Anbindung je Batterie
            ckey = n.upper() + "_COMM"
            if ckey not in self.g:
                self.g[ckey] = self._comm_default(n)
            opts = list((self.g.get("COMM_CHOICES") or {}).keys()) or list(COMM_OPTIONS)
            params.insert(k, (ckey, "select", f"Venus {idx} Anbindung",
                              opts, None, None, None, "mdi:lan-connect", None))
            k += 1
            # FIX 36: Verbindungsdaten je Batterie
            for suffix, ftyp, flabel, flo, fhi, fstep, ficon, fsrc in CONN_FIELDS:
                pkey = n.upper() + suffix
                if pkey not in self.g:
                    self.g[pkey] = self._conn_default(n, fsrc, ftyp)
                params.insert(k, (pkey, ftyp, f"Venus {idx} {flabel}", flo, fhi, fstep, None, ficon, "config"))
                k += 1
            # FIX 48: Grenzwerte je Batterie
            for suffix, ftyp, flabel, flo, fhi, fstep, funit, ficon, fsrc in LIMIT_FIELDS:
                pkey = n.upper() + suffix
                if pkey not in self.g:
                    v = ((self.g.get('BATTERY_CONFIG') or {}).get(n) or {}).get(fsrc)
                    if v is None:
                        continue
                    self.g[pkey] = round(float(v), 2) if isinstance(fstep, float) else int(round(float(v)))
                params.insert(k, (pkey, ftyp, f'Venus {idx} {flabel}', flo, fhi, fstep, funit, ficon, 'config'))
                k += 1
        self.params = [p for p in params if p[0] in self.g]
        self.spec = {p[0]: p for p in self.params}
        self.defaults = {k: self.g.get(k) for k in self.spec}
        self.overrides = {}
        self.pending = {}
        self.actions = []
        self.form = {f[0]: self._form_default(f) for f in FORM_FIELDS}
        self.form["delete_target"] = self.bat_names[-1] if self.bat_names else ""
        self.status = "bereit"
        self._confirm = {}
        self.lock = threading.Lock()
        self.client = None
        self.connected = False

    # ------------------------------------------------------------------ utils
    def _idx(self, name):
        meta = (self.g.get("BATTERY_META") or {}).get(name, {})
        if meta.get("idx"):
            return int(meta["idx"])
        m = re.search(r"(\d+)$", name)
        return int(m.group(1)) if m else 0

    def _comm_default(self, n):
        """FIX 31: Standard-Auswahl aus comm_mode der Batterie und COMM_MODE_FALLBACK_ENABLE."""
        cfg = (self.g.get("BATTERY_CONFIG") or {}).get(n, {})
        mode = str(cfg.get("comm_mode", "modbus")).lower().strip()
        fb = bool(self.g.get("COMM_MODE_FALLBACK_ENABLE", False))
        if mode == "api":
            return "API, sonst Modbus" if fb else "API"
        return "Modbus, sonst API" if fb else "Modbus"
    def _conn_default(self, n, src, typ):
        """FIX 36: Startwert eines Verbindungsfelds aus BATTERY_CONFIG."""
        cfg = (self.g.get("BATTERY_CONFIG") or {}).get(n, {})
        if src == "modbus_host":
            v = cfg.get("modbus_host", cfg.get("host", ""))
        else:
            v = cfg.get(src, {"port": 502, "unit": 1, "api_port": 30000}.get(src, ""))
        if typ == "text":
            return str(v or "")
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return {"port": 502, "unit": 1, "api_port": 30000}.get(src, 0)
    def _form_default(self, f):
        key, typ = f[0], f[1]
        dflt = f[8]
        first = (self.g.get("BATTERY_CONFIG") or {})
        first = first[sorted(first)[0]] if first else {}
        src = {"new_port": "port", "new_capacity_kwh": "capacity_kwh",
               "new_max_charge_w": "max_charge_w", "new_max_discharge_w": "max_discharge_w",
               "new_min_soc": "min_soc", "new_max_soc": "max_soc"}.get(key)
        if src and src in first:
            return first[src]
        if typ == "select":
            return dflt[0]
        return dflt

    def log(self, msg):
        try:
            self.app.log(f"MQTT-BRIDGE: {msg}")
        except Exception:
            pass

    def _set_status(self, txt):
        self.status = str(txt)[:250]
        self.log("VERWALTUNG: " + self.status)
        c = self.client
        if c and self.connected:
            c.publish(f"{BASE}/admin_status/state", self.status, qos=1, retain=True)

    def _validate(self, key, raw):
        _k, typ, _n, lo, hi, step, _u, _i, _c = self.spec[key]
        if typ == "switch":
            return _to_bool(raw)
        if typ == "text":  # FIX 36
            v = str(raw).strip()[:128 if key.endswith('_SENSOR') else 64]
            if key.endswith('_SENSOR'):  # FIX 46: Entity-ID oder leer
                return v if (v == '' or re.match(r'^[a-z_]+\.[a-z0-9_]+$', v)) else str(self.g.get(key) or '')
            if key.endswith("_HOST"):
                return v if _valid_ip(v) else str(self.g.get(key) or "")
            return v
        if typ == "select":  # FIX 31
            opts = list(lo or [])
            cur = self.g.get(key)
            return raw if raw in opts else (cur if cur in opts else opts[0])
        v = float(raw)
        v = max(float(lo), min(float(hi), v))
        if isinstance(step, int) and float(step).is_integer():
            v = int(round(v))
        return v

    def _current(self, key):
        with self.lock:
            if key in self.pending:
                return self.pending[key]
        return self.g.get(key)

    def _payload(self, key, value):
        if self.spec[key][1] == "switch":
            return "ON" if value else "OFF"
        return str(value)

    def _backup(self, path, tag):
        try:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            if os.path.exists(path):
                dst = os.path.join(BACKUP_DIR, os.path.basename(path) + ".bak_" +
                                   time.strftime("%Y%m%d_%H%M%S") + "_" + tag)
                shutil.copy2(path, dst)
                return dst
        except Exception as e:
            self.log(f"Sicherung von {path} fehlgeschlagen: {e}")
        return None

    def _load_overrides(self):
        try:
            with open(OVERRIDE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {k: self._validate(k, v) for k, v in data.items() if k in self.spec}
        except FileNotFoundError:
            return {}
        except Exception as e:
            self.log(f"Overrides nicht lesbar ({e}) - ignoriert")
            return {}

    def _save_overrides(self):
        try:
            tmp = OVERRIDE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.overrides, f, indent=2)
            os.replace(tmp, OVERRIDE_FILE)
        except Exception as e:
            self.log(f"Overrides speichern fehlgeschlagen: {e}")

    def _read_batteries_file(self):
        try:
            with open(BATTERY_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            return {"add": dict(d.get("add") or {}), "remove": list(d.get("remove") or [])}
        except FileNotFoundError:
            return {"add": {}, "remove": []}

    def _write_batteries_file(self, d):
        self._backup(BATTERY_FILE, "vorAenderung")
        tmp = BATTERY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, BATTERY_FILE)

    def _restart_script(self):
        # AppDaemon laedt die App neu, sobald sich pv_control.py aendert (mtime)
        now = time.time()
        os.utime(CONTROL_FILE, (now, now))

    # ------------------------------------------------------------- lifecycle
    def start(self):
        self.overrides = self._load_overrides()
        with self.lock:
            self.pending.update(self.overrides)
        self.apply_pending()
        if self.overrides:
            self.log(f"gespeicherte Werte aktiv: {self.overrides}")

        if mqtt is None:
            self.log("paho-mqtt nicht installiert - nur pv_config-Werte")
            return
        try:
            with open(LOGIN_FILE, "r", encoding="utf-8") as f:
                login = json.load(f)
        except FileNotFoundError:
            self.log(f"{os.path.basename(LOGIN_FILE)} fehlt - Dashboard-Steuerung aus")
            return
        except Exception as e:
            self.log(f"Login-Datei fehlerhaft ({e}) - Dashboard-Steuerung aus")
            return

        host = login.get("host", "core-mosquitto")
        port = int(login.get("port", 1883))
        try:
            try:
                c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="appdaemon_pv_control")
            except AttributeError:
                c = mqtt.Client(client_id="appdaemon_pv_control")
            if login.get("user"):
                c.username_pw_set(login.get("user"), login.get("password") or None)
            c.will_set(f"{BASE}/status", "offline", qos=1, retain=True)
            c.on_connect = self._on_connect
            c.on_disconnect = self._on_disconnect
            c.on_message = self._on_message
            c.reconnect_delay_set(min_delay=5, max_delay=120)
            c.connect_async(host, port, keepalive=60)
            c.loop_start()
            self.client = c
            self.log(f"verbinde mit {host}:{port} ...")
        except Exception as e:
            self.log(f"Start fehlgeschlagen: {e}")

    def stop(self):
        c = self.client
        self.client = None
        if not c:
            return
        try:
            c.publish(f"{BASE}/status", "offline", qos=1, retain=True)
            time.sleep(0.2)
            c.loop_stop()
            c.disconnect()
        except Exception:
            pass

    # --------------------------------------------------------- mqtt callbacks
    def _on_connect(self, client, userdata, flags, rc, *args):
        code = getattr(rc, "value", rc)
        if code != 0:
            self.connected = False
            self.log(f"Verbindung abgelehnt (rc={rc}) - Login pruefen")
            return
        self.connected = True
        self.log("verbunden - melde Entitaeten an")
        try:
            self._publish_discovery(client)
            client.subscribe(f"{BASE}/+/set", qos=1)
            client.publish(f"{BASE}/status", "online", qos=1, retain=True)
            for k in self.spec:
                self._publish_state(k, self._current(k))
            self._publish_overrides()
            self._publish_form()
            client.publish(f"{BASE}/admin_status/state", self.status, qos=1, retain=True)
            # leere Formularfelder nochmal senden, wenn HA die Entitaeten angelegt hat
            t = threading.Timer(5.0, self._publish_form)
            t.daemon = True
            t.start()
        except Exception as e:
            self.log(f"Discovery fehlgeschlagen: {e}")

    def _on_disconnect(self, client, userdata, *args):
        if self.connected:
            self.log("Verbindung getrennt - versuche erneut")
        self.connected = False

    def _confirmed(self, key, what):
        now = time.time()
        if now - self._confirm.get(key, 0) <= CONFIRM_S:
            self._confirm.pop(key, None)
            return True
        self._confirm[key] = now
        self._set_status(f"{what}: zum Bestaetigen innerhalb von {CONFIRM_S} s nochmal druecken")
        return False

    def _on_message(self, client, userdata, msg):
        try:
            key = msg.topic.split("/")[1]
            raw = msg.payload.decode("utf-8", "ignore").strip()
            if key == RESET_KEY:
                self._reset()
                return
            if key == "apply_config":
                self._queue("apply_config")
                return
            if key == "factory_reset":
                if self._confirmed(key, "Werksreset"):
                    self._queue("factory_reset")
                return
            if key == "add_battery":
                self._queue("add_battery")
                return
            if key == "delete_battery":
                tgt = self.form.get("delete_target")
                if self._confirmed(key, f"{tgt} loeschen"):
                    self._queue("delete_battery")
                return
            if key in self.form or key == "delete_target":
                self._set_form(key, raw)
                return
            if key not in self.spec:
                return
            val = self._validate(key, raw)
            with self.lock:
                self.pending[key] = val
            if val == self.defaults.get(key):
                self.overrides.pop(key, None)
            else:
                self.overrides[key] = val
            self._save_overrides()
            self._publish_state(key, val)
            self._publish_overrides()
            self.log(f"{key} -> {val} (ab naechstem Zyklus)")
        except Exception as e:
            self.log(f"Befehl {msg.topic}={msg.payload!r} ungueltig: {e}")

    def _queue(self, action):
        with self.lock:
            if action not in self.actions:
                self.actions.append(action)
        self._set_status(f"{action}: wird im naechsten Regelzyklus ausgefuehrt ...")

    def _set_form(self, key, raw):
        if key == "delete_target":
            if raw in self.bat_names:
                self.form[key] = raw
        else:
            f = next(x for x in FORM_FIELDS if x[0] == key)
            typ, lo, hi, step = f[1], f[3], f[4], f[5]
            if typ == "number":
                v = max(float(lo), min(float(hi), float(raw)))
                self.form[key] = int(round(v)) if float(step).is_integer() else round(v, 2)
            elif typ == "select":
                if raw in f[8]:
                    self.form[key] = raw
            else:
                self.form[key] = raw[:64]
        c = self.client
        if c and self.connected:
            self._pub_form_value(c, key, self.form[key])

    # ---------------------------------------------------------------- publish
    def _publish_discovery(self, client):
        avail = [{"topic": f"{BASE}/status"}]
        for key, typ, name, lo, hi, step, unit, icon, cat in self.params:
            cfg = {
                "name": name,
                "unique_id": f"marstek_ctrl_{key.lower()}",
                "default_entity_id": f"{typ}.marstek_{key.lower()}",
                "state_topic": f"{BASE}/{key}/state",
                "command_topic": f"{BASE}/{key}/set",
                "availability": avail,
                "device": DEVICE,
                "icon": icon,
                "retain": False,
            }
            if cat:
                cfg["entity_category"] = cat
            if typ == "switch":
                cfg.update({"payload_on": "ON", "payload_off": "OFF",
                            "state_on": "ON", "state_off": "OFF"})
            elif typ == "text":  # FIX 36
                cfg.update({"min": 0, "max": 128 if key.endswith("_SENSOR") else 64, "mode": "text"})
            elif typ == "select":  # FIX 31
                cfg["options"] = list(lo)
            else:
                cfg.update({"min": lo, "max": hi, "step": step, "mode": "box"})
                if unit:
                    cfg["unit_of_measurement"] = unit
            client.publish(f"{DISCOVERY}/{typ}/{BASE}/{key.lower()}/config",
                           json.dumps(cfg), qos=1, retain=True)
        # Schalter nicht (mehr) vorhandener Batterien entfernen
        present = {n.upper() + "_ACTIVE" for n in self.bat_names}
        for i in range(1, MAX_BATTERY_SLOTS + 1):
            k = f"BATTERY{i}_ACTIVE"
            if k not in present:
                client.publish(f"{DISCOVERY}/switch/{BASE}/{k.lower()}/config", "", qos=1, retain=True)
                client.publish(f"{DISCOVERY}/select/{BASE}/battery{i}_comm/config", "", qos=1, retain=True)  # FIX 31
                for _sfx, _t, *_r in CONN_FIELDS + LIMIT_FIELDS:  # FIX 36/48
                    client.publish(f"{DISCOVERY}/{_t}/{BASE}/battery{i}{_sfx.lower()}/config", "", qos=1, retain=True)

        for key, name, icon, cat in BUTTONS:
            client.publish(f"{DISCOVERY}/button/{BASE}/{key}/config", json.dumps({
                "name": name,
                "unique_id": f"marstek_ctrl_{key}",
                "default_entity_id": f"button.marstek_{key}",
                "command_topic": f"{BASE}/{key}/set",
                "payload_press": "PRESS",
                "availability": avail,
                "device": DEVICE,
                "icon": icon,
                "entity_category": cat,
            }), qos=1, retain=True)

        # Formular "Neue Batterie" + Loesch-Auswahl
        for key, typ, name, lo, hi, step, unit, icon, dflt in FORM_FIELDS:
            cfg = {
                "name": name,
                "unique_id": f"marstek_ctrl_{key}",
                "default_entity_id": f"{typ}.marstek_{key}",
                "state_topic": f"{BASE}/{key}/state",
                "command_topic": f"{BASE}/{key}/set",
                "availability": avail,
                "device": DEVICE,
                "icon": icon,
                "entity_category": "config",
            }
            if typ == "number":
                cfg.update({"min": lo, "max": hi, "step": step, "mode": "box"})
                if unit:
                    cfg["unit_of_measurement"] = unit
            elif typ == "select":
                cfg["options"] = list(dflt)
            elif typ == "text":
                cfg.update({"min": 0, "max": 64})
            client.publish(f"{DISCOVERY}/{typ}/{BASE}/{key}/config", json.dumps(cfg), qos=1, retain=True)
        client.publish(f"{DISCOVERY}/select/{BASE}/delete_target/config", json.dumps({
            "name": "Zu loeschende Batterie",
            "unique_id": "marstek_ctrl_delete_target",
            "default_entity_id": "select.marstek_delete_target",
            "state_topic": f"{BASE}/delete_target/state",
            "command_topic": f"{BASE}/delete_target/set",
            "options": list(self.bat_names) or ["-"],
            "availability": avail,
            "device": DEVICE,
            "icon": "mdi:battery-remove-outline",
            "entity_category": "config",
        }), qos=1, retain=True)

        client.publish(f"{DISCOVERY}/sensor/{BASE}/admin_status/config", json.dumps({
            "name": "Verwaltung Status",
            "unique_id": "marstek_ctrl_admin_status",
            "default_entity_id": "sensor.marstek_admin_status",
            "state_topic": f"{BASE}/admin_status/state",
            "availability": avail,
            "device": DEVICE,
            "icon": "mdi:information-outline",
            "entity_category": "diagnostic",
        }), qos=1, retain=True)

        client.publish(f"{DISCOVERY}/sensor/{BASE}/overrides/config", json.dumps({
            "name": "Abweichungen von pv_config",
            "unique_id": "marstek_ctrl_overrides",
            "default_entity_id": "sensor.marstek_overrides",
            "state_topic": f"{BASE}/overrides/state",
            "value_template": "{{ value_json.count }}",
            "json_attributes_topic": f"{BASE}/overrides/state",
            "json_attributes_template": "{{ dict(value_json['values'], original=value_json['defaults']) | tojson }}",
            "availability": avail,
            "device": DEVICE,
            "icon": "mdi:file-compare",
            "entity_category": "diagnostic",
        }), qos=1, retain=True)

    def _publish_state(self, key, value):
        c = self.client
        if c and self.connected:
            c.publish(f"{BASE}/{key}/state", self._payload(key, value), qos=1, retain=True)

    def _publish_form(self):
        c = self.client
        if c and self.connected:
            for k, v in self.form.items():
                self._pub_form_value(c, k, v)

    def _pub_form_value(self, c, k, v):
        v = str(v)
        if v:
            c.publish(f"{BASE}/{k}/state", v, qos=1, retain=True)
        else:
            c.publish(f"{BASE}/{k}/state", "", qos=1, retain=True)   # alten retained Wert loeschen
            c.publish(f"{BASE}/{k}/state", "", qos=1, retain=False)  # HA zeigt leeres Feld

    def _publish_overrides(self):
        c = self.client
        if c and self.connected:
            c.publish(f"{BASE}/overrides/state",
                      json.dumps({"count": len(self.overrides), "values": self.overrides,
                                  "defaults": self.defaults}),
                      qos=1, retain=True)

    def _reset(self):
        with self.lock:
            self.pending.update(self.defaults)
        self.overrides = {}
        self._save_overrides()
        for k in self.spec:
            self._publish_state(k, self.defaults[k])
        self._publish_overrides()
        self._set_status("Aenderungen verworfen - Werte aus pv_config.py aktiv")

    # ------------------------------------------------------ Verwaltungs-Aktionen
    def _do_apply_config(self):
        if not self.overrides:
            self._set_status("Uebernehmen: keine Aenderungen vorhanden")
            return
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            text = f.read()
        bak = self._backup(CONFIG_FILE, "vorUebernehmen")
        done, added = [], []
        for key, val in self.overrides.items():
            pat = re.compile(r"^(%s[ \t]*=[ \t]*)([^#\n]*?)([ \t]*(#[^\n]*)?)$" % re.escape(key), re.M)
            hits = pat.findall(text)
            if len(hits) == 1:
                text = pat.sub(lambda m: m.group(1) + _py_literal(val) + m.group(3), text, count=1)
                done.append(key)
            elif not hits:
                text = text.rstrip("\n") + f"\n{key} = {_py_literal(val)}  # per Dashboard uebernommen\n"
                added.append(key)
            else:
                self.log(f"Uebernehmen: {key} mehrfach in pv_config.py - uebersprungen")
        compile(text, CONFIG_FILE, "exec")
        tmp = CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, CONFIG_FILE)
        for key in done + added:
            self.defaults[key] = self.overrides[key]
        self.overrides = {k: v for k, v in self.overrides.items() if k not in done + added}
        self._save_overrides()
        self._publish_overrides()
        self._set_status(f"Uebernommen in pv_config.py: {', '.join(done + added)} (Sicherung: {os.path.basename(bak or '-')})")

    def _stop_battery(self, name):
        drv = (getattr(self.app, "_drivers", {}) or {}).get(name)
        if drv is None:
            return
        try:
            drv.write_setpoints(0, 0, 0)
            self.log(f"{name}: STOP vor Entfernen gesendet")
        except Exception as e:
            self.log(f"{name}: STOP fehlgeschlagen: {e}")

    def _do_factory_reset(self):
        if not os.path.exists(WERK_FILE):
            self._set_status("Werksreset nicht moeglich: pv_config.werk fehlt")
            return
        with open(WERK_FILE, "r", encoding="utf-8") as f:
            werk = f.read()
        compile(werk, WERK_FILE, "exec")
        added = list(self._read_batteries_file().get("add", {}).keys())
        for n in added:
            self._stop_battery(n)
        self._backup(CONFIG_FILE, "vorWerksreset")
        self._backup(OVERRIDE_FILE, "vorWerksreset")
        tmp = CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(werk)
        os.replace(tmp, CONFIG_FILE)
        if os.path.exists(BATTERY_FILE):
            self._backup(BATTERY_FILE, "vorWerksreset")
            os.remove(BATTERY_FILE)
        self.overrides = {}
        self._save_overrides()
        self._set_status("Werksreset ausgefuehrt - Script startet neu")
        self._restart_script()

    def _do_add_battery(self):
        f = self.form
        mode = f.get("new_comm_mode", "modbus")
        mh, ah = str(f.get("new_modbus_host", "")).strip(), str(f.get("new_api_host", "")).strip()
        mh, ah = (mh if _valid_ip(mh) else ""), (ah if _valid_ip(ah) else "")
        if mode == "modbus" and not _valid_ip(mh):
            self._set_status("Anlegen abgebrochen: gueltige IP des LAN-Adapters eintragen")
            return
        if mode == "api" and not _valid_ip(ah):
            self._set_status("Anlegen abgebrochen: gueltige WLAN-IP der Batterie eintragen")
            return
        cfgs = self.g.get("BATTERY_CONFIG") or {}
        if int(f["new_min_soc"]) >= int(f["new_max_soc"]):
            self._set_status("Anlegen abgebrochen: Min-SoC muss kleiner als Max-SoC sein")
            return
        for n, c in cfgs.items():
            if mode == "api" and c.get("comm_mode") == "api" and c.get("api_host") == ah:
                self._set_status(f"Anlegen abgebrochen: {ah} ist schon {n}")
                return
        unit = int(f.get("new_unit") or 0)
        if mode == "modbus":
            # Slave-IDs, die an diesem Adapter schon vergeben sind
            used_units = [int(c.get("unit", 1)) for c in cfgs.values()
                          if c.get("comm_mode", "modbus") == "modbus"
                          and (c.get("modbus_host") or c.get("host")) == mh
                          and int(c.get("port", 502)) == int(f["new_port"])]
            if unit and unit in used_units:
                self._set_status(f"Anlegen abgebrochen: {mh} Slave-ID {unit} ist schon vergeben")
                return
            self._set_status(f"Suche Batterie an {mh}:{f['new_port']} "
                             + (f"(Slave-ID {unit}) ..." if unit else "(Slave-ID automatisch) ..."))
            found, info = self._probe_modbus(mh, int(f["new_port"]), unit, used_units)
            if found is None:
                self._set_status("Anlegen abgebrochen: " + info)
                return
            unit = found
            self.log(f"Anlegen: {mh}:{f['new_port']} Slave-ID {unit} gefunden ({info})")
        else:
            unit = unit or 1
        used = [self._idx(n) for n in cfgs] + [int(re.search(r"(\d+)$", n).group(1))
                                               for n in self._read_batteries_file()["remove"]
                                               if re.search(r"(\d+)$", n)]
        nr = 1
        while nr in used:
            nr += 1
        if nr > MAX_BATTERY_SLOTS:
            self._set_status(f"Anlegen abgebrochen: maximal {MAX_BATTERY_SLOTS} Batterien")
            return
        name = f"battery{nr}"
        new = {
            "enabled": True,
            "comm_mode": mode,
            "modbus_host": mh or "0.0.0.0",
            "api_host": ah or "0.0.0.0",
            "port": int(f["new_port"]),
            "unit": int(unit),
            "api_port": int(f["new_api_port"]),
            "api_local_port": 40000 + nr,
            "api_cd_time": 300,
            "capacity_kwh": float(f["new_capacity_kwh"]),
            "max_charge_w": int(f["new_max_charge_w"]),
            "max_discharge_w": int(f["new_max_discharge_w"]),
            "min_soc": int(f["new_min_soc"]),
            "max_soc": int(f["new_max_soc"]),
        }
        d = self._read_batteries_file()
        d["add"][name] = new
        d["remove"] = [x for x in d["remove"] if x != name]
        self._write_batteries_file(d)
        where = f"Modbus {mh}:{new['port']} Slave-ID {unit}" if mode == "modbus" else f"API {ah}"
        self._set_status(f"{name} angelegt ({where}) - Script startet neu")
        self._restart_script()

    def _probe_modbus(self, host, port, unit, used_units):
        """Sucht die Slave-ID einer Marstek am Modbus-TCP-Adapter.
        unit > 0: nur diese ID pruefen. unit == 0: PROBE_UNITS durchprobieren
        (IDs, die an diesem Adapter schon vergeben sind, werden uebersprungen).
        Rueckgabe: (slave_id, info) oder (None, fehlertext)."""
        regs = self.g.get("REGISTERS") or {}
        soc_reg = int(regs.get("soc", 32104))
        # laeuft schon eine Batterie am gleichen Adapter? -> deren Verbindung mitbenutzen
        # (viele RS485-LAN-Adapter erlauben nur eine TCP-Verbindung)
        shared, own = None, None
        for n, drv in (getattr(self.app, "_drivers", {}) or {}).items():
            cfg = getattr(drv, "cfg", {}) or {}
            if (cfg.get("modbus_host") or cfg.get("host")) == host and int(cfg.get("port", 502)) == int(port) \
                    and hasattr(drv, "_mk_client"):
                shared = drv
                break
        if shared is None:
            if ModbusTcpClient is None:
                return None, "pymodbus fehlt"
            if not _tcp_open(host, port):
                return None, f"{host}:{port} nicht erreichbar (IP / Port / Adapter pruefen)"
            try:
                own = ModbusTcpClient(host, port=int(port), timeout=PROBE_TIMEOUT_S, retries=0)
            except TypeError:
                own = ModbusTcpClient(host, port=int(port), timeout=PROBE_TIMEOUT_S)
            if not own.connect():
                return None, f"{host}:{port} - Modbus-Verbindung fehlgeschlagen"
            client = own
        else:
            client = shared._mk_client()
            if hasattr(client, "connected") and not client.connected:
                client.connect()
        cands = [unit] if unit else [u for u in PROBE_UNITS if u not in used_units]
        t0 = time.time()
        tried = 0
        try:
            for u in cands:
                if time.time() - t0 > PROBE_BUDGET_S:
                    break
                tried += 1
                try:
                    rr = _mb_read(client, soc_reg, u)
                except Exception:
                    rr = None
                if _mb_ok(rr) and 0 <= int(rr.registers[0]) <= 100:
                    return u, f"SoC {int(rr.registers[0])} %"
        finally:
            if own is not None:
                try:
                    own.close()
                except Exception:
                    pass
        if unit:
            return None, f"{host}:{port} Slave-ID {unit} antwortet nicht (Slave-ID auf 0 = automatisch stellen?)"
        return None, (f"{host}:{port} erreichbar, aber keine Marstek gefunden "
                      f"({tried} Slave-IDs probiert: {cands[0]}..{cands[tried - 1] if tried else '-'})")

    def _do_delete_battery(self):
        name = self.form.get("delete_target")
        if name not in (self.g.get("BATTERY_CONFIG") or {}):
            self._set_status(f"Loeschen abgebrochen: {name} unbekannt")
            return
        if len(self.g.get("BATTERY_CONFIG") or {}) <= 1:
            self._set_status("Loeschen abgebrochen: die letzte Batterie kann nicht geloescht werden")
            return
        self._stop_battery(name)
        d = self._read_batteries_file()
        if name in d["add"]:
            d["add"].pop(name)
        elif name not in d["remove"]:
            d["remove"].append(name)
        self._write_batteries_file(d)
        self._set_status(f"{name} geloescht (bekam vorher STOP) - Script startet neu")
        self._restart_script()

    # ------------------------------------------------------ im App-Thread
    def apply_pending(self):
        """Zu Beginn jedes Regelzyklus aufrufen - setzt geaenderte Werte und fuehrt Aktionen aus."""
        with self.lock:
            items = dict(self.pending)
            self.pending.clear()
            actions = list(self.actions)
            self.actions.clear()
        for k, v in items.items():
            old = self.g.get(k)
            if old != v:
                self.g[k] = v
                self.log(f"angewendet: {k} {old} -> {v}")
        if items:
            lo = self.g.get("SOC_SAFETY_MIN_SOC")
            tgt = self.g.get("SOC_SAFETY_TARGET_SOC")
            if isinstance(lo, (int, float)) and isinstance(tgt, (int, float)) and tgt <= lo:
                self.g["SOC_SAFETY_TARGET_SOC"] = lo + 1
                self.log(f"Ziel-SoC {tgt} <= Min-SoC {lo} -> auf {lo + 1} angehoben")
                self._publish_state("SOC_SAFETY_TARGET_SOC", lo + 1)
        for a in actions:
            try:
                getattr(self, "_do_" + a)()
            except Exception as e:
                self._set_status(f"{a} fehlgeschlagen: {e}")
