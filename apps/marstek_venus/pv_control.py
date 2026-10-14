# -----------------------------------------------------------------------------
# Marstek Venus – Home Assistant / AppDaemon Skript
#
# Beschreibung:
# Dieses AppDaemon-Skript dient zur intelligenten Steuerung von
# Marstek-Venus-Batteriesystemen in Home Assistant ueber Modbus oder
# die Marstek Open API (UDP/JSON-RPC Rev 1.0).
#
# Konfiguration: pv_control_config.py
#   Alle Einstellungen werden aus der externen Konfigurationsdatei
#   geladen. Neue Eintraege in pv_control_config.py werden automatisch
#   durch "from pv_config import *" uebernommen.
#
# Autor: Neue Medien Becker
# Lizenz: MIT License
# -----------------------------------------------------------------------------

"""
pv_control.py

Dynamische Lade-, Entlade- und Stopp-Steuerung fuer Marstek-Venus-Batteriesysteme.

Technische Merkmale:
- ASCII-only (keine Umlaute)
- Restart-sicheres Verhalten
- Klare Trennung von Steuerlogik, Schutzmechanismen und Logging
- Vollstaendig eigenstaendig lauffaehig innerhalb von AppDaemon
- Modbus TCP und Marstek Open API als austauschbare Treiber

Aenderungen:
- FIX 1: Register 32302 (ac_offgrid_power) aus HA_REGMAP entfernt.
- FIX 2: Control (42000) wird jetzt beim Start in initialize() freigeschaltet.
- FIX 3: Control-Freischaltung auch im Read-Loop vor dem Lesen von 42010.
- FIX 4: self._busy = False in den finally-Block verschoben.
- FIX 5: self.pv_surplus, self.pv_pending, self.allow_charge in initialize().
- FIX 6: Treiber-Abstraktion (ModbusDriver / ApiDriver) direkt integriert.
- FIX 7: Alle Konfigurationseinstellungen in pv_control_config.py ausgelagert.
- FIX 8: SOC-Safety min_soc_since Tracking war komplett fehlend (nie getriggert).
- FIX 9: safety_done wurde nie zurueckgesetzt (einmaliger Schutz fuer immer).
- FIX 10: BMS-Kandidat ignorierte Lesefehler (err=1) bei der Auswahl.
- FIX 11: ApiDriver: ES.GetStatus wurde doppelt pro Zyklus aufgerufen
          (HA-Export + read_state). Jetzt mit kurzem Cache (TTL < INTERVAL).
- FIX 12: ApiDriver: kein STOP beim Start - Batterie lief nach Neustart
          mit altem Setpoint weiter. init_control sendet jetzt power=0.
- FIX 13: BMS-Zeitplan-Log jede 20s zu verbose. Jetzt nur alle 5 Minuten.
- FIX 14: Falscher Kommentar "immer per Modbus" korrigiert.
- FIX 15: COMM_MODE_FALLBACK: automatischer Treiberwechsel bei Verbindungsausfall.
          API nicht erreichbar -> Wechsel auf Modbus (und umgekehrt).
          Nach COMM_MODE_FALLBACK_RETRY_MIN Minuten Rueckwechselversuch auf Primaer-Treiber.
- FIX 31: Anbindung je Batterie im Dashboard waehlbar (BATTERYn_COMM: Modbus / API /
          mit Ausweichweg). Beide Wege tot -> Batterie bekommt keinen Anteil (FIX 30).
- FIX 33: API-Treiber robust: Socket bleibt offen, kurze Timeouts mit schneller Wiederholung,
          letzte Werte bei Aussetzern, Sollwert-Auffrischung vor cd_time, ES.GetMode fuer neue Firmware.
- FIX 34: ein gemeinsamer UDP-Socket (API_LOCAL_PORT) fuer alle API-Batterien, Antworten nach IP verteilt.
- FIX 36: Verbindungsdaten je Batterie (Modbus-IP/Port/Slave-ID, API-IP/Port) im Dashboard aenderbar.
- FIX 37: API-Einheiten je Firmware (HW 2.0 ab FW 154: Temperatur x10, Energie x100), API-Zaehler auf Modbus-Entitaeten.
- FIX 38: API: zurueckgehaltene kleine Sollwert-Aenderungen werden bei der Auffrischung (<= 30 s) nachgeholt.
- FIX 39: Version 2.0.0 (GitHub). Netz-Zaehler: POWER_SENSOR "invert" (Vorzeichen drehen) und kW-Werte automatisch in W.
- FIX 40: Beobachtungsmodus READ_ONLY (nur lesen, keine Befehle an die Batterien), z. B. wenn noch eine andere Steuerung laeuft oder der Netz-Zaehler fehlt.
- FIX 42: Beim Start werden HA-Sensoren von Batterien, die es nicht mehr gibt (Dashboard: Batterie loeschen), entfernt.
- FIX 43: Ladeleistung von BMS-Pflege/Notladung wird aus dem Netzfluss herausgerechnet (andere Batterien entladen nicht, um die ladende Batterie zu fuettern).
- FIX 44: Nach einer Neuinstallation wartet die BMS-Pflege erst auf PV-Ueberschuss (BMS_CARE_PV_WAIT_H), statt sofort aus dem Netz voll zu laden.
- FIX 45: BMS-Pflege laedt nur mit dem PV-Ueberschuss, der gerade da ist (pausiert bei Wolken/nachts, nach BMS_CARE_PV_WAIT_H ohne Abschluss mit voller Leistung); geregelte Batterien nehmen der BMS-Batterie den Ueberschuss nicht weg und entladen nicht fuer sie.
- FIX 46: Netz-Zaehler (Sensor, Vorzeichen) und ein zweiter Speicher (Entlade-/Ladeleistung, PV) sind im Dashboard einstellbar (GRID_SENSOR, GRID_SENSOR_INVERT, EXT_STORAGE_*_SENSOR, EXT_PV_SENSOR); BMS-Pflege: 'max. Warten auf Sonne' zaehlt ab Faelligkeit, danach wird ohne PV durchgeladen.
- FIX 47: 'STOP wenn Zaehlerwert aelter als' (METER_MAX_AGE_S) ist im Dashboard einstellbar; sensor.marstek_meter_info zeigt Alter des Zaehlerwerts, typischen Abstand neuer Werte, STOPs der letzten 24 h und einen Vorschlag fuer die Wartezeit.
- FIX 48: Grenzwerte je Batterie (max. Lade-/Entladeleistung, Min-/Max-SoC, Kapazitaet) im Dashboard aenderbar (BATTERYn_MAX_CHARGE_W, _MAX_DISCHARGE_W, _MIN_SOC, _MAX_SOC, _CAPACITY_KWH); ohne Treiber-Neuaufbau.
- FIX 49: Gesamt-Energiezaehler (kWh) werden nur plausibel an Home Assistant gegeben: kein Ruecksprung, kein Sprung groesser als physikalisch moeglich (z. B. vertauschte Modbus-Antworten). Ein abweichender Wert wird erst uebernommen, wenn er mind. 10 min stabil bleibt. Sonst zaehlte HA Hunderte kWh 'heute'.
- FIX 50: hoechste/niedrigste Zellspannung (Modbus 37007/37008) und Unterschied in mV je Batterie (sensor.marstekvenus_N_cell_voltage_delta, nur Modbus); liegt der Unterschied >= BMS_CARE_CELL_DELTA_MV laenger an, wird die BMS-Pflege vorgezogen (nur mit PV-Ueberschuss, fruehestens nach BMS_CARE_CELL_MIN_DAYS).
- FIX 16: canonical_key in API_EXPORT_MAP: API und Modbus schreiben auf identische
          HA-Entity-IDs. Treiberwechsel fuer HA vollstaendig unsichtbar.
          z.B.: es_bat_soc->battery_soc, es_ongrid_power->ac_power,
                bat_temp->temp_internal, bat_capacity->battery_energy.
"""

import os
import sys
import importlib
import importlib.util
import json
import re  # FIX 37
import struct
import time
import socket
import random
import threading
from datetime import datetime
from appdaemon.plugins.hass.hassapi import Hass
from pymodbus.client import ModbusTcpClient
PV_CONTROL_VERSION = "2.0.18"  # FIX 39

# Config direkt per Dateipfad laden – unabhaengig von sys.path und __pycache__.
# Laedt immer die pv_control_config.py die im gleichen Verzeichnis wie pv_control.py liegt.
sys.dont_write_bytecode = True
_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
_CFG_FILE  = os.path.join(_THIS_DIR, "pv_config.py")
_cfg_spec  = importlib.util.spec_from_file_location("pv_config", _CFG_FILE)
_pv_cfg    = importlib.util.module_from_spec(_cfg_spec)
_cfg_spec.loader.exec_module(_pv_cfg)       # laedt direkt vom Dateisystem, kein sys.path-Lookup
sys.modules["pv_config"] = _pv_cfg  # fuer spaetere Referenzen registrieren
from pv_config import *             # noqa: F403,F401  <- ALLE Einstellungen kommen von hier

# FIX 40: Beobachtungsmodus (Standard aus, falls eine alte pv_config.py ihn nicht kennt)
if "READ_ONLY" not in globals():
    READ_ONLY = False

# FIX 46: Zaehler und zweiter Speicher - Standard aus POWER_SENSOR, im Dashboard aenderbar
if "GRID_SENSOR" not in globals():
    GRID_SENSOR = str((POWER_SENSOR or {}).get("entity") or "")
if "GRID_SENSOR_INVERT" not in globals():
    GRID_SENSOR_INVERT = bool((POWER_SENSOR or {}).get("invert"))
for _k46 in ("EXT_STORAGE_DISCHARGE_SENSOR", "EXT_STORAGE_CHARGE_SENSOR", "EXT_PV_SENSOR"):
    if _k46 not in globals():
        globals()[_k46] = ""

# FIX 50: Zellspannungen (nur Modbus) - Standardwerte und Register, falls eine alte pv_config.py sie nicht kennt
for _k50, _v50 in (("BMS_CARE_CELL_DELTA_MV", 100), ("BMS_CARE_CELL_DELTA_MIN_S", 1800), ("BMS_CARE_CELL_MIN_DAYS", 3)):
    if _k50 not in globals():
        globals()[_k50] = _v50
try:
    HA_REGMAP.setdefault("cell_voltage_max", {"enabled": True, "addr": 37007, "cnt": 1, "type": "u16", "scale": 0.001, "unit": "V", "prec": 3, "name": "Cell Voltage Max"})
    HA_REGMAP.setdefault("cell_voltage_min", {"enabled": True, "addr": 37008, "cnt": 1, "type": "u16", "scale": 0.001, "unit": "V", "prec": 3, "name": "Cell Voltage Min"})
except NameError:
    pass

# Rueckwaertskompatibilitaet: neue Spalten auch bei alter config sicherstellen.
# Dieser Block ist idempotent – bei aktueller Config passiert nichts.
# -----------------------------------------------------------------------------
if "drv" not in COL_WIDTH:
    COL_WIDTH = dict(COL_WIDTH)
    COL_WIDTH["drv"] = 5
if "drv" not in list(COLUMNS):
    _ins = list(COLUMNS).index("zustand") + 1 if "zustand" in list(COLUMNS) else len(COLUMNS)
    COLUMNS = list(COLUMNS[:_ins]) + ["drv"] + list(COLUMNS[_ins:])
    HEADERS = list(HEADERS[:_ins]) + ["Drv"]  + list(HEADERS[_ins:])

# FIX 15: Fallback-Defaults fuer alte Config ohne diese Variablen
if "COMM_MODE_FALLBACK_ENABLE" not in dir():
    COMM_MODE_FALLBACK_ENABLE    = False   # sicherster Default: aus
if "COMM_MODE_FALLBACK_FAILURES" not in dir():
    COMM_MODE_FALLBACK_FAILURES  = 3
if "COMM_MODE_FALLBACK_RETRY_MIN" not in dir():
    COMM_MODE_FALLBACK_RETRY_MIN = 10
# FIX 31: Auswahl der Anbindung je Batterie (Dashboard-Select BATTERYn_COMM).
# Text -> (Vorrangweg, Ausweichweg oder None). Texte muessen zu COMM_OPTIONS in pv_mqtt.py passen.
COMM_CHOICES = {
    "Modbus": ("modbus", None),
    "API": ("api", None),
    "Modbus, sonst API": ("modbus", "api"),
    "API, sonst Modbus": ("api", "modbus"),
}

def _comm_choice_default(cfg):
    """Standard-Auswahl aus comm_mode der Batterie und COMM_MODE_FALLBACK_ENABLE ableiten."""
    mode = str(cfg.get("comm_mode", COMM_MODE_DEFAULT)).lower().strip()
    fb = bool(globals().get("COMM_MODE_FALLBACK_ENABLE", False))
    if mode == "api":
        return "API, sonst Modbus" if fb else "API"
    return "Modbus, sonst API" if fb else "Modbus"
# FIX 36: Dashboard-Verbindungsfelder je Batterie -> Schluessel in BATTERY_CONFIG
CONN_PARAMS = (
    ("_MODBUS_HOST", "modbus_host", str),
    ("_MODBUS_PORT", "port", int),
    ("_MODBUS_UNIT", "unit", int),
    ("_API_HOST", "api_host", str),
    ("_API_PORT", "api_port", int),
)
# FIX 48: Grenzwerte je Batterie aus dem Dashboard -> Schluessel in BATTERY_CONFIG
LIMIT_PARAMS = (
    ("_MAX_CHARGE_W", "max_charge_w", int, 0, 10000),
    ("_MAX_DISCHARGE_W", "max_discharge_w", int, 0, 10000),
    ("_MIN_SOC", "min_soc", int, 0, 50),
    ("_MAX_SOC", "max_soc", int, 50, 100),
    ("_CAPACITY_KWH", "capacity_kwh", float, 0.5, 50),
)

# Defaults fuer neue API-Diagnose-Variablen (Rueckwaertskompatibilitaet)
if "API_LOCAL_PORT" not in dir():
    # 0 = OS-Ephemerport (altes Verhalten). Empfehlung: 30000 setzen (wie HA-Integration)
    API_LOCAL_PORT = 0
if "API_LOG_DIAG" not in dir():
    API_LOG_DIAG = False
if "API_INTER_CALL_DELAY_S" not in dir():
    API_INTER_CALL_DELAY_S = 0.3


# =============================================================================
# HILFSFUNKTIONEN
# =============================================================================

def _ascii_safe(s):
    return str(s).encode("ascii", "replace").decode("ascii")

def _i32_from_u16_be(words):
    if not words or len(words) != 2:
        return 0
    return struct.unpack(">i", struct.pack(">HH", words[0], words[1]))[0]

def _u32_from_u16_be(words):
    if not words or len(words) != 2:
        return 0
    return struct.unpack(">I", struct.pack(">HH", words[0], words[1]))[0]

def _s16_from_u16(word):
    return struct.unpack(">h", struct.pack(">H", int(word) & 0xFFFF))[0]

def _mb_client(host, port):
    try:
        return ModbusTcpClient(host, port=port, timeout=MB_TIMEOUT_S, retries=MB_RETRIES)
    except TypeError:
        return ModbusTcpClient(host, port=port, timeout=MB_TIMEOUT_S)

def _modbus_read_generic(client, unit, addr, count, log, name):
    prefix = f"[{name}] " if name else ""
    if MODBUS_LOG_TXRX:
        log(f"{prefix}[TX READ] addr={addr} cnt={count} device_id={unit}")
    try:
        rr = client.read_holding_registers(addr, count=count, device_id=unit)
        if rr is None:
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] None response")
            return None
        if hasattr(rr, "isError") and rr.isError():
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] {rr}")
            return None
        regs = getattr(rr, "registers", None)
        if regs is None:
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] keine 'registers'")
            return None
        if MODBUS_LOG_OK:
            log(f"{prefix}[RX OK] {regs}")
        time.sleep(0.03)
        return regs
    except Exception as e:
        if MODBUS_LOG_ERROR:
            log(f"{prefix}[RX FEHLER] Exception: {e}")
        return None

def _modbus_write_generic(client, unit, addr, value, log, name):
    prefix = f"[{name}] " if name else ""
    v = int(value)
    if globals().get("READ_ONLY", False):  # FIX 40: Beobachtungsmodus - nichts schreiben
        return True
    if MODBUS_LOG_TXRX:
        log(f"{prefix}[TX WRITE] addr={addr} val={v} device_id={unit}")
    try:
        wr = client.write_register(addr, v, device_id=unit)
        if wr is None:
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] None response")
            return False
        if hasattr(wr, "isError") and wr.isError():
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] {wr}")
            return False
        if MODBUS_LOG_OK:
            log(f"{prefix}[RX OK]")
        time.sleep(0.03)
        return True
    except Exception as e:
        if MODBUS_LOG_ERROR:
            log(f"{prefix}[RX FEHLER] Exception: {e}")
        return False


# =============================================================================
# LOG-TEXT MAPPING (wird nach dem Config-Import aufgebaut)
# =============================================================================

LOGTXT: dict[str, str] = {}
LOGTXT.update({
    "pv_yes":        "PV-Ueberschuss: JA",
    "pv_no":         "PV-Ueberschuss: NEIN",
    "pv_pending":    "PV-Ueberschuss noch nicht stabil",
    "net_flow":      "Netzfluss",
    "mode_change":   "Moduswechsel",
    "mode_effective":"Wirksamer Modus",
    "bms_active":    "BMS aktiv",
    "bms_skip":      "Normale Regelung uebersprungen wegen BMS",
    "no_dist":       "Keine Leistungsverteilung moeglich",
    "delta":         "Benoetigte Leistung",
    "raw_state":     "ROH-ZUSTAND",
    "skip_discharge":"Entladen uebersprungen",
    "skip_charge":   "Laden uebersprungen",
    "no_discharge":  "kein Entladen moeglich",
    "eligible":      "teilnahmeberechtigt",
    "weights":       "Gewichte",
    "cycle_done":    "Zyklus abgeschlossen",
    "verify":        "PRUEFUNG",
    "warn":          "WARNUNG",
    "idle":          "bereit",
    "charging":      "laedt",
    "hold":          "halten",
    "safety":        "soc11",
})
for _k in list(LOGTXT.keys()):
    LOGTXT[_k] = _ascii_safe(LOGTXT[_k])


# =============================================================================
# TREIBER-ABSTRAKTION (Modbus / API)
# =============================================================================

class BatteryDriver:
    """Gemeinsame Schnittstelle fuer alle Batterie-Treiber."""

    def __init__(self, name, cfg, log_fn, error_fn):
        self.name  = name
        self.cfg   = cfg
        self.log   = log_fn
        self.error = error_fn

    def read_state(self):
        """Gibt dict zurueck: ac, bp, soc, ctrl, mode, err"""
        raise NotImplementedError

    def write_setpoints(self, mode_int, charge_w, discharge_w):
        """mode_int: 0=STOP, 1=CHARGE (charge_w W), 2=DISCHARGE (discharge_w W)"""
        raise NotImplementedError

    def init_control(self):
        """Einmaliger Aufruf beim Start. Gibt (ctrl_val, changed) zurueck."""
        return 21930, False


class ModbusDriver(BatteryDriver):
    """
    Modbus TCP-Treiber.
    Vorzeichen-Konvention: ac>0 = Entladen, ac<0 = Laden, bp>0 = Entladen, bp<0 = Laden.
    """

    def __init__(self, name, cfg, log_fn, error_fn,
                 registers, mb_timeout, mb_retries,
                 control_enabled_values, control_enable_candidates,
                 ac_sign_invert=False,
                 modbus_log_txrx=False, modbus_log_error=True):
        super().__init__(name, cfg, log_fn, error_fn)
        self.registers                 = registers
        self.mb_timeout                = mb_timeout
        self.mb_retries                = mb_retries
        self.control_enabled_values    = control_enabled_values
        self.control_enable_candidates = control_enable_candidates
        self.ac_sign_invert            = ac_sign_invert
        self.log_txrx                  = modbus_log_txrx
        self.log_error                 = modbus_log_error

    def _mk_client(self):
        # Dauerhafte Verbindung je Batterie (statt 3x pro Zyklus neu verbinden)
        c = getattr(self, "_client", None)
        if c is None:
            c = self._mk_client_new()
            self._client = c
            self._bad = False
        return c
    def _drop_client(self):
        c = getattr(self, "_client", None)
        self._client = None
        self._bad = False
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
    def _done(self, client):
        # Verbindung offen lassen - nur nach einem Fehler schliessen
        if getattr(self, "_bad", False) or client is not getattr(self, "_client", None):
            self._drop_client()
    def _read(self, client, addr, count):
        _t0 = time.time()
        r = self._read_raw(client, addr, count)
        if time.time() - _t0 >= 0.8 * float(self.mb_timeout):
            self._bad = True  # FIX 28: spaete Antwort -> Verbindung nach dem Vorgang neu
        if r is None:
            self._bad = True
        return r
    def _write(self, client, addr, value):
        _t0 = time.time()
        r = self._write_raw(client, addr, value)
        if time.time() - _t0 >= 0.8 * float(self.mb_timeout):
            self._bad = True  # FIX 28: spaete Antwort -> Verbindung nach dem Vorgang neu
        if not r:
            self._bad = True
        return r

    def _mk_client_new(self):
        try:
            return ModbusTcpClient(
                self.cfg.get("modbus_host", self.cfg.get("host", "")), port=self.cfg["port"],
                timeout=self.mb_timeout, retries=self.mb_retries
            )
        except TypeError:
            return ModbusTcpClient(
                self.cfg.get("modbus_host", self.cfg.get("host", "")), port=self.cfg["port"],
                timeout=self.mb_timeout
            )

    def _read_raw(self, client, addr, count):
        prefix = f"[{self.name}] "
        if self.log_txrx:
            self.log(f"{prefix}[TX READ] addr={addr} cnt={count}")
        try:
            rr = client.read_holding_registers(addr, count=count, device_id=self.cfg["unit"])
            if rr is None:
                if self.log_error:
                    self.log(f"{prefix}[RX FEHLER] None")
                return None
            if hasattr(rr, "isError") and rr.isError():
                if self.log_error:
                    self.log(f"{prefix}[RX FEHLER] {rr}")
                return None
            regs = getattr(rr, "registers", None)
            time.sleep(0.03)
            return regs
        except Exception as e:
            if self.log_error:
                self.log(f"{prefix}[RX EXC] {e}")
            return None

    def _write_raw(self, client, addr, value):
        prefix = f"[{self.name}] "
        v = int(value)
        if globals().get("READ_ONLY", False):  # FIX 40: Beobachtungsmodus - nichts schreiben
            return True
        if self.log_txrx:
            self.log(f"{prefix}[TX WRITE] addr={addr} val={v}")
        try:
            wr = client.write_register(addr, v, device_id=self.cfg["unit"])
            if wr is None:
                if self.log_error:
                    self.log(f"{prefix}[RX FEHLER] None")
                return False
            if hasattr(wr, "isError") and wr.isError():
                if self.log_error:
                    self.log(f"{prefix}[RX FEHLER] {wr}")
                return False
            time.sleep(0.03)
            return True
        except Exception as e:
            if self.log_error:
                self.log(f"{prefix}[RX EXC] {e}")
            return False

    def _ensure_ctrl(self, client, current_ctrl):
        if current_ctrl in self.control_enabled_values:
            return current_ctrl, False
        for code in self.control_enable_candidates:
            ok = self._write(client, self.registers["control"], code)
            if not ok:
                continue
            time.sleep(0.05)
            regs = self._read(client, self.registers["control"], 1)
            if not regs:
                continue
            val = regs[0]
            if val in self.control_enabled_values:
                return val, True
            current_ctrl = val
        return current_ctrl, False

    def read_state(self):
        result = {"ac": 0, "bp": 0, "soc": 0, "ctrl": 0, "mode": 0, "err": 0}
        client = self._mk_client()
        if not client.connect():
            self._drop_client()
            result["err"] = 1
            return result
        try:
            regs = self._read(client, self.registers["ac_power"][0],
                              self.registers["ac_power"][1])
            if regs is None:
                result["err"] = 1
                return result
            ac_raw = _i32_from_u16_be(regs)
            result["ac"] = (-ac_raw) if self.ac_sign_invert else ac_raw

            regs = self._read(client, self.registers["battery_power"][0],
                              self.registers["battery_power"][1])
            if regs is None:
                result["err"] = 1
                return result
            result["bp"] = _i32_from_u16_be(regs)

            regs = self._read(client, self.registers["soc"], 1)
            if regs:
                result["soc"] = int(regs[0])
            else:
                result["err"] = 1

            regs_ctrl = self._read(client, self.registers["control"], 1)
            if regs_ctrl:
                ctrl, changed = self._ensure_ctrl(client, int(regs_ctrl[0]))
                if changed:
                    self.log(f"[{self.name}] Modbus: Control freigeschaltet ({ctrl})")
                    time.sleep(0.05)
                result["ctrl"] = ctrl
            else:
                result["err"] = 1
                result["ctrl"] = 0

            regs_mode = self._read(client, self.registers["mode"], 1)
            if regs_mode:
                result["mode"] = int(regs_mode[0])
            else:
                result["err"] = 1

            if result["ctrl"] not in self.control_enabled_values:
                result["err"] = 1

        except Exception as e:
            self.error(f"[{self.name}] ModbusDriver.read_state: {e}")
            result["err"] = 1
        finally:
            try:
                self._done(client)
            except Exception:
                pass
        return result

    def write_setpoints(self, mode_int, charge_w, discharge_w):
        client = self._mk_client()
        if not client.connect():
            self._drop_client()
            return False
        try:
            regs_ctrl = self._read(client, self.registers["control"], 1)
            ctrl_val = regs_ctrl[0] if regs_ctrl else 0
            ctrl_val, changed = self._ensure_ctrl(client, ctrl_val)
            if changed:
                time.sleep(0.05)

            if mode_int == 0:
                self._write(client, self.registers["charge_set"], 0)
                time.sleep(0.03)
                self._write(client, self.registers["discharge_set"], 0)
                time.sleep(0.03)
                self._write(client, self.registers["mode"], 0)
            elif mode_int == 1:
                self._write(client, self.registers["mode"], 1)
                time.sleep(0.03)
                self._write(client, self.registers["charge_set"], int(charge_w))
            elif mode_int == 2:
                self._write(client, self.registers["mode"], 2)
                time.sleep(0.03)
                self._write(client, self.registers["discharge_set"], int(discharge_w))
            return True
        except Exception as e:
            self.error(f"[{self.name}] ModbusDriver.write_setpoints: {e}")
            return False
        finally:
            try:
                self._done(client)
            except Exception:
                pass

    def init_control(self):
        client = self._mk_client()
        if not client.connect():
            self._drop_client()
            return 0, False
        try:
            regs = self._read(client, self.registers["control"], 1)
            ctrl_val = regs[0] if regs else 0
            return self._ensure_ctrl(client, ctrl_val)
        except Exception as e:
            self.error(f"[{self.name}] ModbusDriver.init_control: {e}")
            return 0, False
        finally:
            try:
                self._done(client)
            except Exception:
                pass


class _ApiHub:
    """
    FIX 34: Ein UDP-Socket fuer alle API-Batterien.
    Neuere Marstek-Firmware (z.B. Venus E 1573) antwortet nur an Quell-Port 30000; mehrere
    Sockets auf demselben Port (SO_REUSEPORT) verteilen die Antworten zufaellig. Deshalb
    haelt dieser Hub genau einen Socket, ein Lese-Thread verteilt eingehende Pakete nach
    Absender-IP an die Warteschlange der jeweiligen Batterie.
    """
    _inst = None
    _lock = threading.Lock()

    @classmethod
    def get(cls, port, log_fn):
        with cls._lock:
            if cls._inst is None or not cls._inst.alive:
                cls._inst = _ApiHub(int(port), log_fn)
            return cls._inst

    @classmethod
    def close_all(cls):
        with cls._lock:
            if cls._inst is not None:
                cls._inst.close()
                cls._inst = None

    def __init__(self, port, log_fn):
        import queue as _queue
        self._queue_mod = _queue
        self.log = log_fn
        self.port = int(port)
        self.queues = {}
        self.alive = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", self.port))
        except OSError as e:
            log_fn(f"API-HUB: bind(:{self.port}) fehlgeschlagen ({e}) - nutze freien Port; "
                   f"Batterien mit neuerer Firmware antworten dann evtl. nicht", level="WARNING")
            sock.bind(("0.0.0.0", 0))
        sock.settimeout(1.0)
        self.sock = sock
        self.opened_ts = time.time()
        self.thread = threading.Thread(target=self._reader, name="api_hub_rx", daemon=True)
        self.thread.start()
        log_fn(f"API-HUB: UDP-Socket {sock.getsockname()} geoeffnet (gemeinsam fuer alle API-Batterien)")

    def register(self, host):
        q = self.queues.get(host)
        if q is None:
            q = self._queue_mod.Queue()
            self.queues[host] = q
        return q

    def unregister(self, host):
        self.queues.pop(host, None)

    def send(self, raw, host, port):
        self.sock.sendto(raw, (host, int(port)))

    def _reader(self):
        while self.alive:
            try:
                pkt, src = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                if self.alive:
                    time.sleep(0.5)
                continue
            q = self.queues.get(src[0])
            if q is not None:
                q.put((pkt, time.time()))

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass
        self.log("API-HUB: UDP-Socket geschlossen")
class ApiDriver(BatteryDriver):
    """
    Marstek Open API Treiber (Rev 1.0, UDP/JSON-RPC).

    Vorzeichen-Konvention (verifiziert gegen API-Spec):
      passive_cfg power > 0 = Entladen -> ongrid_power > 0 -> ac > 0
      passive_cfg power < 0 = Laden    -> ongrid_power < 0 -> ac < 0
      bat_power   > 0 = Laden    -> bp > 0   (INVERS zu Modbus und passive_cfg!)
      bat_power   < 0 = Entladen -> bp < 0

    Caching:
      _call_cached() cacht API-Antworten fuer API_CACHE_TTL Sekunden.
      Damit wird ES.GetStatus innerhalb eines Zyklus (HA-Export + read_state)
      nur einmal physisch abgefragt.
    """

    # Kurze Cache-Lebensdauer: frisch genug fuer einen Zyklus, laeuft sicher
    # vor dem naechsten Zyklus (INTERVAL=20s) ab.
    API_CACHE_TTL = 15.0

    def __init__(self, name, cfg, log_fn, error_fn,
                 udp_port=API_UDP_PORT,
                 timeout_s=API_TIMEOUT_S,
                 cd_time_s=API_CD_TIME_S,
                 local_port=None):
        super().__init__(name, cfg, log_fn, error_fn)
        self.host        = cfg.get("api_host", cfg.get("host", ""))
        self.port        = int(udp_port)
        self.timeout     = float(timeout_s)
        self.cd_time     = int(cd_time_s)
        self._req_id     = 0
        self._last_power = 0
        self._cache: dict = {}   # FIX 11: {method: (timestamp, result)}
        self._last_call_ts: float = 0.0  # FIX H: Zeitstempel des letzten UDP-Calls
        self._device_src: str = ""       # Geraetename aus "src"-Feld der API-Antwort
        self._unsupported: dict = {}  # FIX 32: {method: epoch bis zum naechsten Versuch}
        self._device_model: str = ""   # FIX 37: "device" aus Marstek.GetDevice (z.B. "VenusE", "VenusE 3.0")
        self._device_ver = None        # FIX 37: "ver" aus Marstek.GetDevice (z.B. 1573, 148)
        self._devinfo_ts: float = 0.0  # FIX 37: letzter Versuch, Geraeteinfo zu holen
        self._fw_rules = None          # FIX 37: {(Methode, Schluessel): Faktor}
        self._pending_power = None     # FIX 38: Aenderung unter der Sende-Schwelle, wird nachgeholt
        # FIX G: Persistenter Socket pro ApiDriver-Instanz.
        # Ein Socket wird einmal geoeffnet und fuer alle Aufrufe wiederverwendet.
        # Kein Rebind = kein Konflikt zwischen Batterien.
        # Marstek antwortet nur auf Pakete von Port 30000 (Source-Port-Pflicht).
        # Da jede Batterie einen eigenen persistenten Socket hat, kommen Antworten
        # immer auf "dem richtigen" Socket an – kein SO_REUSEPORT-Round-Robin.
        self.local_port  = int(local_port if local_port is not None else API_LOCAL_PORT)
        self._sock: socket.socket | None = None

    def _open_socket(self):
        """FIX 34: am gemeinsamen Hub-Socket anmelden (ein Socket fuer alle API-Batterien)."""
        if self._sock is not None:
            return
        hub = _ApiHub.get(globals().get("API_LOCAL_PORT", 30000), self.log)
        self._hub = hub
        self._dead = False
        self._q = hub.register(self.host)
        self._sock = hub.sock
        self._sock_opened_ts = hub.opened_ts
        if API_LOG_DIAG:
            self.log(f"[{self.name}] API SOCKET: nutze Hub {hub.sock.getsockname()} fuer {self.host}:{self.port}")

    def _close_socket(self):
        """Von der Hub-Verteilung abmelden (der Hub-Socket bleibt fuer die anderen Batterien offen)."""
        hub = getattr(self, "_hub", None)
        if hub is not None:
            hub.unregister(self.host)
        self._hub = None
        self._q = None
        self._sock = None
        self._dead = True  # FIX 34: abgemeldeter Treiber darf nicht mehr senden
    def _next_id(self):
        self._req_id = (self._req_id % 9000) + 1
        return self._req_id

    # FIX 33: robuste UDP-Kommunikation
    # - Socket bleibt dauerhaft offen (die Batterie ignoriert frisch geoeffnete Sockets kurzzeitig)
    # - kurzes Timeout (API_TIMEOUT_S) und schnelle Wiederholung (API_RETRY_DELAY_S, API_MAX_ATTEMPTS)
    # - verspaetete oder fremde Antworten werden uebersprungen statt als Fehler gewertet
    # - Warnungen gedrosselt (max. eine pro Minute je Batterie und Thema)
    def _drain(self):
        """Alte, verspaetete Pakete verwerfen, damit sie die naechste Antwort nicht verdecken."""
        q = getattr(self, "_q", None)
        if q is None:
            return
        try:
            while True:
                q.get_nowait()
        except Exception:
            pass
    def _warn_throttled(self, key, msg, every_s=60.0):
        d = getattr(self, "_warn_ts", None)
        if d is None:
            d = self._warn_ts = {}
        now = time.time()
        if now - d.get(key, 0) >= every_s:
            d[key] = now
            self.log(msg, level="WARNING")
        elif API_LOG_DIAG:
            self.log(msg)

    def _call(self, method, params=None):
        if not hasattr(self, "_sock_lock"):
            self._sock_lock = threading.RLock()
        with self._sock_lock:
            return self._call_locked(method, params)

    def _call_locked(self, method, params=None):
        if getattr(self, "_dead", False):
            return None  # FIX 34: alter Treiber nach Treiberwechsel/Neustart
        if self._sock is None:
            self._open_socket()
        if self._sock is None:
            return None
        warm = float(globals().get("API_WARMUP_S", 1.5)) - (time.time() - getattr(self, "_sock_opened_ts", 0))
        if warm > 0:
            time.sleep(warm)
        _inter_delay = float(globals().get("API_INTER_CALL_DELAY_S", 0.3))
        if _inter_delay > 0 and self._last_call_ts > 0:
            elapsed = time.time() - self._last_call_ts
            if elapsed < _inter_delay:
                time.sleep(_inter_delay - elapsed)
        req_id = self._next_id()
        payload = {"id": req_id, "method": method, "params": params or {"id": 0}}
        raw = json.dumps(payload).encode("utf-8")
        attempts = max(1, int(globals().get("API_MAX_ATTEMPTS", 3)))
        retry_delay = float(globals().get("API_RETRY_DELAY_S", 0.3))
        local_addr_used = str(self._hub.sock.getsockname())
        if API_LOG_TXRX:
            self.log(f"[{self.name}] API TX {method}: {raw.decode('utf-8', errors='replace')}")
        t_first = time.time()
        for attempt in range(1, attempts + 1):
            t_send = time.time()
            try:
                self._drain()
                deadline = t_send + self.timeout
                self._hub.send(raw, self.host, self.port)
                resp, data = None, None
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise socket.timeout()
                    try:
                        pkt, _ts = self._q.get(timeout=remaining)
                    except Exception:
                        raise socket.timeout()
                    try:
                        cand = json.loads(pkt.decode("utf-8", errors="replace"))
                    except json.JSONDecodeError:
                        continue
                    if cand.get("id") not in (req_id, 0, None):
                        continue  # verspaetete Antwort auf eine fruehere Anfrage
                    resp, data = cand, pkt
                    break
                t_recv = time.time()
                self._last_call_ts = t_recv
                self._last_ok_ts = t_recv
                _src = resp.get("src")
                if _src:
                    self._device_src = _src
                if API_LOG_DIAG:
                    self.log(f"[{self.name}] API RX ({method}) Versuch {attempt} rtt={t_recv - t_send:.3f}s")
                if API_LOG_TXRX:
                    self.log(f"[{self.name}] API RX {method}: {data.decode('utf-8', errors='replace')}")
                if "error" in resp:
                    err = resp["error"]
                    err_code = err.get("code", "?") if isinstance(err, dict) else err
                    err_msg = err.get("message", "?") if isinstance(err, dict) else "?"
                    err_data = err.get("data", "?") if isinstance(err, dict) else "?"
                    if err_code == -32700 and err_data == 403:
                        ursache = ("data=403: Methode wird von dieser Firmware nicht unterstuetzt (z.B. ES.GetStatus "
                                   "ab Firmware 1573) oder Open API in der Marstek-App nicht eingeschaltet")
                    elif err_code == -32700 and err_data == 402:
                        ursache = "data=402: Geraet hat die Anfrage als Broadcast abgelehnt"
                    elif err_code == -32601:
                        ursache = "Methode unbekannt"
                    elif err_code == -32602:
                        ursache = "ungueltige Parameter"
                    else:
                        ursache = f"Antwort: {data.decode('utf-8', errors='replace')[:200]}"
                    self._last_err_code, self._last_err_method, self._last_err_data = err_code, method, err_data
                    self._warn_throttled("err:" + method,
                                         f"[{self.name}] API FEHLER bei {method}: code={err_code} message='{err_msg}' "
                                         f"data={err_data} | {ursache}")
                    return None
                _MISSING = object()
                result = resp.get("result", _MISSING)
                if result is _MISSING:
                    self.error(f"[{self.name}] API: kein 'result'-Feld in Antwort ({method}): {resp}")
                    return None
                return result if result is not None else {}
            except socket.timeout:
                if attempt < attempts:
                    if API_LOG_DIAG:
                        self.log(f"[{self.name}] API Timeout ({method}, Versuch {attempt}/{attempts}, "
                                 f"{self.timeout:.1f}s) - wiederhole in {retry_delay:.1f}s")
                    time.sleep(retry_delay)
                    continue
                self._last_call_ts = time.time()
                self._warn_throttled("timeout",
                                     f"[{self.name}] API keine Antwort ({method}) nach {attempts} Versuchen "
                                     f"({time.time() - t_first:.1f}s) - remote={self.host}:{self.port} local={local_addr_used}")
                return None
            except OSError as e:
                self.error(f"[{self.name}] API Socket-Fehler ({method}): {e} - Socket wird neu geoeffnet")
                self._close_socket()
                self._open_socket()
                return None
        return None

    def _call_cached(self, method, params=None):
        """
        FIX 11: Gecachter API-Aufruf.
        Wenn das Ergebnis juenger als API_CACHE_TTL Sekunden ist,
        wird kein neuer UDP-Aufruf gemacht.
        SetMode-Aufrufe werden NIEMALS gecacht.
        """
        if "Set" in method:
            return self._call(method, params)

        now    = time.time()
        cached = self._cache.get(method)
        if cached and (now - cached[0]) < float(globals().get("API_READ_S", self.API_CACHE_TTL)):  # FIX 33
            if API_LOG_TXRX:
                self.log(f"[{self.name}] API CACHE-HIT {method}")
            return cached[1]

        # FIX 32: Methoden, die dieses Geraet ablehnt (z.B. ES.GetStatus -> Parse error 403 bei
        # Firmware 1573), erst nach 10 min erneut probieren - kein Fehler-Spam, weniger Funkverkehr.
        if now < float(self._unsupported.get(method, 0) or 0):
            return None
        self._last_err_code = None
        result = self._call(method, params)
        if result is not None:
            self._cache[method] = (now, result)
            self._unsupported.pop(method, None)
            if getattr(self, "_unsupported_cnt", None):
                self._unsupported_cnt.pop(method, None)
        elif getattr(self, "_last_err_code", None) == -32700 and getattr(self, "_last_err_method", "") == method:
            _cnt = getattr(self, "_unsupported_cnt", None)
            if _cnt is None:
                _cnt = self._unsupported_cnt = {}
            _cnt[method] = _cnt.get(method, 0) + 1
            _pause = 600 if _cnt[method] >= 3 else 60  # FIX 33: einzelne 403 kommen auch bei gueltigen Methoden vor
            self._unsupported[method] = now + _pause
            self.log(f"[{self.name}] API: {method} wird von diesem Geraet abgelehnt "
                     f"(data={getattr(self, '_last_err_data', '?')}) - {_pause} s Pause fuer diese Methode ({_cnt[method]}. Mal in Folge)")
        return result

    def invalidate_cache(self):
        """Cache leeren – z.B. nach write_setpoints."""
        self._cache.clear()

    # Pause zwischen aufeinanderfolgenden API-Aufrufen.
    # Referenz (jaapp/ha-marstek-local-api) verwendet 1.0s - Geraet wird
    # laut README instabil bei zu schnellen aufeinanderfolgenden Abfragen.
    API_INTER_CALL_DELAY = 1.0

    def read_state(self):
        """
        FIX 33: Zustand lesen.
        - aeltere Firmware: ES.GetStatus (bat_power, ongrid_power, bat_soc)
        - neuere Firmware (z.B. 1573, ES.GetStatus -> 403): ES.GetMode (ongrid_power, bat_soc)
        - Notnagel: Bat.GetStatus (nur SoC)
        Ohne Antwort werden die letzten guten Werte bis API_STALE_MAX_S weiterverwendet (kein err),
        danach err=1 -> Batterie bekommt keinen Anteil (FIX 30).
        Ein laufender Passive-Sollwert wird vor Ablauf von cd_time aufgefrischt.
        """
        result = {"ac": 0, "bp": 0, "soc": 0, "ctrl": 21930, "mode": 0, "err": 0}
        got = False

        def _num(v):
            try:
                return int(float(str(v)))
            except (ValueError, TypeError):
                return 0

        es = self._call_cached("ES.GetStatus")
        if es is not None:
            result["bp"] = _num(es.get("bat_power"))
            result["ac"] = _num(es.get("ongrid_power"))
            if es.get("bat_soc") is not None:
                result["soc"] = _num(es.get("bat_soc"))
                got = True
            else:
                bat = self._call_cached("Bat.GetStatus")
                if bat is not None:
                    result["soc"] = _num(bat.get("soc"))
                    got = True
        else:
            md = self._call_cached("ES.GetMode")
            if md is not None and md.get("bat_soc") is not None:
                result["soc"] = _num(md.get("bat_soc"))
                result["ac"] = _num(md.get("ongrid_power"))
                result["bp"] = -result["ac"]  # Naeherung: bat_power fehlt in ES.GetMode
                self._mode_txt = str(md.get("mode") or "")
                got = True
            else:
                bat = self._call_cached("Bat.GetStatus")
                if bat is not None:
                    result["soc"] = _num(bat.get("soc"))
                    got = True
                    if API_LOG_DIAG:
                        self.log(f"[{self.name}] read_state: nur Bat.GetStatus -> SoC={result['soc']}% (AC/BP=0)")

        if got:
            if abs(self._last_power) <= 10:
                result["mode"] = 0
            elif self._last_power > 0:
                result["mode"] = 2
            else:
                result["mode"] = 1
            self._last_good = (time.time(), dict(result))
            if getattr(self, "_stale_warned", False):
                self.log(f"[{self.name}] API antwortet wieder")
                self._stale_warned = False
            self._refresh_setpoint()
            return result

        stale_max = float(globals().get("API_STALE_MAX_S", 120))
        lg = getattr(self, "_last_good", None)
        if lg and (time.time() - lg[0]) < stale_max:
            r = dict(lg[1])
            r["stale"] = True
            if not getattr(self, "_stale_warned", False):
                self.log(f"[{self.name}] API: keine Antwort - letzte Werte werden max. {int(stale_max)} s weiterverwendet",
                         level="WARNING")
                self._stale_warned = True
            return r
        result["err"] = 1
        return result

    def _hw_gen(self):
        """FIX 37: Hardware-Generation aus dem Modellnamen ("VenusE 3.0" -> 3.0, "VenusE" -> 2.0)."""
        m = re.search(r"(\d+\.\d+)", self._device_model or "")
        try:
            return float(m.group(1)) if m else 2.0
        except (ValueError, TypeError):
            return 2.0

    def _fw_rules_for(self):
        """FIX 37: Umrechnungsfaktoren fuer diese Firmware (leer = keine Korrektur)."""
        if self._fw_rules is None:
            rules = {}
            tab = globals().get("API_FW_SCALE", {}) or {}
            try:
                ver = int(self._device_ver) if self._device_ver is not None else None
            except (ValueError, TypeError):
                ver = None
            if ver is not None and self._hw_gen() < 3.0 and ver >= 154:
                rules = dict(tab.get("hw2_fw154", {}))
            self._fw_rules = rules
        return self._fw_rules

    def fw_scale(self, method, key):
        """FIX 37: Faktor fuer einen API-Wert (1.0 = unveraendert)."""
        try:
            return float(self._fw_rules_for().get((method, key), 1.0))
        except (ValueError, TypeError):
            return 1.0

    def device_txt(self):
        """FIX 37: Kurztext Modell/Firmware fuer den Status-Sensor."""
        if not self._device_model and self._device_ver is None:
            return ""
        ver = self._device_ver if self._device_ver is not None else "?"
        return f"{self._device_model or '?'} FW {ver} (HW {self._hw_gen():.1f})"

    def _ensure_device_info(self):
        """FIX 37: Modell und Firmware einmalig per Marstek.GetDevice holen; bei Misserfolg alle 10 min erneut."""
        if self._device_ver is not None or (time.time() - self._devinfo_ts) < 600:
            return
        self._devinfo_ts = time.time()
        r = self._call_cached("Marstek.GetDevice", {"ble_mac": "0"})
        if not isinstance(r, dict):
            return
        self._device_model = str(r.get("device") or r.get("model") or "")
        try:
            self._device_ver = int(float(r.get("ver") or 0))
        except (ValueError, TypeError):
            self._device_ver = 0
        self._fw_rules = None
        rules = self._fw_rules_for()
        txt = ", ".join(f"{k[1]} x{v:g}" for k, v in rules.items()) if rules else "keine noetig"
        self.log(f"[{self.name}] API GERAET: {self.device_txt()} -> Einheiten-Korrektur: {txt}")
    def _send_power(self, power, why=""):
        """ES.SetMode Passive: negativ = Laden, positiv = Entladen, 0 = STOP."""
        if globals().get("READ_ONLY", False):  # FIX 40: Beobachtungsmodus - nichts senden
            return True
        res = self._call("ES.SetMode", {
            "id": 0,
            "config": {"mode": "Passive", "passive_cfg": {"power": int(power), "cd_time": self.cd_time}},
        })
        ok = isinstance(res, dict)
        if ok:
            self._last_power = int(power)
            self._last_send_ts = time.time()
            self._pending_power = None  # FIX 38
            self.invalidate_cache()
            if API_LOG_DIAG:
                self.log(f"[{self.name}] API: SetMode OK power={int(power)}W cd_time={self.cd_time}s {why}".rstrip())
        else:
            self._warn_throttled("setmode", f"[{self.name}] API: SetMode fehlgeschlagen bei power={int(power)}W {why}".rstrip())
        return ok

    def _refresh_setpoint(self):
        """FIX 33: Passive-Sollwert laeuft nach cd_time ab -> rechtzeitig erneut senden.
        FIX 38: eine unter der Sende-Schwelle zurueckgehaltene Aenderung wird dabei nachgeholt,
        spaetestens 30 s nach dem letzten Senden (Geraet nicht mit Anfragen fluten, aber keine
        dauerhafte Netz-Abweichung von 4 x 49 W stehen lassen)."""
        pending = getattr(self, "_pending_power", None)
        if pending is None and self._last_power == 0:
            return
        ts = getattr(self, "_last_send_ts", 0)
        wait_s = 30.0 if pending is not None else max(30.0, self.cd_time * 0.5)
        if ts and (time.time() - ts) >= wait_s:
            if pending is not None:
                self._send_power(pending, "(Auffrischung, nachgeholte Aenderung)")
            else:
                self._send_power(self._last_power, "(Auffrischung vor Ablauf von cd_time)")
    def write_setpoints(self, mode_int, charge_w, discharge_w):
        if mode_int == 1:
            power = -int(charge_w)   # negativ = Laden
        elif mode_int == 2:
            power = int(discharge_w)  # positiv = Entladen
        else:
            power = 0
        # FIX 33: nur bei Aenderung senden (Schwelle API_SEND_DELTA_W); Auffrischung macht _refresh_setpoint
        delta = int(globals().get("API_SEND_DELTA_W", 50))
        ts = getattr(self, "_last_send_ts", 0)
        if ts and power == self._last_power and (time.time() - ts) < max(30.0, self.cd_time * 0.5):
            self._pending_power = None  # FIX 38: Wunsch = zuletzt gesendet
            return True
        if (ts and power != 0 and self._last_power != 0 and (power > 0) == (self._last_power > 0)
                and abs(power - self._last_power) < delta):
            self._pending_power = power  # FIX 38
            if API_LOG_DIAG:
                self.log(f"[{self.name}] API: Aenderung {self._last_power}->{power} W unter Schwelle {delta} W - wird in <= 30 s nachgeholt")
            return True
        return self._send_power(power)

    def init_control(self):
        """Beim Start / Treiberwechsel: Zustand lesen und STOP senden (definierter Zustand)."""
        st = self.read_state()
        if st.get("err") or st.get("stale"):
            self.error(f"[{self.name}] API: init_control - Geraet antwortet nicht")
            return 0, False
        self._last_power = 0
        self._last_send_ts = 0
        ok = self._send_power(0, "(STOP beim Start)")
        self.log(f"[{self.name}] API: init_control {'OK' if ok else 'STOP fehlgeschlagen, fortfahren'} (soc={st.get('soc')}%)")
        self.invalidate_cache()
        return 21930, False


def _get_driver(name, cfg, log_fn, error_fn,
                registers, mb_timeout, mb_retries,
                control_enabled_values, control_enable_candidates,
                ac_sign_invert, modbus_log_txrx, modbus_log_error):
    """Factory: gibt ModbusDriver oder ApiDriver zurueck."""
    mode = str(cfg.get("comm_mode", COMM_MODE_DEFAULT)).lower().strip()

    log_fn(f"[{name}] DRIVER-SELECT: comm_mode={mode!r} | "
           f"modbus={cfg.get('modbus_host','?')}:{cfg.get('port',502)} | "
           f"api={cfg.get('api_host','?')}:{cfg.get('api_port', API_UDP_PORT)}")

    if mode == "api":
        api_host = cfg.get("api_host", cfg.get("host", "??"))
        log_fn(f"[{name}] DRIVER-SELECT: ERGEBNIS -> ApiDriver ({api_host}:{cfg.get('api_port', API_UDP_PORT)})")
        return ApiDriver(
            name, cfg, log_fn, error_fn,
            udp_port   = cfg.get("api_port",    API_UDP_PORT),
            timeout_s  = cfg.get("api_timeout", API_TIMEOUT_S),
            cd_time_s  = cfg.get("api_cd_time", API_CD_TIME_S),
            local_port = cfg.get("api_local_port", API_LOCAL_PORT),
        )

    modbus_host = cfg.get("modbus_host", cfg.get("host", "??"))
    if mode != "modbus":
        log_fn(f"[{name}] DRIVER-SELECT: WARNUNG comm_mode={mode!r} unbekannt -> Fallback ModbusDriver")
    log_fn(f"[{name}] DRIVER-SELECT: ERGEBNIS -> ModbusDriver ({modbus_host}:{cfg.get('port', 502)})")

    return ModbusDriver(
        name, cfg, log_fn, error_fn,
        registers                 = registers,
        mb_timeout                = mb_timeout,
        mb_retries                = mb_retries,
        control_enabled_values    = control_enabled_values,
        control_enable_candidates = control_enable_candidates,
        ac_sign_invert            = ac_sign_invert,
        modbus_log_txrx           = modbus_log_txrx,
        modbus_log_error          = modbus_log_error,
    )


# =============================================================================
# KERNKLASSE
# =============================================================================

class PVControlApp(Hass):

    # -------------------------------------------------------------------------
    # HA-Sensor Helpers
    # -------------------------------------------------------------------------

    def _ha_entity(self, battery_name, key):
        idx    = int(BATTERY_META.get(battery_name, {}).get("idx", 0) or 0)
        forced = HA_ENTITY_ID_MAP.get((idx, key))
        if forced:
            return forced
        return (f"sensor.{HA_EXPORT_PREFIX}_{idx}_{key}" if idx
                else f"sensor.{HA_EXPORT_PREFIX}_{battery_name}_{key}")

    def _cell_store(self, name, key, val):
        """FIX 50: Zellspannung merken und Unterschied auswerten (nur plausible Werte 2,0-4,5 V)."""
        try:
            v = float(val)
        except (TypeError, ValueError):
            return
        if not 2.0 <= v <= 4.5:
            return
        now = time.time()
        c = self.__dict__.setdefault('_cell_v', {}).setdefault(name, {})
        c['max' if key.endswith('_max') else 'min'] = (v, now)
        if 'max' not in c or 'min' not in c or abs(c['max'][1] - c['min'][1]) > 600:
            return
        d = int(round((c['max'][0] - c['min'][0]) * 1000))
        if d < 0:
            return
        c['delta'] = (d, now)
        lim = int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0)
        if lim > 0 and d >= lim:
            c.setdefault('high_since', now)
        elif lim <= 0 or d < lim * 0.8:
            c.pop('high_since', None)

    def _cell_early(self, name):
        """FIX 50: True, wenn der Zell-Unterschied lange genug ueber der Grenze liegt (frische Modbus-Werte)."""
        c = (getattr(self, '_cell_v', {}) or {}).get(name) or {}
        hs, dl = c.get('high_since'), c.get('delta')
        if not hs or not dl or time.time() - dl[1] > 900 or int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0) <= 0:
            return False
        return time.time() - hs >= float(globals().get('BMS_CARE_CELL_DELTA_MIN_S', 1800))

    def _cell_publish(self, name):
        """FIX 50: sensor.<prefix>_N_cell_voltage_delta fuers Dashboard (nur Anzeige)."""
        try:
            c = (getattr(self, '_cell_v', {}) or {}).get(name) or {}
            dl = c.get('delta')
            lim = int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0)
            fresh = bool(dl) and time.time() - dl[1] <= 900
            if fresh:
                state = dl[0]
                if self._cell_early(name):
                    status = 'hoch - BMS-Pflege wird vorgezogen (sobald PV-Ueberschuss da ist)'
                elif c.get('high_since'):
                    status = f"hoch seit {int((time.time() - c['high_since']) // 60)} min"
                else:
                    status = 'OK' if lim > 0 else 'OK (Vorziehen aus)'
            else:
                state = 'unknown'
                status = ('nur mit Modbus-Anbindung verfuegbar' if isinstance(self._drivers.get(name), ApiDriver)
                          else 'noch keine Werte')
            idx = BATTERY_META.get(name, {}).get('idx', name)
            self.set_state(self._ha_entity(name, 'cell_voltage_delta'), state=state, attributes={
                'friendly_name': f'MarstekVenus {idx} Cell Voltage Delta', 'unit_of_measurement': 'mV',
                'icon': 'mdi:battery-heart-variant', 'status': status, 'limit_mv': lim,
                'max_v': (c.get('max') or (None,))[0] if fresh else None,
                'min_v': (c.get('min') or (None,))[0] if fresh else None})
        except Exception as e:
            self.log(f'ZELLSPANNUNG {name}: {e}')

    def _kwh_guard(self, eid, v):
        """FIX 49: Rueckgabe des Werts, der an HA gehen darf, oder None (verwerfen)."""
        now = time.time()
        if v < 0:
            return None
        last = self.__dict__.setdefault('_kwh_last', {})
        pend = self.__dict__.setdefault('_kwh_pend', {})
        if eid not in last:
            try:
                cur = float(self.get_state(eid))
            except Exception:
                cur = None
            if cur is None or cur <= 0 or cur != cur:
                last[eid] = (v, now)
                return v
            last[eid] = (cur, now)
        lv, lt = last[eid]
        max_up = 0.05 + max(15.0, now - lt) * 12.0 / 3600.0  # hoechstens 12 kW seit dem letzten guten Wert
        if lv - 0.05 <= v <= lv + max_up:
            pend.pop(eid, None)
            v = max(v, lv)
            last[eid] = (v, now)
            return v
        pv = pend.get(eid)
        if pv and pv[0] - 0.05 <= v <= pv[0] + 0.05 + (now - pv[1]) * 12.0 / 3600.0:
            if now - pv[1] >= 600 and pv[2] >= 4:
                self.log(f'ENERGIE: {eid} neuer Zaehlerstand {v} kWh uebernommen (vorher {lv} kWh, 10 min stabil)', level='WARNING')
                pend.pop(eid, None)
                last[eid] = (v, now)
                return v
            pend[eid] = (pv[0], pv[1], pv[2] + 1)
            return None
        self.log(f'ENERGIE: {eid} unplausibel {v} kWh (letzter guter Wert {lv} kWh) - verworfen', level='WARNING')
        pend[eid] = (v, now, 1)
        return None

    def _set_ha_sensor(self, entity_id, value, unit="", attrs=None):
        if not HA_EXPORT_ENABLE:
            return
        if value is None:
            return
        if str(unit).strip().lower() == 'kwh' and 'total' in str(entity_id):  # FIX 49 (nur Gesamtzaehler)
            try:
                value = self._kwh_guard(entity_id, float(value))
            except (TypeError, ValueError):
                return
            if value is None:
                return
        try:
            if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
                return
        except Exception:
            return
        try:
            a = dict(attrs or {})
            if unit:
                a["unit_of_measurement"] = str(unit)
            if str(unit).lower() == "kwh":
                a["device_class"] = "energy"
                a["state_class"]  = "total_increasing"
            state = str(value) if isinstance(value, (int, float)) else str(value).strip()
            self.set_state(entity_id, state=state, attributes=a)
        except Exception as e:
            self.error(f"HA-EXPORT: set_state failed for {entity_id}: {e}")

    # ---- Berechnete Sensoren (z.B. Lade-/Entladeleistung) -------------------

    @staticmethod
    def _apply_transform(value, transform):
        """Wendet eine Transformationsregel auf einen numerischen Wert an."""
        try:
            v = float(value)
        except (TypeError, ValueError):
            return None
        if transform == "pos":
            return max(0.0, v)
        if transform == "neg_abs":
            return abs(min(0.0, v))
        if transform == "abs":
            return abs(v)
        if transform == "negate":
            return -v
        return v

    def _export_computed_sensors(self, battery_name, source_key, source_val):
        """
        Prueft ob berechnete Sensoren (HA_COMPUTED_SENSORS) vom source_key
        abhaengen und exportiert diese nach HA.
        """
        try:
            computed = HA_COMPUTED_SENSORS        # noqa: F405
        except NameError:
            return
        idx = BATTERY_META.get(battery_name, {}).get("idx", battery_name)
        friendly_prefix = f"MarstekVenus {idx}"
        for ckey, cspec in computed.items():
            if not cspec.get("enabled", True):
                continue
            if cspec.get("source") != source_key:
                continue
            transformed = self._apply_transform(source_val, cspec.get("transform", ""))
            if transformed is None:
                continue
            prec = int(cspec.get("prec", 0))
            val  = round(transformed, prec)
            ent  = self._ha_entity(battery_name, ckey)
            unit = str(cspec.get("unit", ""))
            attrs = {
                "friendly_name": f"{friendly_prefix} {cspec.get('name', ckey)}",
                "source":        "appdaemon_pv_control_computed",
                "source_sensor": source_key,
            }
            # HA Energy-Dashboard: Leistungssensoren brauchen device_class + state_class
            if unit.upper() in ("W", "KW"):
                attrs["device_class"] = "power"
                attrs["state_class"]  = "measurement"
            self._set_ha_sensor(ent, val, unit=unit, attrs=attrs)

    def _read_reg_value(self, client, unit, spec, name):
        addr = int(spec["addr"])
        cnt  = int(spec.get("cnt", 1))
        typ  = str(spec.get("type", "u16"))
        regs = self._modbus_read(client, unit, addr, cnt, name)
        if regs is None:
            return None
        if typ == "u16":
            return int(regs[0])
        if typ == "s16":
            return int(_s16_from_u16(regs[0]))
        if typ == "u32":
            return int(_u32_from_u16_be(regs[:2]))
        if typ == "s32":
            return int(_i32_from_u16_be(regs[:2]))
        return None

    def _poll_and_export_ha_sensors(self, battery_name, cfg):
        """
        FIX 14/15: Dispatcht HA-Export je nach AKTIVEM Treiber (nicht Config-comm_mode).
        Bei aktivem Fallback wird der Export-Treiber automatisch mitgewechselt.
        """
        if not HA_EXPORT_ENABLE:
            return
        driver = self._drivers.get(battery_name)
        if isinstance(driver, ApiDriver):
            self._poll_and_export_api_sensors(battery_name, cfg)
        else:
            self._poll_and_export_modbus_sensors(battery_name, cfg)

    def _poll_and_export_modbus_sensors(self, battery_name, cfg):
        """HA-Export via Modbus TCP – nur Eintraege mit enabled=True aus HA_REGMAP."""
        now_ts = float(self._now_epoch or time.time())
        if now_ts < float(self._mb_backoff_until.get(battery_name, 0) or 0):
            return
        _sdrv = self._drivers.get(battery_name)
        _shared = isinstance(_sdrv, ModbusDriver)
        client = _sdrv._mk_client() if _shared else _mb_client(cfg.get("modbus_host", cfg.get("host", "")), cfg["port"])
        if not client.connect():
            if _shared:
                _sdrv._drop_client()
            self._mb_backoff_until[battery_name] = time.time() + API_ERROR_BACKOFF_S
            return
        try:
            keys = [k for k, v in HA_REGMAP.items() if v.get("enabled", True)]
            _cu = getattr(self, '_cell_unsupported', {}).get(battery_name, 0)  # FIX 50
            keys = [k for k in keys if not (k.startswith('cell_voltage_') and time.time() < _cu)]
            prio = ["battery_soc", "total_charging_energy", "total_discharging_energy"]
            keys = [k for k in prio if k in keys] + [k for k in keys if k not in prio]

            pos_key = f"_ha_export_pos_{battery_name}"
            pos     = int(getattr(self, pos_key, 0) or 0)
            chunk   = keys[pos:pos + HA_EXPORT_KEYS_PER_CYCLE]
            if not chunk:
                pos   = 0
                chunk = keys[:HA_EXPORT_KEYS_PER_CYCLE]
            setattr(self, pos_key, (pos + HA_EXPORT_KEYS_PER_CYCLE) % max(1, len(keys)))

            for key in chunk:
                spec = HA_REGMAP[key]
                _t_rd = time.time()
                raw = self._read_reg_value(client, cfg["unit"], spec, battery_name)
                if _shared and (time.time() - _t_rd) >= 0.8 * float(MB_TIMEOUT_S):
                    _sdrv._bad = True  # FIX 28
                if raw is None and key.startswith('cell_voltage_'):  # FIX 50: Register evtl. nicht vorhanden
                    self.__dict__.setdefault('_cell_unsupported', {})[battery_name] = time.time() + 6 * 3600
                    self.log(f'[{battery_name}] Zellspannung (Register {spec.get("addr")}) nicht lesbar - 6 h pausiert', level='WARNING')
                    continue
                if raw is None:
                    self._mb_backoff_until[battery_name] = time.time() + MODBUS_REG_ERROR_BACKOFF_S
                    if _shared:
                        _sdrv._bad = True
                    break
                scale    = float(spec.get("scale", 1.0))
                offset   = float(spec.get("offset", 0.0))
                prec     = int(spec.get("prec", 0))
                unit     = str(spec.get("unit", ""))
                val      = round((raw * scale) + offset, prec) if prec >= 0 else (raw * scale) + offset
                ent      = self._ha_entity(battery_name, key)
                friendly = (f"MarstekVenus {BATTERY_META.get(battery_name, {}).get('idx', battery_name)} "
                            f"{spec.get('name', key.replace('_', ' ').title())}")
                self._set_ha_sensor(ent, val, unit=unit, attrs={
                    "friendly_name": friendly,
                    "source": "appdaemon_pv_control_modbus",
                    "reg":  int(spec["addr"]),
                    "type": str(spec.get("type")),
                })
                self._export_computed_sensors(battery_name, key, val)
                if key.startswith('cell_voltage_'):  # FIX 50
                    self._cell_store(battery_name, key, val)
        finally:
            try:
                if _shared:
                    _sdrv._done(client)
                else:
                    client.close()
            except Exception:
                pass

    def _poll_and_export_api_sensors(self, battery_name, cfg):
        """
        HA-Export via Marstek Open API.
        FIX 11: Verwendet driver._call_cached() - kein doppelter UDP-Aufruf
        wenn read_state() im selben Zyklus bereits aufgerufen hat.

        Venus C/E: kein PV-Eingang -> PV.GetStatus liefert None, wird uebersprungen.
        """
        now_ts = float(self._now_epoch or time.time())
        if now_ts < float(self._mb_backoff_until.get(battery_name, 0) or 0):
            return

        driver = self._drivers.get(battery_name)
        if not isinstance(driver, ApiDriver):
            return
        if driver._device_ver is None:  # FIX 37: Modell/Firmware einmalig holen, dann Status-Sensor aktualisieren
            driver._ensure_device_info()
            if driver._device_ver is not None:
                self._export_driver_state(battery_name)

        active = {k: v for k, v in API_EXPORT_MAP.items() if v.get("enabled", False)}
        if not active:
            return

        # Alle benoetigten Methoden einmal aufrufen (gecacht wenn moeglich)
        method_cache: dict = {}
        for spec in active.values():
            method = spec["method"]
            if method not in method_cache:
                result = driver._call_cached(method)   # FIX 11: gecacht
                method_cache[method] = result or {}
                if result is None:
                    self.log(f"[{battery_name}] API-EXPORT: {method} -> keine Antwort (uebersprungen)")

        if not any(method_cache.values()):
            self._mb_backoff_until[battery_name] = time.time() + API_ERROR_BACKOFF_S
            return

        idx             = int(BATTERY_META.get(battery_name, {}).get("idx", 0) or 0)
        friendly_prefix = f"MarstekVenus {idx}"

        for key, spec in active.items():
            raw_val = method_cache.get(spec["method"], {}).get(spec["key"])
            if raw_val is None:
                continue
            try:
                scale = float(spec.get("scale", 1.0))
                prec  = int(spec.get("prec", 0))
                unit  = str(spec.get("unit", ""))
                fscale = 1.0  # FIX 37
                if isinstance(raw_val, bool):
                    val = int(raw_val)
                elif isinstance(raw_val, str):
                    val = raw_val
                else:
                    fscale = driver.fw_scale(spec["method"], spec["key"])  # FIX 37
                    val = round(float(raw_val) * scale * fscale, prec)
                # FIX 16: canonical_key stellt sicher dass API und Modbus die
                # gleiche HA-Entity-ID verwenden -> kein Wechsel in HA sichtbar.
                canonical = spec.get("canonical_key", key)
                ent = self._ha_entity(battery_name, canonical)
                attrs = {
                    "friendly_name": f"{friendly_prefix} {spec.get('name', canonical)}",
                    "source": "appdaemon_pv_control_api",
                    "api_method": spec["method"],
                    "api_key": spec["key"],
                }
                if fscale != 1.0:  # FIX 37
                    attrs["api_fw_scale"] = fscale
                    if spec["key"] == "bat_temp":
                        attrs["hinweis"] = "Diese Firmware liefert die Temperatur nur in 10-Grad-Schritten"
                self._set_ha_sensor(ent, val, unit=unit, attrs=attrs)
                self._export_computed_sensors(battery_name, canonical, val)
            except Exception as e:
                self.error(f"[{battery_name}] API-EXPORT {key}: {e}")

    # -------------------------------------------------------------------------
    # Init
    # -------------------------------------------------------------------------

    def _init_control_all_batteries(self, stop_event=None):
        """
        Initialisiert alle Batterien PARALLEL (je ein Thread pro Batterie).
        battery1-Timeout (bis 45s) blockiert damit nicht mehr battery2+3.
        """
        import concurrent.futures

        def _init_one(name):
            if stop_event is not None and stop_event.is_set():
                return name, "abgebrochen"
            driver = self._drivers.get(name)
            if driver is None:
                return name, "kein Treiber"
            try:
                ctrl_val, changed = driver.init_control()
                if ctrl_val == 0 and not changed:
                    drv_type = "API Timeout" if isinstance(driver, ApiDriver) else "Modbus Connection refused"
                    self.error(f"[{name}] INIT-CONTROL: Geraet nicht erreichbar ({drv_type})")
                    return name, "timeout"
                elif changed:
                    self.log(f"[{name}] INIT-CONTROL: freigeschaltet ({ctrl_val})")
                else:
                    self.log(f"[{name}] INIT-CONTROL: OK ({ctrl_val})")
                return name, "ok"
            except Exception as e:
                self.error(f"[{name}] INIT-CONTROL: Fehler: {e}")
                return name, f"fehler: {e}"

        names = list(self.batteries.keys())
        self.log(f"INIT-CONTROL: Starte parallel fuer {names}")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(names), thread_name_prefix="init_bat"
        ) as pool:
            futures = {pool.submit(_init_one, n): n for n in names}
            for fut in concurrent.futures.as_completed(futures):
                if stop_event is not None and stop_event.is_set():
                    self.log("INIT-CONTROL: Stop-Event – warte auf laufende Threads")
                    break
                try:
                    n, status = fut.result()
                    self.log(f"INIT-CONTROL [{n}]: {status}")
                except Exception as e:
                    self.error(f"INIT-CONTROL future error: {e}")

    # -------------------------------------------------------------------------
    # FIX 15: Treiber-Fabrik (wird von initialize und _check_comm_fallback verwendet)
    # -------------------------------------------------------------------------

    def _make_driver(self, name, cfg, mode_override=None):
        """
        Erstellt einen neuen Treiber fuer 'name' mit optionalem comm_mode-Override.
        mode_override: "api" oder "modbus" – ueberschreibt cfg["comm_mode"].
        """
        cfg_effective = dict(cfg)
        if mode_override:
            cfg_effective["comm_mode"] = mode_override
        return _get_driver(
            name, cfg_effective,
            log_fn                    = self.log,
            error_fn                  = self.error,
            registers                 = REGISTERS,
            mb_timeout                = MB_TIMEOUT_S,
            mb_retries                = MB_RETRIES,
            control_enabled_values    = CONTROL_ENABLED_VALUES,
            control_enable_candidates = CONTROL_ENABLE_CANDIDATES,
            ac_sign_invert            = AC_SIGN_INVERT,
            modbus_log_txrx           = MODBUS_LOG_TXRX,
            modbus_log_error          = MODBUS_LOG_ERROR,
        )

    # -------------------------------------------------------------------------
    # FIX 15: Fallback-Logik
    # -------------------------------------------------------------------------

    # FIX 31: Anbindung je Batterie waehlbar (Dashboard: BATTERYn_COMM)
    def _comm_choice(self, name):
        """Liefert (Auswahltext, Vorrangweg, Ausweichweg-oder-None) aus der globalen Auswahl BATTERYn_COMM."""
        key = name.upper() + "_COMM"
        cfg = self.batteries.get(name, {})
        if key not in globals():
            globals()[key] = _comm_choice_default(cfg)
        val = str(globals().get(key) or "")
        pair = COMM_CHOICES.get(val)
        if pair is None:
            dflt = _comm_choice_default(cfg)
            self.log(f"[{name}] ANBINDUNG: Auswahl {val!r} unbekannt -> {dflt!r}", level="WARNING")
            val = dflt
            globals()[key] = val
            pair = COMM_CHOICES[val]
        return val, pair[0], pair[1]

    def _close_driver(self, drv):
        for fn in ("_drop_client", "_close_socket"):
            if hasattr(drv, fn):
                try:
                    getattr(drv, fn)()
                except Exception:
                    pass

    def _replace_driver(self, name, new_drv, init=True):
        old = self._drivers.get(name)
        if old is not None and old is not new_drv:
            self._close_driver(old)
        self._drivers[name] = new_drv
        if init:
            # FIX 32: neuer Treiber im laufenden Betrieb -> wie beim Start initialisieren
            # (Modbus: RS485-Steuermodus 42000 setzen, API: STOP senden)
            try:
                ctrl_val, changed = new_drv.init_control()
                if ctrl_val == 0 and not changed:
                    self.log(f"[{name}] INIT-CONTROL nach Treiberwechsel: Geraet nicht erreichbar", level="WARNING")
                else:
                    self.log(f"[{name}] INIT-CONTROL nach Treiberwechsel: {'freigeschaltet' if changed else 'OK'} ({ctrl_val})")
            except Exception as ex:
                self.error(f"[{name}] INIT-CONTROL nach Treiberwechsel: Fehler: {ex}")
        self._export_driver_state(name)

    def _sync_limit_params(self, name, initial=False):
        """FIX 48: Grenzwerte aus dem Dashboard (BATTERYn_MAX_CHARGE_W usw.) in die Batterie-Config uebernehmen."""
        cfg = self.batteries.get(name)
        if cfg is None:
            return False
        new = {}
        for suffix, ckey, typ, lo, hi in LIMIT_PARAMS:
            gkey = name.upper() + suffix
            if gkey not in globals():
                continue
            try:
                val = typ(max(lo, min(hi, float(globals()[gkey]))))
            except (ValueError, TypeError):
                continue
            if typ is float:
                val = round(val, 2)
            if cfg.get(ckey) is None or typ(cfg.get(ckey)) != val:
                new[ckey] = val
        if not new:
            return False
        mn = new.get('min_soc', cfg.get('min_soc', 0))
        mx = new.get('max_soc', cfg.get('max_soc', 100))
        if int(mn) >= int(mx):
            if not initial and getattr(self, '_limit_warned', {}).get(name) != (mn, mx):
                self.log(f'[{name}] GRENZEN: Min-SoC {mn} % ist nicht kleiner als Max-SoC {mx} % - SoC-Werte bleiben', level='WARNING')
                self.__dict__.setdefault('_limit_warned', {})[name] = (mn, mx)
            new.pop('min_soc', None)
            new.pop('max_soc', None)
        cfg.update(new)
        if new and not initial:
            self.log(f"[{name}] GRENZEN geaendert: {', '.join(f'{k}={v}' for k, v in new.items())}")
        return bool(new)

    def _sync_conn_params(self, name, initial=False):
        """FIX 36: Verbindungsdaten aus dem Dashboard (BATTERYn_MODBUS_HOST/_PORT/_UNIT, _API_HOST/_API_PORT)
        in die Batterie-Config uebernehmen. Bei Aenderung im Betrieb wird der Treiber neu aufgebaut.
        Rueckgabe: True wenn sich etwas geaendert hat."""
        cfg = self.batteries.get(name)
        if cfg is None:
            return False
        changed = []
        for suffix, ckey, typ in CONN_PARAMS:
            gkey = name.upper() + suffix
            if gkey not in globals():
                continue
            raw = globals()[gkey]
            try:
                val = str(raw).strip() if typ is str else int(float(raw))
            except (ValueError, TypeError):
                continue
            if typ is str and not val:
                continue  # leere IP -> Config unveraendert lassen
            cur = cfg.get(ckey, cfg.get("host") if ckey == "modbus_host" else None)
            try:
                same = (str(cur).strip() == val) if typ is str else (cur is not None and int(cur) == val)
            except (ValueError, TypeError):
                same = False
            if not same:
                cfg[ckey] = val
                changed.append(f"{ckey}={val}")
        if changed and not initial:
            self.log(f"[{name}] VERBINDUNG geaendert ({', '.join(changed)}) -> Treiber neu")
            self._comm_fallback.pop(name, None)
            self._mb_backoff_until[name] = 0.0
            self._sync_comm_choice(name)
        return bool(changed)
    def _sync_comm_choice(self, name, initial=False):
        """Uebernimmt die Dashboard-Auswahl; baut den Treiber bei Aenderung neu.
        Rueckgabe: True wenn der Treiber neu erstellt wurde."""
        choice, primary, fallback = self._comm_choice(name)
        fb = self._comm_fallback.get(name)
        if fb is not None and fb.get("choice") == choice and not initial:
            return False
        self._comm_fallback[name] = {
            "choice": choice,
            "primary_mode": primary,
            "fallback_mode": fallback,
            "active": False,
            "failures": 0,
            "retry_at": 0,
        }
        if not initial:
            self.log(f"[{name}] ANBINDUNG: {choice!r} -> Treiber neu: {primary.upper()}"
                     + (f", bei Ausfall {fallback.upper()}" if fallback else ", kein Ausweichweg"))
        self._mb_backoff_until[name] = 0.0
        self._replace_driver(name, self._make_driver(name, self.batteries[name], primary), init=not initial)
        return True

    def _check_comm_fallback(self, name, read_error):
        """
        Wird nach jedem Lesevorgang aufgerufen (FIX 15 / FIX 31).
        read_error=True : Lesefehler -> Fehler zaehlen, nach COMM_MODE_FALLBACK_FAILURES auf Ausweichweg.
        read_error=False: Erfolg -> Fehler zuruecksetzen; auf dem Ausweichweg nach Wartezeit Vorrangweg testen.
        Sind beide Wege gestoert, wird der Vorrangweg sofort wieder getestet; solange bleibt die
        Batterie auf err und bekommt keinen Anteil (FIX 30).
        Rueckgabe: True wenn ein Treiberwechsel stattgefunden hat (Zyklus fuer diese Batterie ueberspringen).
        Ohne Ausweichweg (Auswahl "Modbus" oder "API") passiert hier nichts.
        """
        fb = self._comm_fallback.get(name)
        if fb is None or not fb.get("fallback_mode"):
            return False
        now = time.time()
        primary_mode = fb["primary_mode"]
        fallback_mode = fb["fallback_mode"]
        if not read_error:
            fb["failures"] = 0
            if fb["active"] and fb["retry_at"] > 0 and now >= fb["retry_at"]:
                return self._try_primary(name, fb)
            return False
        fb["failures"] += 1
        if not fb["active"]:
            if fb["failures"] >= COMM_MODE_FALLBACK_FAILURES:
                self.error(f"[{name}] FALLBACK: {fb['failures']} Fehler auf {primary_mode.upper()} "
                           f"-> Wechsel auf {fallback_mode.upper()}")
                fb["active"] = True
                fb["retry_at"] = now + (COMM_MODE_FALLBACK_RETRY_MIN * 60)
                fb["failures"] = 0
                self._mb_backoff_until[name] = 0.0
                self._replace_driver(name, self._make_driver(name, self.batteries[name], fallback_mode))
                return True
            return False
        # bereits auf dem Ausweichweg
        if fb["failures"] >= COMM_MODE_FALLBACK_FAILURES:
            self.log(f"[{name}] FALLBACK: auch {fallback_mode.upper()} antwortet nicht ({fb['failures']} Fehler) "
                     f"-> Batterie nicht erreichbar, {primary_mode.upper()} wird erneut geprueft", level="WARNING")
            fb["failures"] = 0
            return self._try_primary(name, fb)
        if fb["retry_at"] > 0 and now >= fb["retry_at"]:
            return self._try_primary(name, fb)
        return False

    def _try_primary(self, name, fb):
        """Testet den Vorrangweg einmal; bei Erfolg Rueckwechsel."""
        primary_mode = fb["primary_mode"]
        fallback_mode = fb["fallback_mode"]
        now = time.time()
        self.log(f"[{name}] FALLBACK: Pruefe Rueckwechsel auf {primary_mode.upper()} ...")
        try:
            test_drv = self._make_driver(name, self.batteries[name], primary_mode)
            # FIX 35: API erst mit einer billigen Anfrage antesten (sonst kostet der Test bis zu 15 s im Regelzyklus)
            if isinstance(test_drv, ApiDriver) and test_drv._call("Marstek.GetDevice", {"ble_mac": "0"}) is None:
                self._close_driver(test_drv)
                self.log(f"[{name}] FALLBACK: {primary_mode.upper()} antwortet nicht -> bleibe auf {fallback_mode.upper()}")
                fb["retry_at"] = now + (COMM_MODE_FALLBACK_RETRY_MIN * 60)
                return False
            test_st = test_drv.read_state()
            if not test_st.get("err"):
                self.log(f"[{name}] FALLBACK: Rueckwechsel auf {primary_mode.upper()} erfolgreich")
                fb["active"] = False
                fb["retry_at"] = 0
                fb["failures"] = 0
                self._mb_backoff_until[name] = 0.0
                self._replace_driver(name, test_drv)
                return True
            self._close_driver(test_drv)
            self.log(f"[{name}] FALLBACK: {primary_mode.upper()} noch nicht erreichbar -> bleibe auf {fallback_mode.upper()}")
        except Exception as e:
            self.log(f"[{name}] FALLBACK: Rueckwechsel-Test fehlgeschlagen: {e}")
        fb["retry_at"] = now + (COMM_MODE_FALLBACK_RETRY_MIN * 60)
        return False

    def _export_driver_state(self, name):
        """Schreibt den aktuellen Verbindungsweg als HA-Sensor sensor.pv_<name>_comm_mode."""
        try:
            driver = self._drivers.get(name)
            mode = "API" if isinstance(driver, ApiDriver) else "Modbus"
            fb = self._comm_fallback.get(name, {})
            self.set_state(
                f"sensor.pv_{name}_comm_mode",
                state = mode,
                attributes = {
                    "auswahl": str(fb.get("choice", "?")),
                    "vorrang": str(fb.get("primary_mode", "?")),
                    "ausweichweg": str(fb.get("fallback_mode") or "-"),
                    "fallback_active": bool(fb.get("active", False)),
                    "primary_mode": str(fb.get("primary_mode", "?")),
                    "failures": int(fb.get("failures", 0)),
                    "modbus": f"{self.batteries.get(name, {}).get('modbus_host', self.batteries.get(name, {}).get('host', '?'))}:{self.batteries.get(name, {}).get('port', 502)} ID {self.batteries.get(name, {}).get('unit', 1)}",
                    "api": f"{self.batteries.get(name, {}).get('api_host', '?')}:{self.batteries.get(name, {}).get('api_port', 30000)}",
                    "geraet": (getattr(self._drivers.get(name), "device_txt", None) or (lambda: ""))(),  # FIX 37
                    "retry_in_min": max(0, int((fb.get("retry_at", 0) - time.time()) / 60)),
                }
            )
        except Exception:
            pass

    def initialize(self):
        self.log(f"Marstek Venus Steuerung {PV_CONTROL_VERSION} startet")  # FIX 39
        if globals().get("READ_ONLY", False):  # FIX 40
            self.log("BEOBACHTUNGSMODUS: es werden keine Befehle an die Batterien gesendet (nur lesen). Ausschalten im Dashboard: Nur beobachten", level="WARNING")
        self.batteries = {
            name: cfg for name, cfg in BATTERY_CONFIG.items()
            if cfg.get("enabled")
        }

        # FIX 42: Sensoren geloeschter Batterien entfernen (sonst bleiben sie bis zum HA-Neustart sichtbar)
        try:
            _keep = {str(int(BATTERY_META.get(_n, {}).get('idx', 0) or 0)) for _n in self.batteries}
            _pat = re.compile(rf'^sensor\.(?:{re.escape(HA_EXPORT_PREFIX)}_(\d+)_|pv_battery(\d+)_comm_mode$|bms_battery(\d+)_phase$)')
            for _eid in list((self.get_state() or {}).keys()):
                _m = _pat.match(_eid)
                if _m and (_m.group(1) or _m.group(2) or _m.group(3)) not in _keep:
                    try:
                        self.remove_entity(_eid)
                    except Exception:
                        pass
        except Exception as _e:
            self.log(f'FIX 42: alte Sensoren nicht entfernt ({_e})', level='WARNING')

        self.last_distribution = {name: 0 for name in self.batteries}
        self.mode_state        = None
        self.mode_since        = time.monotonic() - STABLE_TIMER_S
        self._busy             = False

        self.pv_surplus        = False
        self.pv_pending        = False
        self.allow_charge      = False
        self._last_pv_surplus  = False

        self._pv_ok_since      = None
        self._grid_zero_since  = None

        self._cycle_last_epoch = 0.0
        self._cycle_next_epoch = 0.0
        self._cycle_counter    = 0
        self._now_epoch        = 0.0

        # FIX 13: BMS-Log-Throttle - nur alle 5 Minuten loggen
        self._last_bms_log_ts  = 0.0

        header = "| " + " | ".join(h.ljust(COL_WIDTH[c]) for c, h in zip(COLUMNS, HEADERS)) + " |"
        sep    = "|-" + "-|-".join("-" * COL_WIDTH[c] for c in COLUMNS) + "-|"
        self.header_line = header
        self.sep_line    = sep

        self.bms_state = {
            name: {
                "last_full_ts": 0, "phase": "idle",
                "holding_until": 0, "cooldown_until": 0,
                "min_soc_since": 0, "safety_done": False, "safety_active": False,
            } for name in self.batteries
        }
        self.bms_day           = datetime.now().date()
        self.bms_today_counter = 0
        self._bms_state_path   = BMS_STATE_FILE
        self._load_bms_state()

        self.pv_rotate = {
            "order": list(self.batteries.keys()),
            "idx":   0,
            "day":   datetime.now().strftime("%Y-%m-%d"),
        }
        self._load_pv_rotate_state()

        self._mb_backoff_until = {name: 0.0 for name in self.batteries}

        # FIX 15 / FIX 31: Anbindung je Batterie (Dashboard-Auswahl BATTERYn_COMM), Fallback-Zustand + Treiber.
        # Alte Treiber-Verbindungen vorher schliessen (RS485-LAN-Adapter erlauben meist nur einen Client).
        for _dn, _dd in list((getattr(self, "_drivers", {}) or {}).items()):
            self._close_driver(_dd)
        self._comm_fallback = {}
        self._drivers = {}
        for name in self.batteries:
            self._sync_conn_params(name, initial=True)  # FIX 36
            self._sync_limit_params(name, initial=True)  # FIX 48
            self._sync_comm_choice(name, initial=True)

        driver_info = {n: type(d).__name__ for n, d in self._drivers.items()}
        self.log(f"Starte PVControlApp, INTERVAL={INTERVAL}s, BMS_CARE={'ON' if ENABLE_BMS_CARE else 'OFF'}")
        self.log(f"Treiber: {driver_info}")
        for _n, _fb in self._comm_fallback.items():
            self.log(f"[{_n}] ANBINDUNG: {_fb['choice']!r} -> {_fb['primary_mode'].upper()}"
                     + (f", bei Ausfall {_fb['fallback_mode'].upper()} (nach {COMM_MODE_FALLBACK_FAILURES} Fehlern, "
                        f"Rueckwechsel-Test nach {COMM_MODE_FALLBACK_RETRY_MIN} min)" if _fb['fallback_mode'] else ", kein Ausweichweg"))
        self._log_bms_schedule()
        self._last_bms_log_ts = time.time()

        # Init-Kontrolle im Hintergrund-Thread ausfuehren, damit AppDaemon
        # nicht blockiert wird (Timeouts koennen bis zu 3×15s = 45s dauern).
        # Session-ID: wird bei jedem initialize() hochgezaehlt.
        # Damit koennen veraltete Zyklus-Callbacks erkennen ob sie noch
        # zur aktuellen Session gehoeren und ggf. abbrechen.
        # Session-ID hochzaehlen – veraltete Zyklen/Threads erkennen ihren Ablauf
        self._session_id = getattr(self, "_session_id", 0) + 1
        _my_session      = self._session_id

        # Stop-Event: signalisiert laufendem Init-Thread dass er abbrechen soll.
        # Wichtig wenn AppDaemon initialize() erneut aufruft waehrend der alte
        # Thread noch laeuft (z.B. battery1 hat 3x Timeout = bis zu 50s).
        _prev_stop = getattr(self, "_init_stop", None)
        if _prev_stop is not None:
            self.log("INIT: Stoppe alten Init-Thread (neuer Neustart erkannt) ...")
            _prev_stop.set()

        # FIX 33: fruehere Socket-Schliessung hier entfernt - sie traf die NEUEN Treiber;
        # alte Treiber werden jetzt in _sync_comm_choice(initial=True) geschlossen.

        self._init_stop = threading.Event()
        _stop           = self._init_stop
        self._init_done = False

        def _init_thread():
            try:
                self.log("INIT-THREAD: Start - Batterien werden im Hintergrund initialisiert")
                self._init_control_all_batteries(_stop)
                if _stop.is_set():
                    self.log("INIT-THREAD: Abgebrochen (neuerer Neustart hat uebernommen)")
                    return
                # FIX 15: HA-Sensoren fuer Kommunikationsstatus initial setzen
                for _n in self.batteries:
                    self._export_driver_state(_n)
                self.log("INIT-THREAD: Abgeschlossen - normaler Betrieb startet")
            except Exception as e:
                self.error(f"INIT-THREAD: Unerwarteter Fehler: {e}")
            finally:
                if not _stop.is_set():
                    self._init_done = True

        t = threading.Thread(target=_init_thread, name="pv_control_init", daemon=True)
        t.start()

        # --- MQTT-Parameterbruecke (Dashboard-Steuerung, siehe pv_mqtt.py) ---
        self._mqtt_bridge = None
        try:
            _b_spec = importlib.util.spec_from_file_location("pv_mqtt", os.path.join(_THIS_DIR, "pv_mqtt.py"))
            _b_mod = importlib.util.module_from_spec(_b_spec)
            _b_spec.loader.exec_module(_b_mod)
            self._mqtt_bridge = _b_mod.ParamBridge(self, globals())
            self._mqtt_bridge.start()
        except Exception as e:
            self.error(f"MQTT-BRIDGE: nicht gestartet, laufe mit pv_config-Werten: {e}")

        self.run_every(self.read_and_log, self.datetime(), INTERVAL)
        self.run_every(self._cleanup_old_logs, self.datetime(), int(LOG_CLEANUP_RUN_EVERY_H) * 3600)

    # -------------------------------------------------------------------------
    # PV Bucket / Rotation
    # -------------------------------------------------------------------------

    def _select_pv_charge_subset(self, target_w, states_no_bms):
        bats = list(states_no_bms.keys())
        if not bats:
            return []
        if not PV_BUCKET_ENABLE:
            return bats

        if target_w < PV_BUCKET_1_TOTAL_W:
            wanted = 1
        elif target_w < PV_BUCKET_2_TOTAL_W:
            wanted = 2
        else:
            wanted = len(bats)

        if PV_MIN_PER_BAT_W > 0:
            wanted = min(wanted, max(1, int(target_w // PV_MIN_PER_BAT_W)))
        wanted = max(1, min(wanted, len(bats)))

        self._pv_rotate_maybe_refresh()
        order = self.pv_rotate.get("order") or list(self.batteries.keys())
        idx   = int(self.pv_rotate.get("idx", 0))
        order = [x for x in order if x in states_no_bms] or bats

        rotated = (order[idx:] + order[:idx]) if PV_ROTATE_ENABLE else order
        rotated_sorted = sorted(
            rotated,
            key=lambda n: (int(states_no_bms[n].get("soc", 0)), rotated.index(n))
        )
        subset = rotated_sorted[:wanted]

        if PV_ROTATE_ENABLE and PV_ROTATE_MODE == "cycle" and wanted < len(bats):
            # FIX 22: nur einmal je Lade-Phase weiterschalten (mode_since aendert sich bei Moduswechsel)
            _rot_sess = getattr(self, "mode_since", None)
            if getattr(self, "_pv_rot_session", None) != _rot_sess:
                self._pv_rot_session = _rot_sess
                self._pv_rotate_step()

        return subset

    # -------------------------------------------------------------------------
    # Logging Helpers
    # -------------------------------------------------------------------------

    def _meter_info(self, age, max_age):
        """FIX 47: sensor.marstek_meter_info fuers Dashboard - nur Anzeige, aendert nichts an der Regelung."""
        try:
            now = time.time()
            stale = bool(getattr(self, '_meter_stale', False))
            st = [g for g in getattr(self, '_meter_stops', []) if now - g[0] < 86400]  # [Beginn, laengstes Alter]
            if stale and not getattr(self, '_meter_stale_prev', False):
                st.append([now, 0.0])
            if stale and st and age is not None:
                st[-1][1] = max(st[-1][1], float(age))
            self._meter_stops, self._meter_stale_prev = st, stale
            iv = sorted(x for x in getattr(self, '_meter_intervals', []) if x <= 900)
            avg = int(round(sum(iv) / len(iv))) if iv else None
            mx = int(round(max([iv[-1] if iv else 0.0] + [g[1] for g in st]))) or None
            sug = None
            if mx and mx <= 300 and (len(iv) >= 5 or st):
                sug = int(max(60, -(-int(mx * 1.3) // 30) * 30))
            ma = int(round(float(max_age or 0)))
            if ma <= 0:
                status = 'Pruefung aus'
            elif stale:
                status = 'STOP - Zaehlerwert zu alt'
            elif mx and mx > 300:
                status = f'Zaehler faellt zeitweise aus (bis {mx} s) - Zaehler bzw. WLAN pruefen'
            elif sug and sug > ma:
                status = f'OK - Vorschlag: Wartezeit auf {sug} s stellen'
            else:
                status = 'OK'
            self.set_state('sensor.marstek_meter_info', state=(int(round(age)) if age is not None else 'unknown'), attributes={
                'friendly_name': 'Zaehler: Alter des letzten Werts', 'unit_of_measurement': 's',
                'icon': 'mdi:timer-outline', 'status': status, 'avg_interval_s': avg, 'max_gap_s': mx,
                'stop_after_s': ma, 'stops_24h': len(st), 'suggested_s': sug, 'samples': len(iv),
                'meter': str(globals().get('GRID_SENSOR') or POWER_SENSOR.get('entity') or '')})
        except Exception as e:
            self.log(f'ZAEHLER-INFO: {e}')

    def _ext_power(self, eid):
        """FIX 46: Leistung eines optionalen Sensors in W (kW wird umgerechnet), 0.0 wenn leer/unlesbar."""
        eid = str(eid or '').strip()
        if not eid:
            return 0.0
        try:
            full = self.get_state(eid, attribute='all') or {}
            v = float(full.get('state'))
            unit = str((full.get('attributes') or {}).get('unit_of_measurement') or 'W').strip().lower()
            return v * 1000.0 if unit == 'kw' else v
        except Exception:
            return 0.0

    def _fmt_ts(self, ts):
        try:
            ts = float(ts or 0)
        except Exception:
            ts = 0
        if ts <= 0:
            return "nie"
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    def _fmt_dh(self, seconds):
        try:
            s = int(seconds)
        except Exception:
            s = 0
        if s <= 0:
            return "0d 0h"
        d = s // 86400; s = s % 86400
        h = s // 3600;  s = s % 3600
        m = s // 60
        return f"{d}d {h}h {m}m"

    def _log_cycle_times(self, where_txt):
        now_ts   = float(self._now_epoch or time.time())
        last_txt = self._fmt_ts(self._cycle_last_epoch)
        next_txt = self._fmt_ts(self._cycle_next_epoch)
        if self._cycle_next_epoch and self._cycle_next_epoch > now_ts:
            rem        = int(self._cycle_next_epoch - now_ts)
            next_extra = f"(in {self._fmt_dh(rem)})"
        elif self._cycle_next_epoch and self._cycle_next_epoch <= now_ts:
            next_extra = "(faellig/ueberfaellig)"
        else:
            next_extra = ""
        self.log(f"CYCLE {where_txt}: last={last_txt} | next={next_txt} {next_extra} | count={self._cycle_counter}")

    # -------------------------------------------------------------------------
    # Modbus Wrapper (fuer HA-Export / direkten Zugriff)
    # -------------------------------------------------------------------------

    def _modbus_read(self, client, unit, addr, count=1, name=""):
        return _modbus_read_generic(client, unit, addr, count, self.log, name)

    # -------------------------------------------------------------------------
    # Persistenz BMS
    # -------------------------------------------------------------------------

    def _load_bms_state(self):
        if not ENABLE_BMS_CARE:
            return
        path = self._bms_state_path
        try:
            d = os.path.dirname(path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass
        if not os.path.exists(path):
            self.log(f"BMS-STATE: keine Datei vorhanden ({path}), starte mit Defaults")
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            st  = data.get("bms_state", {})
            day = data.get("bms_day", None)
            cnt = data.get("bms_today_counter", 0)
            for name in self.batteries.keys():
                if name in st and isinstance(st[name], dict):
                    for k in ("last_full_ts", "phase", "holding_until", "cooldown_until",
                              "min_soc_since", "safety_done", "safety_active", "charging_since"):
                        if k in st[name]:
                            self.bms_state[name][k] = st[name][k]
            try:
                if day:
                    self.bms_day = datetime.strptime(day, "%Y-%m-%d").date()
            except Exception:
                pass
            try:
                self.bms_today_counter = int(cnt or 0)
            except Exception:
                self.bms_today_counter = 0
            self.log(f"BMS-STATE: geladen aus {path}")
        except Exception as e:
            self.error(f"BMS-STATE: Laden fehlgeschlagen ({path}): {e}")

    def _save_bms_state(self):
        if not ENABLE_BMS_CARE:
            return
        path    = self._bms_state_path
        payload = {
            "ts": time.time(),
            "bms_day": self.bms_day.strftime("%Y-%m-%d"),
            "bms_today_counter": int(self.bms_today_counter),
            "bms_state": self.bms_state
        }
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True, indent=2)
            os.replace(tmp, path)
        except Exception as e:
            self.error(f"BMS-STATE: Speichern fehlgeschlagen ({path}): {e}")

    def _bms_any_active(self):
        if not ENABLE_BMS_CARE:
            return False
        for bms in self.bms_state.values():
            if bms.get("phase") in ("charging", "hold"):
                return True
        return False

    # -------------------------------------------------------------------------
    # Persistenz PV-Rotation
    # -------------------------------------------------------------------------

    def _load_pv_rotate_state(self):
        path = self._bms_state_path
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            pv = data.get("pv_rotate", None)
            if not isinstance(pv, dict):
                return
            order = pv.get("order", None)
            idx   = pv.get("idx", 0)
            day   = pv.get("day", None)
            bats  = list(self.batteries.keys())
            if isinstance(order, list):
                order = [x for x in order if x in bats]
                for x in bats:
                    if x not in order:
                        order.append(x)
            else:
                order = bats
            if not order:
                order = bats
            self.pv_rotate["order"] = order
            self.pv_rotate["idx"]   = int(idx or 0) % max(1, len(order))
            if day:
                self.pv_rotate["day"] = str(day)
        except Exception:
            return

    def _save_pv_rotate_state(self):
        path = self._bms_state_path
        try:
            data = {}
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            data["pv_rotate"] = self.pv_rotate
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=True, indent=2)
            os.replace(tmp, path)
        except Exception:
            return

    def _pv_rotate_maybe_refresh(self):
        if not PV_ROTATE_ENABLE:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if PV_ROTATE_MODE == "daily" and self.pv_rotate.get("day") != today:
            order = list(self.batteries.keys())
            random.shuffle(order)
            self.pv_rotate = {"order": order, "idx": 0, "day": today}
            self._save_pv_rotate_state()

    def _pv_rotate_step(self):
        if not PV_ROTATE_ENABLE:
            return
        order = self.pv_rotate.get("order") or list(self.batteries.keys())
        if not order:
            return
        self.pv_rotate["idx"] = (int(self.pv_rotate.get("idx", 0)) + 1) % len(order)
        self._save_pv_rotate_state()

    # -------------------------------------------------------------------------
    # Log Cleanup
    # -------------------------------------------------------------------------

    def _cleanup_old_logs(self, kwargs=None):
        try:
            base = LOG_CLEANUP_PATH
            if not os.path.isdir(base):
                self.log(f"LOG-CLEANUP: Pfad existiert nicht: {base}", level="WARNING")
                return
            now     = time.time()
            max_age = int(LOG_RETENTION_DAYS) * 86400
            removed = 0
            for fn in os.listdir(base):
                if not fn.endswith(LOG_CLEANUP_GLOB):
                    continue
                path = os.path.join(base, fn)
                if not os.path.isfile(path):
                    continue
                try:
                    st = os.stat(path)
                except Exception:
                    continue
                age = now - st.st_mtime
                if age > max_age:
                    try:
                        os.remove(path)
                        removed += 1
                        self.log(f"LOG-CLEANUP: geloescht {fn} (alt {int(age/86400)} Tage)")
                    except Exception as e:
                        self.error(f"LOG-CLEANUP: konnte {fn} nicht loeschen: {e}")
            if removed:
                self.log(f"LOG-CLEANUP: {removed} Logdateien entfernt (>{LOG_RETENTION_DAYS} Tage)")
            else:
                self.log("LOG-CLEANUP: nichts zu tun")
        except Exception as e:
            self.error(f"LOG-CLEANUP Fehler: {e}")

    # -------------------------------------------------------------------------
    # BMS CARE
    # -------------------------------------------------------------------------

    def _bms_reset_daily_counter_if_needed(self):
        today = datetime.now().date()
        if today != self.bms_day:
            self.bms_day           = today
            self.bms_today_counter = 0
            self._save_bms_state()

    def _bms_battery_block_discharge(self, name):
        if not ENABLE_BMS_CARE:
            return False
        bms = self.bms_state.get(name)
        if bms is None:
            bms = {"min_soc_since": 0, "safety_done": False}
            self.bms_state[name] = bms
        if not bms:
            return False
        now_ts = float(self._now_epoch or time.time())
        if bms.get("phase") == "hold" and now_ts < float(bms.get("holding_until") or 0):
            return True
        if now_ts < float(bms.get("cooldown_until") or 0):
            return True
        return False

    def _bms_next_due_ts(self, name):
        if not ENABLE_BMS_CARE:
            return 0
        bms  = self.bms_state.get(name)
        if not bms:
            return 0
        last = float(bms.get("last_full_ts") or 0)
        if last <= 0:
            return 0
        return last + (FULL_CHARGE_INTERVAL_DAYS * 86400)

    def _log_bms_schedule(self):
        if not ENABLE_BMS_CARE:
            return
        now_ts = float(self._now_epoch or time.time())
        self.log("BMS-ZEITPLAN: Letzter Zyklus / Naechste Faelligkeit / Status (Hold/Cooldown)")
        for name in self.batteries.keys():
            bms        = self.bms_state.get(name, {})
            phase      = bms.get("phase", "idle")
            last_ts    = float(bms.get("last_full_ts") or 0)
            next_ts    = self._bms_next_due_ts(name)
            hold_until = float(bms.get("holding_until") or 0)
            cool_until = float(bms.get("cooldown_until") or 0)

            if next_ts > 0:
                rem_s     = int(next_ts - now_ts)
                next_info = (f"{self._fmt_ts(next_ts)} "
                             f"({'faellig' if rem_s <= 0 else 'in ' + self._fmt_dh(rem_s)})")
            else:
                next_info = "sofort moeglich (kein letzter Zyklus gespeichert)"

            hold_info = (f"aktiv bis {self._fmt_ts(hold_until)} ({self._fmt_dh(int(hold_until-now_ts))})"
                         if hold_until > now_ts else
                         (f"beendet am {self._fmt_ts(hold_until)}" if hold_until > 0 else "kein Hold aktiv"))
            cool_info = (f"aktiv bis {self._fmt_ts(cool_until)} ({self._fmt_dh(int(cool_until-now_ts))})"
                         if cool_until > now_ts else
                         (f"beendet am {self._fmt_ts(cool_until)}" if cool_until > 0 else "kein Cooldown aktiv"))
            phase_de  = {"idle": "Bereitschaft", "charging": "Volladung aktiv",
                         "hold": "Voll geladen (Haltephase)"}.get(phase, phase)

            self.log(f"BMS {name}: Letzte Voll-Ladung: {self._fmt_ts(last_ts)} | "
                     f"Naechste Faelligkeit: {next_info} | Phase: {phase_de} | "
                     f"Hold: {hold_info} | Cooldown: {cool_info}")

    def _bms_mark_cycle_done(self, name, now_ts, reason):
        if not ENABLE_BMS_CARE:
            return
        bms = self.bms_state.get(name)
        if not bms:
            return
        bms["last_full_ts"]   = float(now_ts)
        bms["phase"]          = "idle"
        bms["holding_until"]  = 0
        bms["cooldown_until"] = 0
        self.log(f"BMS {name}: cycle done ({reason}), last_full_ts={int(now_ts)}")
        self._save_bms_state()

    def _bms_select_candidate(self, states):
        if not ENABLE_BMS_CARE:
            return None
        self._bms_reset_daily_counter_if_needed()
        now_ts = float(self._now_epoch or time.time())
        for name in self.batteries.keys():
            bms = self.bms_state.get(name, {})
            if bms.get("phase") in ("charging", "hold"):
                return name
        # FIX 18: Tageslimit erst NACH der laufenden Batterie pruefen
        if self.bms_today_counter >= MAX_BMS_BATTERIES_PER_DAY:
            return None
        # FIX 20: neue BMS-Vollladung nur bei PV-Ueberschuss starten (kein Netzstrom nachts)
        # FIX 24: PV bevorzugt - ohne PV erst starten, wenn BMS_CARE_PV_WAIT_H ueberfaellig
        _bms_pv_ok = (not globals().get("BMS_CARE_REQUIRE_PV", True)) or bool(getattr(self, "pv_surplus", False))
        for name, st in states.items():
            # FIX 10: Batterien mit Lesefehler nicht als BMS-Kandidat waehlen
            if st.get("err"):
                continue
            bms = self.bms_state.get(name, {})
            soc = int(st.get("soc", 0))
            if soc >= FULL_CHARGE_SOC:
                self._bms_mark_cycle_done(name, now_ts, "soc already full")
                continue
            last = float(bms.get("last_full_ts") or 0)
            if last <= 0 and name in self.bms_state:
                # FIX 44: unbekannter Stand -> gilt als faellig, aber mit PV-Wartezeit (kein Sofort-Laden aus dem Netz)
                last = now_ts - FULL_CHARGE_INTERVAL_DAYS * 86400
                self.bms_state[name]['last_full_ts'] = last
                self._save_bms_state()
                self.log(f'BMS {name}: kein Vollladedatum bekannt -> faellig, wartet auf PV-Ueberschuss')
            if last > 0 and (now_ts - last) < (FULL_CHARGE_INTERVAL_DAYS * 86400):
                # FIX 50: vorziehen bei grossem Zell-Unterschied - nur Modbus, nur mit PV-Ueberschuss, Mindestabstand
                if not (self._cell_early(name) and bool(getattr(self, 'pv_surplus', False))
                        and (now_ts - last) >= float(globals().get('BMS_CARE_CELL_MIN_DAYS', 3)) * 86400):
                    continue
                self.log(f"BMS {name}: Zellspannungen {((getattr(self, '_cell_v', {}) or {}).get(name) or {}).get('delta', ('?',))[0]} mV "
                         f"auseinander -> BMS-Pflege vorgezogen (nur mit PV-Ueberschuss)")
                return name
            if not _bms_pv_ok:
                _bms_wait = float(globals().get("BMS_CARE_PV_WAIT_H", 48)) * 3600
                if last > 0 and (now_ts - last) < (FULL_CHARGE_INTERVAL_DAYS * 86400 + _bms_wait):
                    continue  # FIX 24: faellig, wartet aber noch auf PV-Ueberschuss
                self.log(f"BMS {name}: PV-Wartezeit ({int(globals().get('BMS_CARE_PV_WAIT_H', 48))} h) abgelaufen -> Vollladung aus dem Netz")
            return name
        return None

    def _bms_cmd(self, name, driver, key, *args):
        # FIX 27: gleichen Befehl nicht jeden Zyklus neu senden (Auffrischung alle 5 min)
        if driver is None:
            return False
        c = getattr(self, "_bms_last_cmd", None)
        if c is None:
            c = self._bms_last_cmd = {}
        last = c.get(name)
        now = time.time()
        if last and last[0] == key and (now - last[1]) < 300:
            return True
        ok = driver.write_setpoints(*args)
        if ok:
            c[name] = (key, now)
        return ok

    def _bms_apply(self, name, states, verify_logs, write_logs):
        if not ENABLE_BMS_CARE:
            return False
        if name not in self.batteries:
            return False
        now_ts = float(self._now_epoch or time.time())
        bms    = self.bms_state[name]
        soc    = int(states.get(name, {}).get("soc", 0))
        driver = self._drivers.get(name)
        # FIX 18: Haltephase laeuft bis holding_until, auch wenn der SoC kurz auf 98 % faellt
        if bms.get("phase") == "hold":
            if now_ts >= float(bms.get("holding_until") or 0):
                bms["phase"] = "idle"
                write_logs.append(f"{name}: BMS HOLD beendet, COOLDOWN bis {int(bms.get('cooldown_until') or 0)} (epoch)")
                self._save_bms_state()
                return True
            if driver:
                self._bms_cmd(name, driver, "stop", 0, 0, 0)
            verify_logs.append(f"{name}: BMS HOLD aktiv bis {int(bms['holding_until'])} (epoch)")
            return True

        if soc >= FULL_CHARGE_SOC:
            if bms.get("phase") != "hold":
                bms["phase"]          = "hold"
                bms["holding_until"]  = now_ts + FULL_CHARGE_MIN_HOLD_S
                bms["cooldown_until"] = bms["holding_until"] + ALLOW_DISCHARGE_AFTER_S
                bms["last_full_ts"]   = now_ts
                bms['charging_since'] = 0  # FIX 45
                write_logs.append(f"{name}: BMS erreicht FULL_CHARGE_SOC={FULL_CHARGE_SOC}%, starte HOLD")
                self._save_bms_state()

            if driver:
                self._bms_cmd(name, driver, "stop", 0, 0, 0)
                verify_logs.append(f"{name}: BMS HOLD aktiv bis {int(bms['holding_until'])} (epoch)")

            if now_ts >= float(bms.get("holding_until") or 0):
                bms["phase"] = "idle"
                write_logs.append(f"{name}: BMS HOLD beendet, COOLDOWN bis {int(bms['cooldown_until'])} (epoch)")
                self._save_bms_state()
            return True

        if bms.get("phase") != "charging":
            bms["phase"]         = "charging"
            bms["holding_until"] = 0
            write_logs.append(f"{name}: BMS starte sanfte Volladung (charging)")
            self._bms_reset_daily_counter_if_needed()
            self.bms_today_counter += 1
            self._save_bms_state()

        # FIX 45: nur mit PV-Ueberschuss laden, sonst pausieren; nach BMS_CARE_PV_WAIT_H ohne Abschluss volle Leistung
        _w = int(BMS_FORCE_CHARGE_W)
        _budget = None
        if globals().get('BMS_CARE_REQUIRE_PV', True):
            _wait_s = float(globals().get('BMS_CARE_PV_WAIT_H', 48)) * 3600
            _due_ts = float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400
            if now_ts < _due_ts + _wait_s:  # FIX 46: 'max. Warten auf Sonne' zaehlt ab Faelligkeit, danach ohne PV durchladen
                _budget = float(getattr(self, '_surplus_w', 0.0) or 0.0)
                _w = int(min(float(BMS_FORCE_CHARGE_W), max(0.0, _budget)) // 50 * 50)
        if driver:
            if _w < int(globals().get('BMS_CARE_MIN_W', 150)):
                self._bms_cmd(name, driver, 'pause', 0, 0, 0)
                verify_logs.append(f"{name}: BMS CHARGE pausiert - kein PV-Ueberschuss ({(_budget or 0):.0f}W), wartet")
            else:
                ok = self._bms_cmd(name, driver, 'charge%d' % _w, 1, _w, 0)
                if ok:
                    verify_logs.append(f"{name}: BMS CHARGE set={_w}W" + (' (PV-Ueberschuss)' if _budget is not None else ' (Wartezeit auf Sonne abgelaufen - ohne PV)') + f" bis SoC>={FULL_CHARGE_SOC}%")
        return True

    # -------------------------------------------------------------------------
    # Verifikation
    # -------------------------------------------------------------------------

    def _verify_setpoint_execution(self, name, mode_txt, set_limit_w, verify_logs):
        if not VERIFY_SETPOINT_EXEC:
            return
        if set_limit_w < VERIFY_MIN_SET_W:
            return
        verify_logs.append(f"{name} [{LOGTXT['verify']} {mode_txt.upper()}]: set={set_limit_w}W (Verifikation aktiv)")

    # -------------------------------------------------------------------------
    # Power Allocation
    # -------------------------------------------------------------------------

    def _calc_weights(self, states, effective_mode):
        eligible = {}
        wsum     = 0.0
        for n, st in states.items():
            cfg       = st["cfg"]
            soc       = int(st["soc"])
            mn        = int(cfg["min_soc"])
            mx        = int(cfg["max_soc"])
            cap       = float(cfg["capacity_kwh"])
            bms       = self.bms_state.get(n, {})
            bms_phase = bms.get("phase", "idle")
            # FIX 30: nicht erreichbare Batterie (Lesefehler/Sperre) bekommt keinen Anteil
            if st.get("err") or st.get("backoff"):
                if DEBUG_WEIGHTS and CALC_LOG:
                    self.log(f"{n}: skip (nicht erreichbar / Lesefehler)")
                continue

            if ENABLE_BMS_CARE and bms_phase in ("charging", "hold"):
                if DEBUG_WEIGHTS and CALC_LOG:
                    self.log(f"{n}: skip (BMS eigene Batterie, phase={bms_phase})")
                continue

            if effective_mode == "charge":
                if not (self.pv_surplus or getattr(self, "allow_charge", False)):
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: skip charge (kein PV und allow_charge=False)")
                    continue
                if soc >= mx:
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: skip charge (SoC {soc}% >= MaxSoC {mx}%)")
                    continue
                rest_kwh = cap * ((mx - soc) / 100.0)
                headroom = int(cfg["max_charge_w"])

            elif effective_mode == "discharge":
                if soc <= mn:
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: {LOGTXT['skip_discharge']} (SoC {soc}% <= MinSoC {mn}%)")
                    continue
                if self._bms_battery_block_discharge(n):
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: skip discharge (BMS hold/cooldown)")
                    continue
                rest_kwh = cap * ((soc - mn) / 100.0)
                headroom = int(cfg["max_discharge_w"])
            else:
                continue

            if rest_kwh <= 0 or headroom <= 0:
                continue

            eligible[n] = {"weight": rest_kwh, "headroom": headroom}
            wsum        += rest_kwh

            if DEBUG_WEIGHTS and CALC_LOG:
                self.log(f"{n}: eligible {effective_mode} -> SoC={soc}% rest_kwh={rest_kwh:.3f} headroom={headroom}")

        if CALC_LOG and not eligible:
            for n, st in states.items():
                self.log(f"{n}: no-{effective_mode} soc={st['soc']} min={st['cfg']['min_soc']} "
                         f"bms_phase={self.bms_state.get(n, {}).get('phase')}")

        if DEBUG_WEIGHTS and CALC_LOG:
            self.log(f"Gewichte {effective_mode}: sum={wsum:.3f}, bats={list(eligible.keys())}")

        return eligible, wsum

    def _distribute_waterfill(self, delta_watt, states, effective_mode, calc_logs):
        if delta_watt <= 0:
            return {}

        eligible, wsum = self._calc_weights(states, effective_mode)

        if not eligible:
            reasons = []
            for n, st in states.items():
                soc       = int(st.get("soc", 0))
                cfg       = st["cfg"]
                bms_phase = self.bms_state.get(n, {}).get("phase", "idle")
                if effective_mode == "discharge" and soc <= int(cfg["min_soc"]):
                    reasons.append(f"{n}: SoC {soc}% <= MinSoC {cfg['min_soc']}%")
                elif effective_mode == "discharge" and self._bms_battery_block_discharge(n):
                    reasons.append(f"{n}: BMS block ({bms_phase})")
                else:
                    reasons.append(f"{n}: keine Teilnahmebedingungen erfuellt")
            calc_logs.append(
                f"{LOGTXT['no_dist']} ({'Laden' if effective_mode=='charge' else 'Entladen'}): "
                + "; ".join(reasons)
            )
            return {}

        if wsum <= 0.0:
            calc_logs.append(f"KEINE VERTEILUNG ({effective_mode}): Gesamtgewicht=0")
            return {}

        remaining    = {n: dict(weight=info["weight"], headroom=info["headroom"]) for n, info in eligible.items()}
        distribution = {n: 0 for n in remaining.keys()}
        residual     = int(delta_watt)
        round_idx    = 0

        while residual > 0 and remaining:
            round_idx  += 1
            wsum_round  = sum(info["weight"] for info in remaining.values())
            if wsum_round <= 0:
                break

            any_capped           = False
            allocated_this_round = 0
            tentative            = {}

            for n, info in remaining.items():
                tentative[n] = int(round(residual * (info["weight"] / wsum_round)))

            saturated = []
            for n, share in tentative.items():
                head = remaining[n]["headroom"]
                give = min(max(share, 0), head)
                distribution[n]          += give
                remaining[n]["headroom"] -= give
                allocated_this_round     += give
                if give < share or remaining[n]["headroom"] <= 0:
                    saturated.append(n)
                    any_capped = any_capped or (give < share)

            residual -= allocated_this_round
            for n in saturated:
                remaining.pop(n, None)

            if not any_capped and residual > 0 and remaining:
                for n in list(remaining.keys()):
                    if residual <= 0:
                        break
                    if remaining[n]["headroom"] > 0:
                        distribution[n]          += 1
                        remaining[n]["headroom"] -= 1
                        residual                 -= 1
                        if remaining[n]["headroom"] <= 0:
                            remaining.pop(n, None)

            if CALC_LOG:
                calc_logs.append(
                    f"[Verteilrunde {round_idx}] {allocated_this_round} W verteilt, "
                    f"{residual} W verbleibend aktive Batterien: {list(remaining.keys())}"
                )

        if residual > 0:
            calc_logs.append(f"Verbleibende {residual}W nicht verteilbar (Limits/SoC).")
        if CALC_LOG:
            calc_logs.append(f"Endgueltige Verteilung ({effective_mode}): {distribution}")

        return distribution

    # -------------------------------------------------------------------------
    # Main Loop
    # -------------------------------------------------------------------------

    def terminate(self):
        for _d in list(getattr(self, "_drivers", {}).values()):
            for _fn in ("_drop_client", "_close_socket"):  # FIX 33: auch API-Sockets schliessen
                if hasattr(_d, _fn):
                    try:
                        getattr(_d, _fn)()
                    except Exception:
                        pass
        _ApiHub.close_all()  # FIX 34: gemeinsamen API-Socket schliessen (sonst Antwort-Verlust nach Neustart)
        _br = getattr(self, "_mqtt_bridge", None)
        if _br is not None:
            _br.stop()

    def read_and_log(self, kwargs):
        _br = getattr(self, "_mqtt_bridge", None)
        if _br is not None:
            try:
                _br.apply_pending()
            except Exception as e:
                self.error(f"MQTT-BRIDGE: apply fehlgeschlagen: {e}")
        # FIX 31: Dashboard-Auswahl der Anbindung uebernehmen (baut den Treiber bei Aenderung neu)
        if getattr(self, "_init_done", True):
            try:
                for _n in list(self.batteries.keys()):
                    self._sync_conn_params(_n)  # FIX 36
                    self._sync_limit_params(_n)  # FIX 48
                    self._sync_comm_choice(_n)
            except Exception as e:
                self.error(f"ANBINDUNG: Umschalten fehlgeschlagen: {e}")

        # Warten bis der Init-Thread fertig ist – Zyklus ueberspringen wenn noch nicht bereit
        if not getattr(self, "_init_done", True):
            self.log("INIT laeuft noch im Hintergrund - Zyklus wird uebersprungen (Batterie-Init nicht abgeschlossen)")
            return

        if not hasattr(self, "_mb_backoff_until") or not isinstance(self._mb_backoff_until, dict):
            self._mb_backoff_until = {name: 0.0 for name in self.batteries}
        else:
            for _n in self.batteries:
                if _n not in self._mb_backoff_until:
                    self._mb_backoff_until[_n] = 0.0

        states       = {}
        consumption  = None
        sum_ac       = 0
        actual_flow  = 0.0
        calc_logs, write_logs, verify_logs = [], [], []
        ac_ctrl_now  = 0.0
        A_set        = 0.0
        grid_flow    = 0.0

        self._now_epoch        = time.time()
        self._cycle_next_epoch = float(self._now_epoch) + float(INTERVAL)

        # Session-ID beim Start merken – am Ende pruefen ob noch aktuell.
        # Wenn initialize() inzwischen lief (AppDaemon Neustart), ist dieser
        # Zyklus veraltet und darf keine Tabelle / kein Schreiben mehr ausgeben.
        _my_session = getattr(self, "_session_id", 0)

        if self._busy:
            self.log("Vorheriger Zyklus noch aktiv - ueberspringe diesen Aufruf.", level="WARNING")
            self._log_cycle_times("SKIP_BUSY")
            return

        self._busy             = True
        self._cycle_counter   += 1
        self._cycle_last_epoch = float(self._now_epoch)
        if (self._cycle_counter % max(1, int(globals().get("LOG_TABLE_EVERY_N", 4)))) == 0:  # FIX 23
            self._log_cycle_times("START")

        try:
            bms_active     = set()
            distribution   = {}
            effective_mode = "stop"

            # 1) Netz-Zaehler
            if POWER_SENSOR.get("enabled"):
                try:
                    # FIX 19: unplausible Werte verwerfen, veralteten Zaehler erkennen
                    _mfull = self.get_state((globals().get("GRID_SENSOR") or POWER_SENSOR.get("entity")), attribute="all") or {}  # FIX 46
                    _mval = float(_mfull.get("state"))
                    _munit = str((_mfull.get("attributes") or {}).get("unit_of_measurement") or "W").strip().lower()  # FIX 39
                    if _munit == "kw":
                        _mval *= 1000.0
                    if globals().get("GRID_SENSOR_INVERT", POWER_SENSOR.get("invert")):  # FIX 46
                        _mval = -_mval
                    # FIX 46: zweiter Speicher (z. B. Hybrid-Wechselrichter) herausrechnen
                    _ext_dis = self._ext_power(globals().get('EXT_STORAGE_DISCHARGE_SENSOR'))
                    _ext_chg = self._ext_power(globals().get('EXT_STORAGE_CHARGE_SENSOR'))
                    if not str(globals().get('EXT_STORAGE_CHARGE_SENSOR') or '').strip() and _ext_dis < 0:
                        _ext_chg, _ext_dis = -_ext_dis, 0.0  # ein gemeinsamer Sensor: + Entladen, - Laden
                    if _ext_dis or _ext_chg:
                        _ext_pv = self._ext_power(globals().get('EXT_PV_SENSOR'))
                        _mval += max(0.0, _ext_dis) - max(0.0, _ext_chg - max(0.0, _ext_pv))
                    _mts = 0.0
                    for _mk in ("last_reported", "last_updated", "last_changed"):
                        try:
                            _mts = max(_mts, datetime.fromisoformat(str(_mfull.get(_mk))).timestamp())
                        except Exception:
                            pass
                    _mage = (time.time() - _mts) if _mts > 0 else 0.0
                    # FIX 47: Abstand neuer Zaehlerwerte merken (fuer die Zaehler-Info im Dashboard)
                    if _mts > 0 and _mts != getattr(self, '_meter_prev_ts', 0.0):
                        if getattr(self, '_meter_prev_ts', 0.0) > 0 and 0 < _mts - self._meter_prev_ts < 3600:
                            self._meter_intervals = (getattr(self, '_meter_intervals', []) + [_mts - self._meter_prev_ts])[-40:]
                        self._meter_prev_ts = _mts
                    _mmax_w = float(globals().get("METER_MAX_ABS_W", 25000))
                    _mmax_age = float(globals().get("METER_MAX_AGE_S", 60))
                    self._meter_stale = _mmax_age > 0 and _mage > _mmax_age
                    if abs(_mval) > _mmax_w:
                        _lg = getattr(self, "_meter_last_good", None)
                        if _lg and (time.time() - _lg[1]) <= 30:
                            self.log(f"ZAEHLER: Ausreisser {_mval:.0f} W verworfen -> letzter guter Wert {_lg[0]:.0f} W", level="WARNING")
                            consumption = _lg[0]
                        else:
                            self.log(f"ZAEHLER: Ausreisser {_mval:.0f} W verworfen (kein guter Wert < 30 s)", level="WARNING")
                            consumption = None
                    elif self._meter_stale:
                        self.log(f"ZAEHLER: Wert ist {_mage:.0f} s alt (> {_mmax_age:.0f} s) -> Batterien auf STOP", level="WARNING")
                        consumption = None
                    else:
                        consumption = _mval
                        self._meter_last_good = (_mval, time.time())
                    self._meter_info(_mage, _mmax_age)  # FIX 47
                except Exception as e:
                    _lg = getattr(self, "_meter_last_good", None)
                    _mmax_age = float(globals().get("METER_MAX_AGE_S", 60))
                    self._meter_stale = (not _lg) or (_mmax_age > 0 and (time.time() - _lg[1]) > _mmax_age)
                    self.log(f"Zaehler nicht lesbar ({e}){' -> Batterien auf STOP' if self._meter_stale else ''}", level="WARNING")
                    self._meter_info(None, _mmax_age)  # FIX 47

            # 1b) HA Sensor Export (treiber-spezifisch: Modbus oder API)
            # PARALLEL (je Batterie ein Thread): HA-Export + read_state vorab.
            # Jede Batterie hat ihren eigenen RS485-LAN-Adapter -> kein Konflikt.
            # Zyklusdauer = langsamste Batterie statt Summe aller Batterien.
            import concurrent.futures as _cf
            _bat_prefetch = {}
            def _bat_job(_bn, _cfg):
                if HA_EXPORT_ENABLE:
                    try:
                        self._poll_and_export_ha_sensors(_bn, _cfg)
                    except Exception as _e:
                        self.error(f"[{_bn}] HA-Export Fehler: {_e}")
                _now = float(self._now_epoch or time.time())
                if _now < float(self._mb_backoff_until.get(_bn, 0) or 0):
                    return
                _drv = self._drivers.get(_bn)
                if _drv is None:
                    return
                try:
                    _bat_prefetch[_bn] = _drv.read_state()
                except Exception as _e:
                    self.error(f"[{_bn}] driver.read_state Exception: {_e}")
                    _bat_prefetch[_bn] = {"ac": 0, "bp": 0, "soc": 0, "ctrl": 0, "mode": 0, "err": 1}
            _t_par = time.time()
            with _cf.ThreadPoolExecutor(max_workers=max(1, len(self.batteries)), thread_name_prefix="bat_read") as _pool:
                list(_pool.map(lambda _kv: _bat_job(*_kv), list(self.batteries.items())))
            if time.time() - _t_par > 2.0:  # FIX 23: nur wenn langsam
                self.log(f"PARALLEL-READ: {len(_bat_prefetch)}/{len(self.batteries)} Batterien in {time.time() - _t_par:.1f} s")

            # 2) Batteriewerte auslesen (per Treiber)
            states, sum_ac = {}, 0

            for name, cfg in self.batteries.items():
                states[name] = dict(ac=0, bp=0, soc=0, ctrl=0, mode=0, cfg=cfg, err=0)

                now_ts = float(self._now_epoch or time.time())
                if now_ts < float(self._mb_backoff_until.get(name, 0) or 0):
                    states[name]["err"]     = 1
                    states[name]["backoff"] = True
                    self._check_comm_fallback(name, read_error=True)  # FIX 30: Rueckwechsel-Pruefung auch bei Sperre
                    continue
                states[name]["backoff"] = False

                driver = self._drivers.get(name)
                if driver is None:
                    states[name]["err"] = 1
                    continue

                try:
                    st = _bat_prefetch[name] if name in _bat_prefetch else driver.read_state()
                except Exception as e:
                    self.error(f"[{name}] driver.read_state Exception: {e}")
                    st = {"ac": 0, "bp": 0, "soc": 0, "ctrl": 0, "mode": 0, "err": 1}

                if st.get("err"):
                    self._mb_backoff_until[name] = time.time() + API_ERROR_BACKOFF_S
                    states[name]["err"] = 1
                    # FIX 15: Fallback pruefen – wechselt ggf. den Treiber
                    self._check_comm_fallback(name, read_error=True)
                    continue

                # FIX 15: Erfolgreicher Lesevorgang -> Fehlerzaehler zurueck
                self._check_comm_fallback(name, read_error=False)

                ac   = int(st.get("ac",   0))
                bp   = int(st.get("bp",   0))
                soc  = int(st.get("soc",  0))
                ctrl = int(st.get("ctrl", 0))
                mode = int(st.get("mode", 0))

                if LOG_DIAG_SIGN:
                    meaning    = "ZERO" if abs(ac) <= TOLERANCE_W else ("CHARGE" if ac < 0 else "DISCHARGE")
                    bp_meaning = "ZERO" if abs(bp) <= TOLERANCE_W else ("CHARGE" if bp < 0 else "DISCHARGE")
                    self.log(f"{name}: DIAG AC raw={ac} -> ac={ac} meaning={meaning}")
                    self.log(f"{name}: DIAG BP bp={bp} meaning={bp_meaning}")

                sum_ac += ac
                states[name].update(ac=ac, bp=bp, soc=soc, ctrl=ctrl, mode=mode)

                if ctrl not in CONTROL_ENABLED_VALUES:
                    states[name]["err"] = 1

                if DEBUG_STATE and CALC_LOG:
                    _drv_src = getattr(self._drivers.get(name), "_device_src", "")
                    _src_txt = f" | geraet={_drv_src}" if _drv_src else ""
                    self.log(f"{name}: {LOGTXT['raw_state']} soc={soc}%, bp={bp}W, ac={ac}W, "
                             f"ctrl={ctrl}, mode_reg={mode}, last_dist={self.last_distribution.get(name, 0)}"
                             f"{_src_txt}")

            # 3) Grid-Follow Basiswerte
            grid_flow   = float(consumption or 0.0)
            actual_flow = grid_flow

            bms_active = set()
            if ENABLE_BMS_CARE:
                for _n, _b in self.bms_state.items():
                    if _b.get("phase") in ("charging", "hold"):
                        bms_active.add(_n)

            # FIX 29: Hauptschalter / Batterie-Schalter aus dem Dashboard
            if not globals().get("CONTROL_ENABLE", True):
                _disabled = set(self.batteries.keys())
            else:
                _disabled = {_n for _n in self.batteries if not globals().get(_n.upper() + "_ACTIVE", True)}
            for _n in _disabled:
                if _n in states:
                    states[_n]["disabled"] = True
            bms_active |= _disabled

            soc_safety_active = set()
            if SOC_SAFETY_ENABLE:
                for _n, _b in self.bms_state.items():
                    if _b.get("safety_active"):
                        soc_safety_active.add(_n)

            # FIX 43: Laden fuer BMS-Pflege / Notladung ist kein Hausverbrauch -> aus dem Netzfluss herausrechnen
            _care_charge = 0.0
            for _n in (bms_active | soc_safety_active):
                _st = states.get(_n) or {}
                if not _st.get('disabled') and not _st.get('err'):
                    _care_charge += min(0.0, float(_st.get('ac', 0) or 0))  # ac < 0 = Laden
            # FIX 45: echter PV-Ueberschuss = -Netzfluss + alles, was gerade in Batterien geht
            _ctrl_charge = 0.0
            for _n, _st in states.items():
                if _n in bms_active or _n in soc_safety_active or _st.get('disabled') or _st.get('err'):
                    continue
                _ctrl_charge += min(0.0, float(_st.get('ac', 0) or 0))
            self._surplus_w = -float(actual_flow) - _ctrl_charge - _care_charge
            if _care_charge < 0:
                grid_flow += _care_charge  # FIX 45: nur noch fuer die PV-Erkennung

            ac_ctrl_now = 0.0
            for n, st in states.items():
                if n in bms_active or n in soc_safety_active:
                    continue
                ac_ctrl_now += float(st.get("ac", 0) or 0)

            A_set = ac_ctrl_now + actual_flow
            # FIX 45: andere Batterien entladen nicht fuer BMS-/Notladung - nehmen ihr aber auch den Ueberschuss nicht weg
            if _care_charge < 0 and A_set > 0:
                A_set = max(A_set + _care_charge, 0.0)

            if LOG_DIAG_GRID:
                self.log(f"DIAG GRID: grid_flow={grid_flow:.1f}W ac_ctrl_now={ac_ctrl_now:.1f}W "
                         f"A_set={A_set:.1f}W deadband={GRID_DEADBAND_W}W")

            # PV-Ueberschuss Zeitfilter
            if grid_flow <= PV_CHARGE_ALLOW_W:
                if self._pv_ok_since is None:
                    self._pv_ok_since = time.time()  # FIX 21: Mindestdauer PV_SURPLUS_MIN_TIME wirkt
            else:
                self._pv_ok_since     = None
                self._grid_zero_since = None

            pv_surplus = (self._pv_ok_since is not None
                          and (time.time() - self._pv_ok_since) >= PV_SURPLUS_MIN_TIME)
            pv_pending = (self._pv_ok_since is not None) and (not pv_surplus)

            self.pv_surplus       = bool(pv_surplus)
            self.pv_pending       = bool(pv_pending)
            self._last_pv_surplus = self.pv_surplus

            now_ts    = time.time()
            pending_s = None
            if self._pv_ok_since is not None:
                pending_s = int(max(0, PV_SURPLUS_MIN_TIME - (now_ts - self._pv_ok_since)))

            if CALC_LOG:
                if pv_surplus:
                    self.log(f"PV-SURPLUS=JA (Netzfluss={actual_flow:.1f}W, stable>={PV_SURPLUS_MIN_TIME}s)")
                else:
                    if self._pv_ok_since is not None and pending_s and pending_s > 0:
                        self.log(f"PV-SURPLUS_PENDING: warte noch {pending_s}s (Netzfluss={actual_flow:.1f}W)")
                    else:
                        self.log(f"PV-SURPLUS=NEIN (Netzfluss={actual_flow:.1f}W, Schwellwert<{PV_CHARGE_ALLOW_W}W)")

            # ----------------------------------------------------------------
            # FIX 8 + FIX 9: SOC-Safety min_soc_since Tracking
            # min_soc_since muss explizit gesetzt werden, damit der Trigger
            # ueberhaupt ausloesen kann. Ausserdem: safety_done zuruecksetzen
            # wenn SoC wieder ausreichend gestiegen ist (Hysterese 2%).
            # ----------------------------------------------------------------
            if SOC_SAFETY_ENABLE:
                for n, st in states.items():
                    if st.get("err"):
                        continue
                    bms = self.bms_state.get(n)
                    if not bms:
                        continue
                    soc = int(st.get("soc", 0))
                    if soc <= SOC_SAFETY_MIN_SOC:
                        # Zeitstempel setzen wenn noch nicht gesetzt
                        if not bms.get("min_soc_since"):
                            bms["min_soc_since"] = self._now_epoch
                            self._save_bms_state()
                            self.log(f"SOC-SAFETY TRACK: {n} bei {soc}% seit jetzt")
                    elif soc > SOC_SAFETY_MIN_SOC + 2:
                        # Hysterese: SoC ausreichend erholt -> Tracking + safety_done zuruecksetzen
                        changed = False
                        if bms.get("min_soc_since") and not bms.get("safety_active"):
                            bms["min_soc_since"] = 0
                            changed = True
                        if bms.get("safety_done"):
                            bms["safety_done"] = False  # FIX 9: Erlaubt neuen Zyklus
                            changed = True
                        if changed:
                            self._save_bms_state()

            # SOC-SAFETY Trigger und Ausfuehrung
            if SOC_SAFETY_ENABLE:
                for n, st in states.items():
                    if st.get("err") or st.get("disabled"):  # FIX 26
                        continue
                    bms   = self.bms_state.get(n)
                    if not bms:
                        continue
                    soc   = int(st.get("soc", 0))
                    since = float(bms.get("min_soc_since") or 0)
                    if (soc <= SOC_SAFETY_MIN_SOC and since > 0
                            and not bms.get("safety_active") and not bms.get("safety_done")
                            and (self._now_epoch - since) >= (SOC_SAFETY_AFTER_HOURS * 3600)
                            and (not SOC_SAFETY_REQUIRE_PV or self.pv_surplus  # FIX 25: PV bevorzugt,
                                 or (self._now_epoch - since) >= ((SOC_SAFETY_AFTER_HOURS + float(globals().get("SOC_SAFETY_PV_WAIT_H", 12))) * 3600))):
                        self.log(f"SOC-SAFETY START: {n} seit {SOC_SAFETY_AFTER_HOURS}h bei {soc}% - Quelle: {'PV' if self.pv_surplus else 'Netz (keine PV)'}")
                        bms["safety_active"] = True
                        self._save_bms_state()

                for n, st in states.items():
                    bms = self.bms_state.get(n)
                    if not bms or not bms.get("safety_active"):
                        continue
                    if st.get("err") or st.get("disabled"):  # FIX 26: bei Lesefehler nicht blind laden/stoppen
                        continue
                    soc    = int(st.get("soc", 0))
                    driver = self._drivers.get(n)
                    if soc < SOC_SAFETY_TARGET_SOC:
                        if driver:
                            self._bms_cmd(n, driver, "safety%d" % int(SOC_SAFETY_CHARGE_W), 1, SOC_SAFETY_CHARGE_W, 0)
                    else:
                        self.log(f"SOC-SAFETY DONE: {n} erreicht {soc}% (>= {SOC_SAFETY_TARGET_SOC}%)")
                        if driver:
                            driver.write_setpoints(0, 0, 0)
                        bms["safety_active"] = False
                        bms["safety_done"]   = True
                        bms["min_soc_since"] = 0
                        self._save_bms_state()

            # BMS CARE
            if ENABLE_BMS_CARE:
                for n, st in states.items():
                    bms = self.bms_state.get(n)
                    if not bms:
                        continue
                    if bms.get("phase") in ("charging", "hold") and int(st.get("soc", 0)) >= FULL_CHARGE_SOC:
                        pass  # FIX 18: Uebergang charging -> hold -> idle macht jetzt _bms_apply

            bms_name = self._bms_select_candidate({_k: _v for _k, _v in states.items() if not _v.get("disabled")})
            if bms_name and bms_name not in _disabled:
                skipped = self._bms_apply(bms_name, states, verify_logs, write_logs)
                if skipped and CALC_LOG:
                    calc_logs.append(f"BMS: aktiv fuer {bms_name} (Netzfluss={actual_flow:.1f}W)")

            if ENABLE_BMS_CARE:
                for n in self.batteries.keys():
                    bms = self.bms_state.get(n, {})
                    try:
                        self.set_state(f"sensor.bms_{n}_phase", state=bms.get("phase", "idle"), attributes={
                            "last_full":      self._fmt_ts(bms.get("last_full_ts")),
                            "hold_until":     self._fmt_ts(bms.get("holding_until")),
                            "cooldown_until": self._fmt_ts(bms.get("cooldown_until")),
                            # FIX 45: fuer das Dashboard - wann zuletzt, wann wieder faellig
                            'next_due': (self._fmt_ts(float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400)
                                         if float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400 > time.time()
                                         else 'faellig - wartet auf PV-Ueberschuss'),
                            'status': ({'idle': 'Bereitschaft', 'charging': 'Vollladung laeuft (nur mit PV-Ueberschuss)', 'hold': 'Haltephase (voll)'}
                                       .get(bms.get('phase', 'idle'), bms.get('phase', 'idle'))),
                        })
                    except Exception as e:
                        self.error(f"BMS-HA-SENSOR {n}: set_state fehlgeschlagen: {e}")

            # FIX 50: Zellspannungs-Unterschied je Batterie (nur Modbus)
            for n in self.batteries.keys():
                self._cell_publish(n)

            # 5) Mode-Wechsel
            now        = time.monotonic()
            prev       = self.mode_state or "stop"
            stable_for = now - float(self.mode_since or now)

            soc_safety_active_any = any(b.get("safety_active") for b in self.bms_state.values())
            new_mode = prev or "stop"

            if A_set > GRID_DEADBAND_W:
                new_mode = "discharge"
                self._grid_zero_since = None
            elif A_set < -GRID_DEADBAND_W:
                new_mode = "charge"
                self._grid_zero_since = None
            else:
                if self._grid_zero_since is None:
                    self._grid_zero_since = time.monotonic()
                zero_for = time.monotonic() - self._grid_zero_since
                if soc_safety_active_any:
                    new_mode = prev or "charge"
                else:
                    new_mode = "stop" if zero_for >= GRID_STOP_AFTER_S else (prev or "stop")

            if new_mode != prev:
                if stable_for >= STABLE_TIMER_S:
                    self.mode_state  = new_mode
                    self.mode_since  = now
                    effective_mode   = new_mode
                    if CALC_LOG:
                        calc_logs.append(
                            f"Moduswechsel: {MODE_DE.get(prev,'stopp')} -> {MODE_DE.get(new_mode,'stopp')}"
                        )
                else:
                    effective_mode = prev
            else:
                effective_mode = prev

            if CALC_LOG:
                calc_logs.append(
                    f"Netzfluss = {actual_flow:.1f} W | "
                    f"Modus: {MODE_DE.get(prev)} -> {MODE_DE.get(new_mode)} | "
                    f"stabil seit {int(stable_for)} s | wirksam: {MODE_DE.get(effective_mode)}"
                )

            # Batterien ohne BMS/SOC-SAFETY
            states_no_bms = {
                n: st for n, st in states.items()
                if n not in bms_active
                and not (SOC_SAFETY_ENABLE and self.bms_state.get(n, {}).get("safety_active"))
            }

            if effective_mode == "discharge":
                any_ok = any(
                    int(st.get("soc", 0)) > int(st["cfg"].get("min_soc", 0))
                    and not self._bms_battery_block_discharge(n)
                    for n, st in states_no_bms.items()
                )
                if not any_ok:
                    forced = "charge" if A_set < -GRID_DEADBAND_W else "stop"
                    if CALC_LOG:
                        calc_logs.append(f"HARD-OVERRIDE: discharge nicht moeglich -> {MODE_DE.get(forced)}")
                    effective_mode  = forced
                    self.mode_state = forced
                    self.mode_since = time.monotonic()

            # Verteilung
            distribution = {}

            if effective_mode == "discharge":
                target_w = int(round(max(0.0, A_set)))
                if CALC_LOG:
                    calc_logs.append(
                        f"GRID-FOLLOW(DISCHARGE): grid={grid_flow:.1f}W "
                        f"ac_ctrl_now={ac_ctrl_now:.1f}W A_set={A_set:.1f}W target={target_w}W"
                    )
                distribution = self._distribute_waterfill(target_w, states_no_bms, "discharge", calc_logs)

            elif effective_mode == "charge":
                target_w     = int(round(max(0.0, -A_set)))
                allow_charge = bool(self.pv_surplus) or (ac_ctrl_now < -TOLERANCE_W)
                self.allow_charge = bool(allow_charge)
                if CALC_LOG:
                    calc_logs.append(
                        f"GRID-FOLLOW(CHARGE): grid={grid_flow:.1f}W "
                        f"ac_ctrl_now={ac_ctrl_now:.1f}W A_set={A_set:.1f}W "
                        f"target={target_w}W allow={'YES' if allow_charge else 'NO'}"
                    )
                if allow_charge:
                    subset        = (self._select_pv_charge_subset(target_w, states_no_bms)
                                     if PV_BUCKET_ENABLE else list(states_no_bms.keys()))
                    states_subset = {n: states_no_bms[n] for n in subset}
                    distribution  = self._distribute_waterfill(target_w, states_subset, "charge", calc_logs)
                else:
                    distribution = {}
                    if CALC_LOG:
                        calc_logs.append(
                            "CHARGE PENDING: PV noch nicht stabil -> halte Setpoints."
                            if getattr(self, "pv_pending", False)
                            else "CHARGE gesperrt: kein PV-Ueberschuss."
                        )

            self._last_distribution_debug = dict(distribution)

            # FIX 19: Zaehler veraltet/ausgefallen -> keine Regelung auf alten Werten
            if getattr(self, "_meter_stale", False):
                effective_mode = "stop"
                distribution = {}
                calc_logs.append("ZAEHLER VERALTET -> Regelung STOP (Batterien im Leerlauf)")

            # 7) Schreiben (per Treiber)
            _pw = []
            def _pw_add(_n, _d, _old, *_a):
                _pw.append((_n, _d, _old, _a))
            for name, st in states.items():
                if name in _disabled:  # FIX 29: einmal STOP, danach nicht mehr ansteuern
                    _ds = getattr(self, "_dis_stopped", None)
                    if _ds is None:
                        _ds = self._dis_stopped = set()
                    if name not in _ds and not st.get("err"):
                        _drv_d = self._drivers.get(name)
                        if _drv_d is not None:
                            _pw_add(name, _drv_d, int(self.last_distribution.get(name, 0)), 0, 0, 0)
                            write_logs.append(f"{name}: STOP (per Schalter abgeschaltet)")
                            self.last_distribution[name] = 0
                            _ds.add(name)
                    continue
                getattr(self, "_dis_stopped", set()).discard(name)
                if name in bms_active:
                    calc_logs.append(f"{name}: Regelung uebersprungen - BMS-Ladung aktiv")
                    continue
                bms = self.bms_state.get(name, {})
                if bms.get("safety_active"):
                    if CALC_LOG:
                        calc_logs.append(f"{name}: Regelung uebersprungen - SOC-SAFETY aktiv")
                    continue
                if st.get("err"):
                    if CALC_LOG:
                        calc_logs.append(f"{name}: skip write (read/connect error)")
                    continue

                cfg     = st["cfg"]
                mn      = int(cfg["min_soc"])
                mx      = int(cfg["max_soc"])
                old     = int(self.last_distribution.get(name, 0))
                desired = int(distribution.get(name, 0))
                driver  = self._drivers.get(name)

                if effective_mode == "discharge" and self._bms_battery_block_discharge(name):
                    desired = 0
                    if CALC_LOG:
                        calc_logs.append(f"{name}: discharge block wegen BMS hold/cooldown -> desired=0")

                if driver is None:
                    continue

                try:
                    # STOP
                    if effective_mode == "stop":
                        need_stop = ((old != 0)
                                     or (int(st.get("mode", 0)) != 0)
                                     or (abs(int(st.get("bp", 0))) > TOLERANCE_W))
                        if need_stop:
                            _pw_add(name, driver, old, 0, 0, 0)
                            write_logs.append(f"{name}: STOP")
                            self.last_distribution[name] = 0
                        continue

                    # CHARGE
                    if effective_mode == "charge":
                        limit = min(desired, int(cfg["max_charge_w"]))
                        if int(st.get("soc", 0)) >= mx:
                            limit = 0
                        need_write = abs(limit - old) > TOLERANCE_W
                        if limit == 0:
                            if getattr(self, "pv_pending", False):
                                need_write = False
                            else:
                                need_write = ((old != 0)
                                              or (int(st.get("mode", 0)) != 0)
                                              or (abs(int(st.get("bp", 0))) > TOLERANCE_W))
                        if need_write:
                            _pw_add(name, driver, old, 1 if limit > 0 else 0, limit, 0)
                            write_logs.append(f"{name}: laden {old}->{limit}")
                            self.last_distribution[name] = limit
                            self._verify_setpoint_execution(name, "charge", limit, verify_logs)
                        continue

                    # DISCHARGE
                    elif effective_mode == "discharge":
                        limit = min(desired, int(cfg["max_discharge_w"]))
                        if int(st.get("soc", 0)) <= mn:
                            limit = 0
                        need_write = abs(limit - old) > TOLERANCE_W
                        if limit == 0:
                            need_write = ((old != 0)
                                          or (int(st.get("mode", 0)) != 0)
                                          or (abs(int(st.get("bp", 0))) > TOLERANCE_W))
                        if need_write:
                            _pw_add(name, driver, old, 2 if limit > 0 else 0, 0, limit)
                            write_logs.append(f"{name}: entladen {old}->{limit}")
                            self.last_distribution[name] = limit
                            self._verify_setpoint_execution(name, "discharge", limit, verify_logs)

                except Exception as e:
                    self.error(f"[{name}] driver.write_setpoints: {e}")
                    st["err"] = 1

            # PARALLEL-WRITE: Sollwerte an alle Batterien gleichzeitig senden
            # (je Batterie max. 1 Befehl pro Zyklus, eigener LAN-Adapter).
            if _pw:
                import concurrent.futures as _cf2
                def _pw_run(_it):
                    _n, _d, _old, _a = _it
                    try:
                        _d.write_setpoints(*_a)
                    except Exception as _e:
                        self.error(f"[{_n}] driver.write_setpoints: {_e}")
                        self.last_distribution[_n] = _old
                _t_pw = time.time()
                with _cf2.ThreadPoolExecutor(max_workers=len(_pw), thread_name_prefix="bat_write") as _pool:
                    list(_pool.map(_pw_run, _pw))
                if time.time() - _t_pw > 1.0:  # FIX 23: nur wenn langsam
                    self.log(f"PARALLEL-WRITE: {len(_pw)} Befehle in {time.time() - _t_pw:.1f} s")

            # FIX 13: BMS-Zeitplan nur alle 5 Minuten loggen, nicht jeden Zyklus
            if (self._now_epoch - float(self._last_bms_log_ts or 0)) >= 300:
                self._log_bms_schedule()
                self._last_bms_log_ts = self._now_epoch

            if (self._cycle_counter % max(1, int(globals().get("LOG_TABLE_EVERY_N", 4)))) == 0:  # FIX 23
                self._log_cycle_times("END")

        finally:
            try:
                # Stale-Callback-Schutz: Session noch aktuell?
                _stale_cycle = False
                if getattr(self, "_session_id", 0) != _my_session:
                    self.log(
                        f"VERALTETER ZYKLUS (Session {_my_session} != aktuell {self._session_id})"
                        f" – Tabelle und Schreibbefehle werden NICHT ausgefuehrt.",
                        level="WARNING"
                    )
                    _stale_cycle = True  # FIX 23: kein return im finally mehr
                # FIX 23: Tabelle nur alle LOG_TABLE_EVERY_N Zyklen - sofort bei Fehler, Moduswechsel oder Zaehlerproblem
                _tbl_n = max(1, int(globals().get("LOG_TABLE_EVERY_N", 4)))
                _tbl_important = (any(_s.get("err") for _s in states.values())
                                  or any(("Moduswechsel" in _m) or ("ZAEHLER" in _m) or ("OVERRIDE" in _m) for _m in calc_logs))
                _show_tbl = (not _stale_cycle) and (_tbl_important or (self._cycle_counter % _tbl_n) == 0)
                if _show_tbl:
                    # Geraetenamen (src) pro Batterie zusammenstellen
                    _src_map = {
                        n: getattr(d, "_device_src", "") 
                        for n, d in self._drivers.items() 
                        if isinstance(d, ApiDriver) and getattr(d, "_device_src", "")
                    }
                    if _src_map:
                        _src_info = " | ".join(f"{n}={s}" for n, s in _src_map.items())
                        self.log(f"GERAETE-IDs: {_src_info}")
                    self.log("=== PVCONTROL TABLE BEGIN ===")
                    self.log(self.header_line)
                    self.log(self.sep_line)

                    for name in self.batteries.keys():
                        st        = states.get(name, {})
                        cfg       = st.get("cfg", self.batteries.get(name, {}))
                        ctrl_val  = int(st.get("ctrl", 0))
                        ctrl_txt  = "En" if ctrl_val in CONTROL_ENABLED_VALUES else "Un"
                        err_txt   = ("ERR" if st.get("err") else "OK").ljust(COL_WIDTH["err"])
                        last_val  = int(self.last_distribution.get(name, 0))
                        soll_val  = int(getattr(self, "_last_distribution_debug", {}).get(name, 0))
                        bms_obj   = self.bms_state.get(name, {})
                        bms_phase = bms_obj.get("phase", "idle")
                        pv_txt    = "JA" if bool(getattr(self, "_last_pv_surplus", False)) else "NEI"
                        mode_reg  = int(st.get("mode", 0))
                        mode_txt  = {1: "CHG", 2: "DIS"}.get(mode_reg, "STP")

                        driver  = self._drivers.get(name)
                        fb_info = self._comm_fallback.get(name, {})
                        is_fb   = bool(fb_info.get("active", False))
                        _src = getattr(driver, "_device_src", "") if isinstance(driver, ApiDriver) else ""
                        drv_txt = ("API" if isinstance(driver, ApiDriver) else "MOD") + ("*" if is_fb else "")

                        if st.get("disabled"):
                            zustand_txt = "AUS (Schalter)"
                        elif bms_obj.get("safety_active"):
                            zustand_txt = "Laden - SOC-SAFETY"
                        elif bms_phase in ("charging", "hold"):
                            zustand_txt = "Laden - BMS aktiv"
                        else:
                            ac_val = int(st.get("ac", 0) or 0)
                            if abs(ac_val) <= TOLERANCE_W:
                                zustand_txt = "Leerlauf"
                            elif ac_val < 0:
                                zustand_txt = "Laden"
                            else:
                                zustand_txt = "Entladen"

                        since     = float(bms_obj.get("min_soc_since") or 0)
                        soc11_txt = self._fmt_dh(int(self._now_epoch - since)) if since > 0 else "-"

                        vals = [
                            name[-1].ljust(COL_WIDTH["bat"]),
                            f"{int(st.get('soc', 0))}%".ljust(COL_WIDTH["soc"]),
                            str(int(st.get("ac", 0))).ljust(COL_WIDTH["ac"]),
                            zustand_txt.ljust(COL_WIDTH["zustand"]),
                            drv_txt.ljust(COL_WIDTH["drv"]),
                            ctrl_txt.ljust(COL_WIDTH["ctrl"]),
                            mode_txt.ljust(COL_WIDTH["mode"]),
                            str(last_val).ljust(COL_WIDTH["last"]),
                            str(soll_val).ljust(COL_WIDTH["soll"]),
                            str(int(cfg.get("max_charge_w", 0))).ljust(COL_WIDTH["maxch"]),
                            str(int(cfg.get("max_discharge_w", 0))).ljust(COL_WIDTH["maxdis"]),
                            str(int(cfg.get("min_soc", 0))).ljust(COL_WIDTH["minsoc"]),
                            str(int(cfg.get("max_soc", 0))).ljust(COL_WIDTH["maxsoc"]),
                            str(bms_phase[:5]).ljust(COL_WIDTH["bms"]),
                            pv_txt.ljust(COL_WIDTH["pv"]),
                            soc11_txt.ljust(COL_WIDTH["soc11"]),
                            err_txt,
                        ]
                        self.log("| " + " | ".join(vals) + " |")

                    if CALC_LOG:
                        self.log(f"Zaehler:{consumption}W AC_sum:{sum_ac}W Netz:{actual_flow}W | "
                                 f"ac_ctrl_now:{ac_ctrl_now:.1f}W A_set:{A_set:.1f}W")
                        for m in calc_logs:
                            self.log(m)
                        for m in write_logs:
                            self.log(m)
                        for m in verify_logs:
                            self.log(m)

                    self.log("=== PVCONTROL TABLE END ===")

            except Exception as e:
                self.error(f"Tabellen-Logging fehlgeschlagen: {e}")

            self._busy = False
