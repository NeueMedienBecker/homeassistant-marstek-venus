# pv_control.py
# Dynamische Lade-/Entlade- und Stopp-Steuerung fuer mehrere Batterien
# ASCII-only, keine Umlaute, inkl. SoC-Grenzen, Max-Limits,
# schaltbare Detail-Logs, klare TX/RX-Fehlererkennung und Setpoint-Verifikation.
#
# BMS / Battery Care (restart-sicher, Persistenz in JSON):
# - BMS darf auch ohne PV laufen (kein PV-Ueberschuss-Zwang)
# - Wenn Batterie waehrend Zyklus schon FULL_CHARGE_SOC erreicht -> Zyklus sofort als erledigt
# - Maximal MAX_BMS_BATTERIES_PER_DAY pro Tag
# - Nach Voll: Hold (halten) und danach Cooldown (keine Entladung)
#
# WICHTIG:
# - Dieses Script ist bewusst "monolithisch" und vollstaendig (kein apps.yaml noetig).
# - Pfad fuer Statefile ist Addon-sicher fix (siehe BMS_STATE_FILE).
#
# Getestet auf AppDaemon/HA: keine fehlenden Attribute/Methoden wie _cycle_counter/_log_cycle_times.
# -----------------------------------------------------------------------------

import os
import json
import struct
import time
from datetime import datetime
from appdaemon.plugins.hass.hassapi import Hass
from pymodbus.client import ModbusTcpClient





# --- KONFIGURATION ----------------------------------------------------------

# --- LOG TEXTE -------------------------------------------------------------

LOGTXT: dict[str, str] = {}
LOGTXT.update({
    "pv_yes": "PV-Ueberschuss: JA",
    "pv_no": "PV-Ueberschuss: NEIN",
    "pv_pending": "PV-Ueberschuss noch nicht stabil",
    "net_flow": "Netzfluss",
    "mode_change": "Moduswechsel",
    "mode_effective": "Wirksamer Modus",
    "bms_active": "BMS aktiv",
    "bms_skip": "Normale Regelung uebersprungen wegen BMS",
    "no_dist": "Keine Leistungsverteilung moeglich",
    "delta": "Benoetigte Leistung",
    "raw_state": "ROH-ZUSTAND",
    "skip_discharge": "Entladen uebersprungen",
    "skip_charge": "Laden uebersprungen",
    "no_discharge": "kein Entladen moeglich",
    "eligible": "teilnahmeberechtigt",
    "weights": "Gewichte",
    "cycle_done": "Zyklus abgeschlossen",
    "verify": "PRUEFUNG",
    "warn": "WARNUNG",
    "idle": "bereit",
    "charging": "laedt",
    "hold": "halten",
    "safety": "soc11",
})

def _ascii_safe(s):
    return str(s).encode("ascii", "replace").decode("ascii")

for k in list(LOGTXT.keys()):
    LOGTXT[k] = _ascii_safe(LOGTXT[k])


# --- MODBUS LOGGING --------------------------------------------------------

MODBUS_LOG_OK = False      # RX OK / TX OK anzeigen
MODBUS_LOG_TXRX = False    # TX READ / TX WRITE anzeigen
MODBUS_LOG_ERROR = True   # RX FEHLER IMMER loggen (empfohlen: True)
MODBUS_DEBUG = True     # Detail-Logs Modbus-Requests/Responses



CALC_LOG = True     # Berechnungs-Logs (Moduswahl, Verteilung, Delta)
LOG_DISCHARGE = True     # Discharge-Setpoint-Logs

# Zusaetzliche Debug-Flags
DEBUG_STATE = True     # Gelesene Rohdaten/Interpretation pro Batterie
DEBUG_WEIGHTS = True     # Details, welche Batterie warum teilnimmt
DEBUG_SETPOINTS = True     # Vergleich last_distribution vs. desired/limit

# Setpoint-Ausfuehrungs-Check:
# Liest nach erfolgreich geschriebenen Setpoints AC-/Batterieleistung zurueck
# und protokolliert, ob das Geraet den Befehl offenbar ignoriert.
VERIFY_SETPOINT_EXEC = True     # Ein/Aus

# Watt-Toleranz um Null (kein Umschalten / kein Neu-Write)
TOLERANCE_W = 40
INTERVAL = 10       # Abfrage-Intervall in Sekunden
STABLE_TIMER_S = 60       # Zeit bis Mode-Wechsel (Ping-Pong-Schutz)

# Modbus-Client-Parameter
MB_TIMEOUT_S = 0.7
MB_RETRIES = 1

# --- Setpoint-Verifikation ---------------------------------

VERIFY_SETPOINT_EXEC = True     # bleibt wie gehabt

VERIFY_MIN_SET_W = 50          # <<< erst ab dieser Leistung ueberhaupt pruefen
VERIFY_TOL_W = 20               # <<< Mess-Toleranz fuer AC/BP bei der Verifikation

# ---------------------------------------------------------------------------
# SOC-SAFETY (Anti-Totlaufen unter Min-SoC)
# ---------------------------------------------------------------------------

SOC_SAFETY_ENABLE = True

SOC_SAFETY_MIN_SOC = 11          # unter/gleich diesem Wert aktiv
SOC_SAFETY_TARGET_SOC = 12       # Ziel nach Schutzladung
SOC_SAFETY_AFTER_HOURS = 48      # nach wieviel Stunden reagieren

SOC_SAFETY_CHARGE_W = 400        # sehr sanft, absichtlich klein


SOC_SAFETY_LOG_ENABLE = True     # <<< Logging EIN/AUS
SOC_SAFETY_LOG_EVERY_CYCLE = False  # <<< wenn True: jedes Cycle loggen


# ---------------------------------------------------------------------------
# LOG RETENTION
# ---------------------------------------------------------------------------

LOG_RETENTION_DAYS = 365   # <<< HIER EINSTELLEN (z.B. 30, 90, 365)
LOG_CLEANUP_PATH = "/config/appdaemon/logs"
LOG_CLEANUP_GLOB = ".log"   # nur *.log anfassen
LOG_CLEANUP_RUN_EVERY_H = 3650  # wie oft aufraeumen (Stunden)

# --- PV Surplus Zeitfilter (Anti-Wolken-PingPong) ---------------------------

PV_SURPLUS_MIN_TIME = 30   # Sekunden stabiler PV-Ueberschuss noetig
SOC_SAFETY_REQUIRE_PV = False   # NEU: Schutzladung nur bei PV-Ueberschuss
# ---------------------------------------------------------------------------
# BMS / BATTERY CARE
# ---------------------------------------------------------------------------

ENABLE_BMS_CARE = True

PV_CHARGE_ALLOW_W = -150   # < -150W = PV-Ueberschuss


FULL_CHARGE_SOC = 99          # Ziel-SoC fuer BMS-Pflege (Balancing)
# Abstand pro Batterie (Empfehlung 7-14 Tage)
FULL_CHARGE_INTERVAL_DAYS = 10
FULL_CHARGE_MIN_HOLD_S = 45 * 60     # 45 Minuten auf voll halten
ALLOW_DISCHARGE_AFTER_S = 30 * 60     # 30 Minuten danach keine Entladung

MAX_BMS_BATTERIES_PER_DAY = 1
# sanfte Leistung fuer Volladung (unter MaxCharge)
BMS_FORCE_CHARGE_W = 1200

# Persistenz-Datei (Addon-sicherer Pfad)
BMS_STATE_FILE = "/config/appdaemon/data/pv_control_bms_state.json"

# Control-Register (42000)
CONTROL_ENABLE_CANDIDATES = [21930, 21931]
CONTROL_ENABLED_VALUES = set(CONTROL_ENABLE_CANDIDATES)
CONTROL_DISABLED_VALUES = {21947, 0}

# Smartmeter (via HA Entity)
POWER_SENSOR = {
    "enabled": True,
    "entity":  "sensor.stromzaehler_sml_aktuelle_wirkleistung"
}

# Modbus-Register
REGISTERS = {
    "soc":            32104,
    "battery_power":  (32102, 2),   # signed 32-bit
    "control":        42000,
    "mode":           42010,        # 0=stop, 1=charge, 2=discharge
    "charge_set":     42020,        # W
    "discharge_set":  42021,        # W
    "ac_power":       (32202, 2),   # signed 32-bit (AC out/in)
}

# Batterie-Konfiguration
BATTERY_CONFIG = {
    "battery1": {
        "enabled":          True,
        "modbus":           True,
        "host":             "xxx.xx.xxx.xxx",
        "port":             502,
        "unit":             1,
        "capacity_kwh":     5.6,
        "max_charge_w":     2500,
        "max_discharge_w":  2500,
        "min_soc":          11,
        "max_soc":          99
    },
    "battery2": {
        "enabled":          True,
        "modbus":           True,
        "host":             "xxx.xx.xxx.xxx",
        "port":             502,
        "unit":             1,
        "capacity_kwh":     5.6,
        "max_charge_w":     2500,
        "max_discharge_w":  2500,
        "min_soc":          11,
        "max_soc":          99
    },
    "battery3": {
        "enabled":          True,
        "modbus":           True,
        "host":             "xxx.xx.xxx.xxx",
        "port":             502,
        "unit":             1,
        "capacity_kwh":     5.6,
        "max_charge_w":     2500,
        "max_discharge_w":  2500,
        "min_soc":          11,
        "max_soc":          99
    },
}

COLUMNS = [
    "bat", "soc", "bp", "ac", "zustand",
    "ctrl", "mode",
    "last", "soll",
    "maxch", "maxdis", "minsoc", "maxsoc",
    "bms", "pv", "soc11", "err"
]


HEADERS = [
    "Bat", "SoC", "BP(W)", "AC(W)", "Zustand",
    "Ctrl", "Mode",
    "Last", "Soll",
    "MxCh", "MxDs", "MinSoC", "MaxSoC",
    "BMS", "PV", "SOC11_SEIT", "Err"
]

COL_WIDTH = {
    "bat": 5, "soc": 5, "bp": 7, "ac": 7,
    "zustand": 18,      # war 12 -> reicht jetzt fuer "Laden - BMS aktiv"
    "ctrl": 6, "mode": 5,
    "last": 6, "soll": 6,
    "maxch": 6, "maxdis": 6, "minsoc": 6, "maxsoc": 6,
    "bms": 7,           # war 5 -> damit "laedt"/"bereit"/"halten" sauber passt
    "pv": 3,"soc11": 12, "err": 3
    
}



# Mapping fuer deutsche Anzeige der Modi
MODE_DE = {"charge": "laden", "discharge": "entladen", "stop": "stopp"}

# ----------------------------------------------------------------------------
# Hilfsfunktionen (Modbus / Konvertierung)
# ----------------------------------------------------------------------------


def _i32_from_u16_be(words):
    """Konvertiert 2x u16 big-endian zu signed int32."""
    if not words or len(words) != 2:
        return 0
    return struct.unpack(">i", struct.pack(">HH", words[0], words[1]))[0]


def _mb_client(host, port):
    """Erstellt einen ModbusTcpClient mit kurzen Timeouts und wenig Retries."""
    try:
        return ModbusTcpClient(host, port=port, timeout=MB_TIMEOUT_S, retries=MB_RETRIES)
    except TypeError:
        return ModbusTcpClient(host, port=port, timeout=MB_TIMEOUT_S)


def _modbus_read_generic(client, unit, addr, count, log, name):
    """
    Gelesene Register mit TX/RX-Log und Rueckmeldung.
    Rueckgabe:
      - list[int] bei Erfolg
      - None bei Fehler
    """
    prefix = f"[{name}] " if name else ""

    if MODBUS_LOG_TXRX:
        log(f"{prefix}[TX READ] addr={addr} cnt={count} unit={unit}")

    try:
        try:
            rr = client.read_holding_registers(address=addr, count=count, slave=unit)
        except TypeError:
            try:
                rr = client.read_holding_registers(address=addr, count=count, unit=unit)
            except TypeError:
                rr = client.read_holding_registers(address=addr, count=count)

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
                log(f"{prefix}[RX FEHLER] keine 'registers' im Response")
            return None

        if MODBUS_LOG_OK:
            log(f"{prefix}[RX OK] {regs}")

        return regs

    except Exception as e:
        if MODBUS_LOG_ERROR:
            log(f"{prefix}[RX FEHLER] Exception: {e}")
        return None


def _modbus_write_generic(client, unit, addr, value, log, name):
    """
    Write mit TX/RX-Log und Rueckmeldung.
    Rueckgabe:
      - True bei Erfolg
      - False bei Fehler
    """
    prefix = f"[{name}] " if name else ""
    v = int(value)

    if MODBUS_LOG_TXRX:
        log(f"{prefix}[TX WRITE] addr={addr} val={v} unit={unit}")

    try:
        try:
            wr = client.write_register(address=addr, value=v, slave=unit)
        except TypeError:
            try:
                wr = client.write_register(address=addr, value=v, unit=unit)
            except TypeError:
                wr = client.write_register(address=addr, value=v)

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

        return True

    except Exception as e:
        if MODBUS_LOG_ERROR:
            log(f"{prefix}[RX FEHLER] Exception: {e}")
        return False


# ----------------------------------------------------------------------------
# Kernklasse
# ----------------------------------------------------------------------------


class PVControlApp(Hass):

    # ------------------------- Init ----------------------------------------

    def initialize(self):
        # aktive Batterien
        self.batteries = {
            name: cfg for name, cfg in BATTERY_CONFIG.items()
            if cfg.get("enabled") and cfg.get("modbus")
        }

        self.last_distribution = {name: 0 for name in self.batteries}
        self.mode_state = None
        self.mode_since = time.monotonic() - STABLE_TIMER_S
        self._busy = False

        # PV-Surplus Zeitfilter
        self._pv_ok_since = None


        self._cycle_last_epoch = 0.0
        self._cycle_next_epoch = 0.0
        self._cycle_counter = 0
        self._now_epoch = 0.0

        header = "| " + \
            " | ".join(h.ljust(COL_WIDTH[c])
                       for c, h in zip(COLUMNS, HEADERS)) + " |"
        sep = "|-" + "-|-".join("-" * COL_WIDTH[c] for c in COLUMNS) + "-|"
        self.header_line = header
        self.sep_line = sep

        # BMS State
        self.bms_state = {
            name: {
                "last_full_ts": 0,
                "phase": "idle",
                "holding_until": 0,
                "cooldown_until": 0,
                "min_soc_since": 0,        # <<< NEU
                "safety_done": False,      # <<< NEU
                "safety_active": False,   # <<< HIER NEU
            } for name in self.batteries
        }
        self.bms_day = datetime.now().date()
        self.bms_today_counter = 0

        self._bms_state_path = BMS_STATE_FILE
        self._load_bms_state()

        # 🔥 HIER ist der entscheidende Teil
        self.log(
            f"Starte PVControlApp, INTERVAL={INTERVAL}s, BMS_CARE={'ON' if ENABLE_BMS_CARE else 'OFF'}")
        self._log_bms_schedule()

        # PV-Regel-Loop
        self.run_every(self.read_and_log, self.datetime(), INTERVAL)

        # Log-Retention
        self.run_every(
            self._cleanup_old_logs,
            self.datetime(),
            int(LOG_CLEANUP_RUN_EVERY_H) * 3600
        )

    # ------------------------- Logging Helpers -----------------------------

    def _fmt_ts(self, ts):
        """Epoch -> lokales Datum/Uhrzeit, ts=0 -> 'nie'."""
        try:
            ts = float(ts or 0)
        except Exception:
            ts = 0
        if ts <= 0:
            return "nie"
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    def _log_cycle_times(self, where_txt):
        """
        Protokolliert, wann der letzte Zyklus war und wann der naechste geplant ist.
        where_txt: z.B. 'START', 'END', 'SKIP_BUSY', 'BMS_RETURN'
        """
        now_ts = float(self._now_epoch or time.time())
        last_txt = self._fmt_ts(self._cycle_last_epoch)
        next_txt = self._fmt_ts(self._cycle_next_epoch)

        if self._cycle_next_epoch and self._cycle_next_epoch > now_ts:
            rem = int(self._cycle_next_epoch - now_ts)
            next_extra = f"(in {self._fmt_dh(rem)})"
        elif self._cycle_next_epoch and self._cycle_next_epoch <= now_ts:
            next_extra = "(faellig/ueberfaellig)"
        else:
            next_extra = ""

        self.log(
            f"CYCLE {where_txt}: last={last_txt} | next={next_txt} {next_extra} | count={self._cycle_counter}"
        )
        
    def _fmt_dh(self, seconds):
        """
        Sekunden -> 'Xd Yh' oder 'Xd Yh Zm' (ASCII-only).
        """
        try:
            s = int(seconds)
        except Exception:
            s = 0
    
        if s <= 0:
            return "0d 0h"
    
        d = s // 86400
        s = s % 86400
        h = s // 3600
        s = s % 3600
        m = s // 60
    
        # ohne Minuten:
        # return f"{d}d {h}h"
    
        # mit Minuten (empfohlen, weil deine Logs Dezimalstunden haben):
        return f"{d}d {h}h {m}m"


    # ------------------------- Modbus Wrapper ------------------------------

    def _modbus_read(self, client, unit, addr, count=1, name=""):
        return _modbus_read_generic(client, unit, addr, count, self.log, name)

    def _modbus_write(self, client, unit, addr, value, name=""):
        return _modbus_write_generic(client, unit, addr, value, self.log, name)

    def _ensure_control_enabled(self, client, unit, current_ctrl, name):
        """Sorgt dafuer, dass Control (42000) auf 'enabled' steht."""
        if current_ctrl in CONTROL_ENABLED_VALUES:
            return current_ctrl, False

        for code in CONTROL_ENABLE_CANDIDATES:
            ok = self._modbus_write(
                client, unit, REGISTERS["control"], code, name)
            if not ok:
                continue
            time.sleep(0.05)
            regs = self._modbus_read(
                client, unit, REGISTERS["control"], 1, name)
            if not regs:
                continue
            val = regs[0]
            if val in CONTROL_ENABLED_VALUES:
                return val, True
            current_ctrl = val

        return current_ctrl, False

    # ------------------------- Persistenz (BMS) ----------------------------

    def _load_bms_state(self):
        if not ENABLE_BMS_CARE:
            return

        path = self._bms_state_path
        # ensure directory exists (best-effort)
        try:
            d = os.path.dirname(path)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass

        if not os.path.exists(path):
            self.log(
                f"BMS-STATE: keine Datei vorhanden ({path}), starte mit Defaults")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            st = data.get("bms_state", {})
            day = data.get("bms_day", None)
            cnt = data.get("bms_today_counter", 0)

            for name in self.batteries.keys():
                if name in st and isinstance(st[name], dict):
                    for k in (
                        "last_full_ts",
                        "phase",
                        "holding_until",
                        "cooldown_until",
                        "min_soc_since",   # <<< WICHTIG
                        "safety_done",     # <<< WICHTIG
                        "safety_active",
                    ):
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
        path = self._bms_state_path
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
        """
        True, wenn irgendeine Batterie aktuell im BMS-Zyklus ist
        (charging oder hold).
        """
        if not ENABLE_BMS_CARE:
            return False
        for bms in self.bms_state.values():
            if bms.get("phase") in ("charging", "hold"):
                return True
        return False

    # ------------------------- Log Cleanup ---------------------------------

    def _cleanup_old_logs(self, kwargs=None):
        """
        Loescht Logdateien aelter als LOG_RETENTION_DAYS.
        Sicher:
        - nur Dateien mit Endung LOG_CLEANUP_GLOB (Default: ".log")
        - nur im konfigurierten Verzeichnis LOG_CLEANUP_PATH
        """
        try:
            base = LOG_CLEANUP_PATH
            if not os.path.isdir(base):
                self.log(
                    f"LOG-CLEANUP: Pfad existiert nicht: {base}", level="WARNING")
                return

            now = time.time()
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
                        self.log(
                            f"LOG-CLEANUP: geloescht {fn} "
                            f"(alt {int(age / 86400)} Tage)"
                        )
                    except Exception as e:
                        self.error(
                            f"LOG-CLEANUP: konnte {fn} nicht loeschen: {e}"
                        )

            if removed:
                self.log(
                    f"LOG-CLEANUP: {removed} Logdateien entfernt "
                    f"(>{LOG_RETENTION_DAYS} Tage)"
                )
            else:
                self.log("LOG-CLEANUP: nichts zu tun")

        except Exception as e:
            self.error(f"LOG-CLEANUP Fehler: {e}")

    # ------------------------- BMS CARE ------------------------------------

    def _bms_reset_daily_counter_if_needed(self):
        today = datetime.now().date()
        if today != self.bms_day:
            self.bms_day = today
            self.bms_today_counter = 0
            self._save_bms_state()

    def _bms_battery_block_discharge(self, name):
        """
        True, wenn Batterie aktuell NICHT entladen werden darf
        (hold oder cooldown).
        """
        if not ENABLE_BMS_CARE:
            return False
        bms = self.bms_state.get(name)
        if bms is None:
            # sollte praktisch nie passieren, aber verhindert genau diesen Crash
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
        """Naechste Faelligkeit (epoch) fuer BMS-Zyklus aus last_full_ts."""
        if not ENABLE_BMS_CARE:
            return 0
        bms = self.bms_state.get(name)
        if not bms:
            return 0
        last = float(bms.get("last_full_ts") or 0)
        if last <= 0:
            return 0
        return last + (FULL_CHARGE_INTERVAL_DAYS * 86400)

    def _log_bms_schedule(self):
        """Protokolliert pro Batterie: letzter Zyklus, naechster Zyklus, Status/Timer."""
        if not ENABLE_BMS_CARE:
            return

        now_ts = float(self._now_epoch or time.time())
        self.log("BMS-SCHEDULE: last cycle / next due / status (hold/cooldown)")

        for name in self.batteries.keys():
            bms = self.bms_state.get(name, {})
            phase = bms.get("phase", "idle")
            last_ts = float(bms.get("last_full_ts") or 0)

            next_ts = self._bms_next_due_ts(name)
            hold_until = float(bms.get("holding_until") or 0)
            cool_until = float(bms.get("cooldown_until") or 0)

            if next_ts > 0:
                rem_s = int(next_ts - now_ts)
                if rem_s <= 0:
                    next_info = f"{self._fmt_ts(next_ts)} (faellig)"
                else:
                    next_info = f"{self._fmt_ts(next_ts)} (in {self._fmt_dh(rem_s)})"
            else:
                next_info = "sofort moeglich (kein last_full_ts)"

            hold_info = "-"
            if hold_until > now_ts:
                hold_info = f"bis {self._fmt_ts(hold_until)} ({int(hold_until - now_ts)}s)"
            elif hold_until > 0:
                hold_info = f"beendet {self._fmt_ts(hold_until)}"

            cool_info = "-"
            if cool_until > now_ts:
                cool_info = f"bis {self._fmt_ts(cool_until)} ({int(cool_until - now_ts)}s)"
            elif cool_until > 0:
                cool_info = f"beendet {self._fmt_ts(cool_until)}"

            self.log(
                f"BMS {name}: last={self._fmt_ts(last_ts)} | next={next_info} | "
                f"phase={phase} | hold={hold_info} | cooldown={cool_info}"
            )

    def _bms_mark_cycle_done(self, name, now_ts, reason):
        """
        Markiert BMS-Zyklus als erledigt.
        Wichtig: auch wenn Batterie einfach so FULL wurde.
        """
        if not ENABLE_BMS_CARE:
            return
        bms = self.bms_state.get(name)
        if not bms:
            return

        bms["last_full_ts"] = float(now_ts)
        bms["phase"] = "idle"
        bms["holding_until"] = 0
        bms["cooldown_until"] = 0
        self.log(
            f"BMS {name}: cycle done ({reason}), last_full_ts={int(now_ts)}")
        self._save_bms_state()

    def _bms_select_candidate(self, states):
        """
        Waehlt eine Batterie fuer BMS-Pflege aus.

        Regeln:
        - Kein PV-Ueberschuss-Zwang (BMS kann auch ohne PV)
        - Wenn Batterie bereits FULL_CHARGE_SOC erreicht -> Zyklus als erledigt markieren
        - Maximal MAX_BMS_BATTERIES_PER_DAY pro Tag
        - Wenn bereits eine Batterie in phase charging/hold ist -> diese hat Vorrang
        """
        if not ENABLE_BMS_CARE:
            return None

        self._bms_reset_daily_counter_if_needed()
        if self.bms_today_counter >= MAX_BMS_BATTERIES_PER_DAY:
            return None

        now_ts = float(self._now_epoch or time.time())

        # Wenn irgendeine Batterie schon im BMS-Lauf ist, hat sie Vorrang
        for name in self.batteries.keys():
            bms = self.bms_state.get(name, {})
            if bms.get("phase") in ("charging", "hold"):
                return name

        # sonst: faellige Batterie suchen
        for name, st in states.items():
            bms = self.bms_state.get(name, {})
            soc = int(st.get("soc", 0))

            # Wenn Batterie schon voll (egal warum): sofort Zyklus erledigt
            if soc >= FULL_CHARGE_SOC:
                self._bms_mark_cycle_done(name, now_ts, "soc already full")
                continue

            last = float(bms.get("last_full_ts") or 0)
            if last > 0:
                if (now_ts - last) < (FULL_CHARGE_INTERVAL_DAYS * 86400):
                    continue

            # faellig
            return name

        return None

    def _bms_apply(self, name, states, verify_logs, write_logs):
        """
        Fuehrt den BMS-Pflegelauf fuer genau diese Batterie aus.
        Return:
          True  -> normaler Regelbetrieb soll uebersprungen werden
          False -> normaler Regelbetrieb darf weiterlaufen
        """
        if not ENABLE_BMS_CARE:
            return False

        if name not in self.batteries:
            return False

        now_ts = float(self._now_epoch or time.time())
        cfg = self.batteries[name]
        bms = self.bms_state[name]
        soc = int(states.get(name, {}).get("soc", 0))

        # Wenn Batterie bereits voll: Hold starten oder weiter halten
        if soc >= FULL_CHARGE_SOC:
            if bms.get("phase") != "hold":
                bms["phase"] = "hold"
                bms["holding_until"] = now_ts + FULL_CHARGE_MIN_HOLD_S
                bms["cooldown_until"] = bms["holding_until"] + \
                    ALLOW_DISCHARGE_AFTER_S
                bms["last_full_ts"] = now_ts
                write_logs.append(
                    f"{name}: BMS erreicht FULL_CHARGE_SOC={FULL_CHARGE_SOC}%, starte HOLD")
                self._save_bms_state()

            # In Hold: Setpoints auf 0, Mode auf stop (schonend)
            client = _mb_client(cfg["host"], cfg["port"])
            if client.connect():
                try:
                    regs_ctrl = self._modbus_read(
                        client, cfg["unit"], REGISTERS["control"], 1, name)
                    ctrl_val = regs_ctrl[0] if regs_ctrl else 0
                    ctrl_val, changed = self._ensure_control_enabled(
                        client, cfg["unit"], ctrl_val, name)
                    if changed:
                        write_logs.append(
                            f"{name}: control freigeschaltet ({ctrl_val})")
                        time.sleep(0.05)

                    ok1 = self._modbus_write(
                        client, cfg["unit"], REGISTERS["charge_set"], 0, name)
                    time.sleep(0.03)
                    ok2 = self._modbus_write(
                        client, cfg["unit"], REGISTERS["discharge_set"], 0, name)
                    time.sleep(0.03)
                    ok3 = self._modbus_write(
                        client, cfg["unit"], REGISTERS["mode"], 0, name)

                    if ok1 and ok2 and ok3:
                        verify_logs.append(
                            f"{name}: BMS HOLD aktiv bis {int(bms['holding_until'])} (epoch)")

                finally:
                    client.close()

            # Wenn Hold fertig -> phase idle setzen (Cooldown bleibt)
            if now_ts >= float(bms.get("holding_until") or 0):
                bms["phase"] = "idle"
                write_logs.append(
                    f"{name}: BMS HOLD beendet, COOLDOWN bis {int(bms['cooldown_until'])} (epoch)")
                self._save_bms_state()

            return True

        # Wenn noch nicht voll -> charging Phase
        if bms.get("phase") != "charging":
            bms["phase"] = "charging"
            bms["holding_until"] = 0
            write_logs.append(
                f"{name}: BMS starte sanfte Volladung (charging)")

            self._bms_reset_daily_counter_if_needed()
            self.bms_today_counter += 1
            self._save_bms_state()

        # Sanft laden (Mode=charge, charge_set=BMS_FORCE_CHARGE_W)
        client = _mb_client(cfg["host"], cfg["port"])
        if client.connect():
            try:
                regs_ctrl = self._modbus_read(
                    client, cfg["unit"], REGISTERS["control"], 1, name)
                ctrl_val = regs_ctrl[0] if regs_ctrl else 0
                ctrl_val, changed = self._ensure_control_enabled(
                    client, cfg["unit"], ctrl_val, name)
                if changed:
                    write_logs.append(
                        f"{name}: control freigeschaltet ({ctrl_val})")
                    time.sleep(0.05)

                ok1 = self._modbus_write(
                    client, cfg["unit"], REGISTERS["mode"], 1, name)
                time.sleep(0.03)
                ok2 = self._modbus_write(
                    client, cfg["unit"], REGISTERS["charge_set"], BMS_FORCE_CHARGE_W, name)
                if ok1 and ok2:
                    verify_logs.append(
                        f"{name}: BMS CHARGE set={BMS_FORCE_CHARGE_W}W bis SoC>={FULL_CHARGE_SOC}%")
            finally:
                client.close()

        return True

    # ------------------------- Verifikation nach Writes ----------------------

    def _verify_setpoint_execution(self, client, cfg, name, mode_txt, set_limit_w, verify_logs):
        """
        Liest AC und Battery Power nach einem gesetzten Setpoint und prueft,
        ob das Geraet reagiert. Nur Logging, keine harte Logik.
        """
        if not VERIFY_SETPOINT_EXEC:
            return
        if set_limit_w < VERIFY_MIN_SET_W:
            return

        try:
            time.sleep(0.05)

            regs_ac = self._modbus_read(
                client, cfg["unit"],
                REGISTERS["ac_power"][0],
                REGISTERS["ac_power"][1],
                name
            )
            regs_bp = self._modbus_read(
                client, cfg["unit"],
                REGISTERS["battery_power"][0],
                REGISTERS["battery_power"][1],
                name
            )

            ac_raw = _i32_from_u16_be(regs_ac) if regs_ac else None
            ac = (-ac_raw) if ac_raw is not None else None

            bp = _i32_from_u16_be(regs_bp) if regs_bp else None

            msg = f"{name} [{LOGTXT['verify']} {mode_txt.upper()}]: set={set_limit_w}W, ac={ac}W, bp={bp}W"
            if (
                ac is not None and abs(ac) < VERIFY_TOL_W
                and bp is not None and abs(bp) < VERIFY_TOL_W
            ):
                msg += " -> WARN: keine erkennbare Leistung, Geraet ignoriert Setpoint? (MinSoC/Modus/Internes Limit pruefen)"
            verify_logs.append(msg)

        except Exception as e:
            verify_logs.append(
                f"{name} [VERIFY {mode_txt.upper()}]: Verifikation fehlgeschlagen ({e})")

    # ------------------------- Power Allocation -----------------------------

    def _calc_weights(self, states, effective_mode):
        """
        Bestimmt, welche Batterien an Charge/Discharge teilnehmen.
        Gewicht = verfuegbare kWh oberhalb/unterhalb Min/Max-SoC.
        """
        eligible = {}
        wsum = 0.0

        for n, st in states.items():
            cfg = st["cfg"]
            soc = int(st["soc"])
            mn = int(cfg["min_soc"])
            mx = int(cfg["max_soc"])
            cap = float(cfg["capacity_kwh"])

            bms = self.bms_state.get(n, {})
            bms_phase = bms.get("phase", "idle")

            # BMS-Batterie nie normal verteilen
            if ENABLE_BMS_CARE and bms_phase in ("charging", "hold"):
                if DEBUG_WEIGHTS and CALC_LOG:
                    self.log(f"{n}: skip (BMS eigene Batterie, phase={bms_phase})")
                continue

            # ---------- CHARGE ----------
            if effective_mode == "charge":
                if not self.pv_surplus:
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: skip charge (kein PV-Ueberschuss)")
                    continue
                if soc >= mx:
                    if DEBUG_WEIGHTS and CALC_LOG:
                        self.log(f"{n}: skip charge (SoC {soc}% >= MaxSoC {mx}%)")
                    continue
                rest_kwh = cap * ((mx - soc) / 100.0)
                headroom = int(cfg["max_charge_w"])

            # ---------- DISCHARGE ----------
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
            wsum += rest_kwh

            if DEBUG_WEIGHTS and CALC_LOG:
                self.log(
                    f"{n}: eligible {effective_mode} -> "
                    f"SoC={soc}% rest_kwh={rest_kwh:.3f} headroom={headroom}"
                )

        if CALC_LOG and not eligible:
            for n, st in states.items():
                self.log(
                    f"{n}: no-{effective_mode} "
                    f"soc={st['soc']} min={st['cfg']['min_soc']} "
                    f"bms_phase={self.bms_state.get(n, {}).get('phase')}"
                )

        if DEBUG_WEIGHTS and CALC_LOG:
            self.log(
                f"Gewichte {effective_mode}: sum={wsum:.3f}, bats={list(eligible.keys())}"
            )

        return eligible, wsum

    


    def _distribute_waterfill(self, delta_watt, states, effective_mode, calc_logs):
        if delta_watt <= 0:
            return {}

        eligible, wsum = self._calc_weights(states, effective_mode)
        
        if not eligible:
            reasons = []
            for n, st in states.items():
                soc = int(st.get("soc", 0))
                cfg = st["cfg"]
                bms_phase = self.bms_state.get(n, {}).get("phase", "idle")
        
                if effective_mode == "discharge" and soc <= int(cfg["min_soc"]):
                    reasons.append(f"{n}: SoC {soc}% <= MinSoC {cfg['min_soc']}%")
                elif effective_mode == "discharge" and self._bms_battery_block_discharge(n):
                    reasons.append(f"{n}: BMS block ({bms_phase})")
                else:
                    reasons.append(f"{n}: keine Teilnahmebedingungen erfuellt")
        
            calc_logs.append(
                f"{LOGTXT['no_dist']} (Entladen): "
                + "; ".join(reasons)
            )

            return {}
        
        if wsum <= 0.0:
            calc_logs.append(
                f"KEINE VERTEILUNG ({effective_mode}): Gesamtgewicht=0 (alle rest_kwh=0)"
            )
            return {}


        remaining = {n: dict(
            weight=info["weight"], headroom=info["headroom"]) for n, info in eligible.items()}
        distribution = {n: 0 for n in remaining.keys()}
        residual = int(delta_watt)
        round_idx = 0

        while residual > 0 and remaining:
            round_idx += 1
            wsum_round = sum(info["weight"] for info in remaining.values())
            if wsum_round <= 0:
                break

            any_capped = False
            allocated_this_round = 0
            tentative = {}

            for n, info in remaining.items():
                share_f = residual * (info["weight"] / wsum_round)
                tentative[n] = int(round(share_f))

            saturated = []
            for n, share in tentative.items():
                head = remaining[n]["headroom"]
                give = min(max(share, 0), head)
                distribution[n] += give
                remaining[n]["headroom"] -= give
                allocated_this_round += give
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
                        distribution[n] += 1
                        remaining[n]["headroom"] -= 1
                        residual -= 1
                        if remaining[n]["headroom"] <= 0:
                            remaining.pop(n, None)
            

            if CALC_LOG:
                calc_logs.append(
                    f"[Verteilrunde {round_idx}] {allocated_this_round} W verteilt, {residual} W verbleibend "
                    f"aktive Batterien: {list(remaining.keys())}"
                )

        if residual > 0:
            calc_logs.append(
                f"Verbleibende {residual}W nicht verteilbar (Limits/SoC).")

        if CALC_LOG:
            calc_logs.append(
                f"Endgueltige Verteilung ({effective_mode}): {distribution}")

        return distribution

    # ------------------------------ Main Loop -------------------------------


    def read_and_log(self, kwargs):
        

        # Defaults fuer finally (damit nie "unbound")
        states = {}
        consumption = None
        sum_ac = 0
        actual_flow = 0.0
        calc_logs, write_logs, verify_logs = [], [], []
        
        # Zykluszeiten setzen
        self._now_epoch = time.time()
        self._cycle_next_epoch = float(self._now_epoch) + float(INTERVAL)

        if self._busy:
            self.log(
                "Vorheriger Zyklus noch aktiv - ueberspringe diesen Aufruf.", level="WARNING")
            self._log_cycle_times("SKIP_BUSY")
            return

        self._busy = True
        self._cycle_counter += 1
        self._cycle_last_epoch = float(self._now_epoch)
        self._log_cycle_times("START")

        # WICHTIG: alles ab hier laeuft in EINEM try, und am Ende kommt EIN finally
        try:
            # --- Minimaler Schutz: damit finally IMMER loggen kann ---
            states = {}
            sum_ac = 0
            consumption = None
            actual_flow = 0.0
            calc_logs, write_logs, verify_logs = [], [], []

            # (optional, aber gut) damit spaeter nix "unbound" wird:
            bms_active = set()
            distribution = {}
            effective_mode = "stop"
            # --------------------------------------------------------

            # 1) Netz-Zaehler (optional)
            consumption = None
            if POWER_SENSOR.get("enabled"):
                try:
                    consumption = float(self.get_state(POWER_SENSOR["entity"]))
                except Exception as e:
                    self.error(f"Zaehler-Lesefehler: {e}")

            # 2) Batteriewerte auslesen
            states, sum_ac = {}, 0
            for name, cfg in self.batteries.items():
                states[name] = dict(ac=0, bp=0, soc=0,
                                    ctrl=0, mode=0, cfg=cfg, err=0)

                client = _mb_client(cfg["host"], cfg["port"])
                if not client.connect():
                    self.error(f"[{name}] Verbindung fehlgeschlagen")
                    states[name]["err"] = 1
                    try:
                        client.close()
                    except Exception:
                        pass
                    continue

                try:
                    regs = self._modbus_read(
                        client, cfg["unit"],
                        REGISTERS["ac_power"][0], REGISTERS["ac_power"][1],
                        name
                    )
                    ac_raw = _i32_from_u16_be(regs) if regs else 0
                    ac = -ac_raw  # NORMALIZE: AC>0 discharge, AC<0 charge

                    regs = self._modbus_read(
                        client, cfg["unit"],
                        REGISTERS["battery_power"][0], REGISTERS["battery_power"][1],
                        name
                    )
                    bp = _i32_from_u16_be(regs) if regs else 0

                    regs = self._modbus_read(client, cfg["unit"], REGISTERS["soc"], 1, name)
                    if not regs:
                        states[name]["err"] = 1
                        soc = 0
                    else:
                        soc = int(regs[0])


                    # BMS state handle fuer diese Batterie (wird unten benutzt)
                    bms = self.bms_state.get(name)
                    if bms is None:
                        bms = {
                            "last_full_ts": 0,
                            "phase": "idle",
                            "holding_until": 0,
                            "cooldown_until": 0,
                            "min_soc_since": 0,
                            "safety_done": False,
                            "safety_active": False,
                        }
                        self.bms_state[name] = bms

                    # SOC-SAFETY: Zeit merken/reset
                    if SOC_SAFETY_ENABLE:
                        if soc <= SOC_SAFETY_MIN_SOC:
                            if not bms.get("min_soc_since"):
                                bms["min_soc_since"] = self._now_epoch
                                self._save_bms_state()
                                if SOC_SAFETY_LOG_ENABLE:
                                    self.log(
                                        f"SOC-SAFETY: {name} hat Min-SoC {SOC_SAFETY_MIN_SOC}% erreicht "
                                        f"um {self._fmt_ts(self._now_epoch)}"
                                    )
                            elif SOC_SAFETY_LOG_ENABLE and SOC_SAFETY_LOG_EVERY_CYCLE:
                                since = float(bms.get("min_soc_since") or 0)
                                if since > 0:
                                    dur = int(self._now_epoch - since)
                                    self.log(
                                        f"SOC-SAFETY: {name} bei {soc}% seit {self._fmt_dh(dur)} "
                                        f"(seit {self._fmt_ts(since)})"
                                    )
                        else:
                            bms["min_soc_since"] = 0
                            bms["safety_done"] = False
                            bms["safety_active"] = False

                    regs_ctrl = self._modbus_read(client, cfg["unit"], REGISTERS["control"], 1, name)
                    ctrl = regs_ctrl[0] if regs_ctrl else 0

                    regs_mode = self._modbus_read(client, cfg["unit"], REGISTERS["mode"], 1, name)
                    mode = regs_mode[0] if regs_mode else 0

                    sum_ac += ac
                    states[name].update(ac=ac, bp=bp, soc=soc, ctrl=ctrl, mode=mode)

                    if (not regs_ctrl) or ctrl not in CONTROL_ENABLED_VALUES:
                        states[name]["err"] = 1

                    if DEBUG_STATE and CALC_LOG:
                        self.log(
                            f"{name}: {LOGTXT['raw_state']} soc={soc}%, bp={bp}W, ac={ac}W, ctrl={ctrl}, mode_reg={mode}, "
                            f"last_dist={self.last_distribution.get(name, 0)}"
                        )


                except Exception as e:
                    self.error(f"[{name}] Modbus-Lesefehler: {e}")
                    states[name]["err"] = 1
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass



            
            # 3) Grid-Follow Basiswerte
            grid_flow = float(consumption or 0.0)   # + = Bezug, - = Einspeisung (NUR Smartmeter)
            
            # Aus AC je Batterie: bei dir ist AC<0 = Entladen, AC>0 = Laden
            bat_discharge_now = sum(max(0, -int(st.get("ac", 0))) for st in states.values())  # W
            bat_charge_now    = sum(max(0,  int(st.get("ac", 0))) for st in states.values())  # W
            
            # fuer bestehende Logs/Kompatibilitaet: "actual_flow" als Netzfluss behalten
            actual_flow = grid_flow



            # Zeitfilter fuer PV-Ueberschuss (Anti-PingPong bei Wolken)
            if grid_flow < PV_CHARGE_ALLOW_W:
                if self._pv_ok_since is None:
                    # sofortige Freigabe nach Restart (optional)
                    self._pv_ok_since = time.time() - PV_SURPLUS_MIN_TIME
            else:
                self._pv_ok_since = None
           
            
            # ---------------------------------------------------------------------
            # SOC-SAFETY (START/ACTIVE/DONE) - NACH pv_surplus, VOR BMS/Mode/Verteilung
            # ---------------------------------------------------------------------
            if SOC_SAFETY_ENABLE:
                # 1) START
                for n, st in states.items():
                    bms = self.bms_state.get(n)
                    if not bms:
                        continue
            
                    soc = int(st.get("soc", 0))
                    since = float(bms.get("min_soc_since") or 0)
            
                    if (
                        soc <= SOC_SAFETY_MIN_SOC
                        and since > 0
                        and not bms.get("safety_active")
                        and not bms.get("safety_done")
                        and (self._now_epoch - since) >= (SOC_SAFETY_AFTER_HOURS * 3600)
                        and (not SOC_SAFETY_REQUIRE_PV or self.pv_surplus)
                    ):
                        self.log(f"SOC-SAFETY START: {n} seit {SOC_SAFETY_AFTER_HOURS}h bei {soc}%")
                        bms["safety_active"] = True
                        self._save_bms_state()
            
                # 2) ACTIVE/DONE
                for n, st in states.items():
                    bms = self.bms_state.get(n)
                    if not bms or not bms.get("safety_active"):
                        continue
            
                    soc = int(st.get("soc", 0))
                    cfg = self.batteries[n]
            
                    if soc < SOC_SAFETY_TARGET_SOC:
                        client = _mb_client(cfg["host"], cfg["port"])
                        if client.connect():
                            try:
                                regs_ctrl = self._modbus_read(client, cfg["unit"], REGISTERS["control"], 1, n)
                                ctrl_val = regs_ctrl[0] if regs_ctrl else 0
                                ctrl_val, _ = self._ensure_control_enabled(client, cfg["unit"], ctrl_val, n)
            
                                self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 1, n)
                                time.sleep(0.05)
                                self._modbus_write(client, cfg["unit"], REGISTERS["charge_set"], SOC_SAFETY_CHARGE_W, n)
                            finally:
                                client.close()
                    else:
                        self.log(f"SOC-SAFETY DONE: {n} erreicht {soc}% (>= {SOC_SAFETY_TARGET_SOC}%)")
            
                        client = _mb_client(cfg["host"], cfg["port"])
                        if client.connect():
                            try:
                                self._modbus_write(client, cfg["unit"], REGISTERS["charge_set"], 0, n)
                                time.sleep(0.03)
                                self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 0, n)
                            finally:
                                client.close()
            
                        bms["safety_active"] = False
                        bms["safety_done"] = True
                        bms["min_soc_since"] = 0   # re-arm fuer naechstes Mal
                        self._save_bms_state()






            pv_surplus = (
                self._pv_ok_since is not None
                and (time.time() - self._pv_ok_since) >= PV_SURPLUS_MIN_TIME
            )

            self.pv_surplus = bool(pv_surplus)

            now_ts = time.time()
            pending_s = None
            if self._pv_ok_since is not None:
                pending_s = int(max(0, PV_SURPLUS_MIN_TIME - (now_ts - self._pv_ok_since)))

            if CALC_LOG:
                if pv_surplus:
                    self.log(
                        f"PV-SURPLUS=JA (Netzfluss={actual_flow:.1f}W, stable>={PV_SURPLUS_MIN_TIME}s)"
                    )
                else:
                    if self._pv_ok_since is not None and pending_s is not None and pending_s > 0:
                        # Wir sind zwar unter PV_CHARGE_ALLOW_W, aber noch nicht lange genug
                        self.log(
                            f"PV-SURPLUS_PENDING: warte noch {pending_s}s bis Freigabe "
                            f"(Netzfluss={actual_flow:.1f}W, Schwellwert<{PV_CHARGE_ALLOW_W}W)"
                        )
                    else:
                        # Kein Ueberschuss oder wieder weg
                        self.log(
                            f"PV-SURPLUS=NEIN (Netzfluss={actual_flow:.1f}W, Schwellwert<{PV_CHARGE_ALLOW_W}W)"
                        )


            # 3a) FULL waehrend aktivem BMS-Zyklus -> Zyklus beenden
            if ENABLE_BMS_CARE:
                for n, st in states.items():
                    bms = self.bms_state.get(n)
                    if not bms:
                        continue
                    if bms.get("phase") in ("charging", "hold") and int(st.get("soc", 0)) >= FULL_CHARGE_SOC:
                        self._bms_mark_cycle_done(n, self._now_epoch, "soc reached during bms cycle")


            # 4) BMS CARE Vorrang (WEICH: kein return, andere Batterien laufen weiter)
            bms_name = self._bms_select_candidate(states)
            if bms_name:
                skipped = self._bms_apply(bms_name, states, verify_logs, write_logs)
            
                if skipped and CALC_LOG:
                    calc_logs.append(
                        f"BMS: aktiv fuer {bms_name} (Netzfluss={actual_flow:.1f}W) -> weicher BMS-Modus (andere Batterien weiterhin aktiv)"
                    )

            # 4b) BMS Timeline Sensoren in HA aktualisieren (immer)
            if ENABLE_BMS_CARE:
                for n in self.batteries.keys():
                    bms = self.bms_state.get(n, {})
                    try:
                        self.set_state(
                            f"sensor.bms_{n}_phase",
                            state=bms.get("phase", "idle"),
                            attributes={
                                "last_full": self._fmt_ts(bms.get("last_full_ts")),
                                "hold_until": self._fmt_ts(bms.get("holding_until")),
                                "cooldown_until": self._fmt_ts(bms.get("cooldown_until")),
                            }
                        )
                    except Exception as e:
                        self.error(f"BMS-HA-SENSOR {n}: set_state fehlgeschlagen: {e}")



            # 5) Mode-Wechsel (stabilisiert)

            # Zeitbasis fuer Modus-Stabilisierung (monotonic, restart-sicher)
            now = time.monotonic()
            
            # vorheriger Modus (restart-sicher)
            prev = self.mode_state or "stop"
                        

            # ---------------------------------------------------------
            # MODE-ENTSCHEIDUNG (Grid-Follow, STOP nur bei Ziel == 0)
            # ---------------------------------------------------------
            
            soc_safety_active_any = any(
                b.get("safety_active") for b in self.bms_state.values()
            )
            
            # Default: bleibe im bisherigen Modus
            new_mode = prev or "stop"
            
            # CHARGE nur bei echter Einspeisung
            if grid_flow < -TOLERANCE_W:
                new_mode = "charge"
            
            # DISCHARGE, sobald Last da ist ODER Batterie schon laeuft
            elif grid_flow > TOLERANCE_W or bat_discharge_now > 0:
                new_mode = "discharge"
            
            # STOP NUR, wenn wirklich nichts mehr zu tun ist
            else:
                # Grid ~ 0 UND Batterie liefert nichts UND kein Zwang aktiv
                if (
                    bat_discharge_now == 0
                    and not soc_safety_active_any
                ):
                    new_mode = "stop"
                else:
                    # dynamisch weiterregeln
                    new_mode = prev or "discharge"

            now = time.monotonic()
            stable_for = now - self.mode_since
            effective_mode = new_mode
            
            # -------------------------------------------------------------------------
            # BMS active -> states_no_bms -> Log -> Verteilung  (EINMALIG, ohne Duplikate)
            # -------------------------------------------------------------------------
            
            # 1) BMS aktive Batterie(n) erfassen
            bms_active = set()
            bms_charging = set()
            if ENABLE_BMS_CARE:
                for _n, _b in self.bms_state.items():
                    ph = _b.get("phase")
                    if ph in ("charging", "hold"):
                        bms_active.add(_n)
                    if ph == "charging":
                        bms_charging.add(_n)
            
            # 2) Batterien fuer normale Verteilung (ohne BMS-Batterien)
            states_no_bms = {n: st for n, st in states.items() if n not in bms_active}
            
            # 3) Modus-Stabilisierung (Ping-Pong-Schutz)
            if new_mode != prev:
                if stable_for >= STABLE_TIMER_S:
                    self.mode_state = new_mode
                    self.mode_since = now
                    if CALC_LOG:
                        calc_logs.append(
                            f"Moduswechsel: {MODE_DE.get(prev or 'stop','stopp')} -> {MODE_DE.get(new_mode,'stopp')}"
                        )
                else:
                    effective_mode = prev or "stop"
            else:
                self.mode_since = now
            
            # 4) Non-BMS laden nur bei PV-Ueberschuss (BMS darf trotzdem laufen)
            if effective_mode == "charge" and not self.pv_surplus:
                if CALC_LOG:
                    calc_logs.append("Kein PV-Ueberschuss -> non-BMS charge deaktiviert (BMS ggf. aktiv)")
            
            # 5) Ein einziges Status-Log (nicht doppelt)
            if CALC_LOG:
                calc_logs.append(
                    f"Netzfluss = {actual_flow:.1f} W | "
                    f"Modus: {MODE_DE.get(prev or 'stop')} -> {MODE_DE.get(new_mode)} | "
                    f"stabil seit {int(stable_for)} s | "
                    f"wirksam: {MODE_DE.get(effective_mode)}"
                )
            
            # -------------------------
            # Verteilung (WEICH – Variante B)
            # -------------------------
            distribution = {}
            
            # ---------- DISCHARGE ----------
            if effective_mode == "discharge":
                target_discharge_total = max(0, int(round(bat_discharge_now + grid_flow)))
                if CALC_LOG:
                    calc_logs.append(f"GRID-FOLLOW DISCHARGE: grid={grid_flow:.1f}W bat_now={bat_discharge_now:.1f}W -> target={target_discharge_total}W")
                
                distribution = self._distribute_waterfill(
                    target_discharge_total, states_no_bms, "discharge", calc_logs
)
            
                if CALC_LOG:
                    if not distribution:
                        calc_logs.append(
                            "Zielverteilung-entladen: LEER (siehe Gruende oben)"
                        )
                    else:
                        calc_logs.append(
                            f"Zielverteilung-entladen: {distribution}"
                        )
            
                        
            # ---------- CHARGE (PV-REST!) ----------
            elif effective_mode == "charge":
                target_charge_total = max(0, int(round(bat_charge_now + (-grid_flow))))
                if CALC_LOG:
                    calc_logs.append(f"GRID-FOLLOW CHARGE: grid={grid_flow:.1f}W bat_now={bat_charge_now:.1f}W -> target={target_charge_total}W")
                
                # optional: wenn du CHARGE trotzdem nur bei PV-Surplus erlauben willst:
                if self.pv_surplus:
                    distribution = self._distribute_waterfill(target_charge_total, states_no_bms, "charge", calc_logs)
                else:
                    distribution = {}

            
                if CALC_LOG and (not self.pv_surplus):
                    calc_logs.append("CHARGE-Mode aber pv_surplus=NEIN -> distribution leer (nur BMS kann laden)")
                if CALC_LOG and target_charge_total <= 0 and bms_charging:
                    calc_logs.append(
                        "CHARGE target=0: kein Rest fuer non-BMS (BMS ggf. aktiv / keine Einspeisung)."
                    )
                            
            # ---------- STOP ----------
            else:
                distribution = {}

            # fuer Tabelle im finally
            self._last_distribution_debug = dict(distribution or {})
            self._last_effective_mode = effective_mode
            self._last_pv_surplus = bool(self.pv_surplus)


            # 7) Schreiben
            for name, st in states.items():
            
                # BMS-Batterie niemals in der normalen Schreibphase anfassen
                if name in bms_active:
                    calc_logs.append(f"{name}: Regelung uebersprungen - BMS-Ladung aktiv")
                    continue
            
                # SOC-SAFETY Batterie niemals ueberschreiben
                bms = self.bms_state.get(name, {})
                if bms.get("safety_active"):
                    if CALC_LOG:
                        calc_logs.append(f"{name}: Regelung uebersprungen - SOC-SAFETY aktiv")
                    continue
            
                cfg = st["cfg"]
             

                mn = int(cfg["min_soc"])
                mx = int(cfg["max_soc"])
                old = int(self.last_distribution.get(name, 0))
            
                # Wenn BMS aktiv -> non-BMS immer STOP
                desired = int(distribution.get(name, 0))
            
                if effective_mode == "discharge" and self._bms_battery_block_discharge(name):
                    desired = 0
                    if CALC_LOG:
                        calc_logs.append(
                            f"{name}: discharge block wegen BMS hold/cooldown -> desired=0"
                        )
            
                client = _mb_client(cfg["host"], cfg["port"])
                if not client.connect():
                    self.error(f"[{name}] Verbindung fehlgeschlagen (Schreibphase)")
                    st["err"] = 1
                    continue
            
                try:
                    ctrl_val = int(st.get("ctrl", 0))
                    ctrl_val, changed = self._ensure_control_enabled(
                        client, cfg["unit"], ctrl_val, name
                    )
                    if changed:
                        write_logs.append(f"{name}: control freigeschaltet ({ctrl_val})")
                        st["ctrl"] = ctrl_val
                        time.sleep(0.05)
                                
                    # ---------- STOP ----------
                    if effective_mode == "stop":
                        need_stop = (old != 0) or (int(st.get("mode", 0)) != 0) or (abs(int(st.get("bp", 0))) > TOLERANCE_W)
                        if need_stop:
                            self._modbus_write(client, cfg["unit"], REGISTERS["charge_set"], 0, name)
                            time.sleep(0.03)
                            self._modbus_write(client, cfg["unit"], REGISTERS["discharge_set"], 0, name)
                            time.sleep(0.03)
                            self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 0, name)
                            write_logs.append(f"{name}: STOP")
                            self.last_distribution[name] = 0
                        continue


                    # ---------- CHARGE ----------
                    if effective_mode == "charge":
                        limit = min(desired, int(cfg["max_charge_w"]))
                        if int(st.get("soc", 0)) >= mx:
                            limit = 0

                        need_write = abs(limit - old) > TOLERANCE_W

                        # Wenn limit=0: nur schreiben, wenn wirklich etwas zu stoppen ist
                        if limit == 0:
                            need_write = (old != 0) or (int(st.get("mode", 0)) != 0) or (abs(int(st.get("bp", 0))) > TOLERANCE_W)

                        if need_write:
                            self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 1 if limit > 0 else 0, name)
                            time.sleep(0.03)
                            self._modbus_write(client, cfg["unit"], REGISTERS["charge_set"], limit, name)
                            write_logs.append(f"{name}: laden {old}->{limit}")
                            self.last_distribution[name] = limit

                            self._verify_setpoint_execution(client, cfg, name, "charge", limit, verify_logs)

                    
                    # ---------- DISCHARGE ----------
                    elif effective_mode == "discharge":
                        limit = min(desired, int(cfg["max_discharge_w"]))
                        if int(st.get("soc", 0)) <= mn:
                            limit = 0
                                            
                        self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 2 if limit > 0 else 0, name)
                        time.sleep(0.03)
                        self._modbus_write(client, cfg["unit"], REGISTERS["discharge_set"], limit, name)
                        write_logs.append(f"{name}: entladen {old}->{limit}")
                        self.last_distribution[name] = limit
                        
                        # >>> NEW: Setpoint-Verifikation
                        self._verify_setpoint_execution(client, cfg, name, "discharge", limit, verify_logs)


                except Exception as e:
                    self.error(f"[{name}] Modbus-Schreibfehler: {e}")
                    st["err"] = 1
            
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass


            self._log_bms_schedule()
            self._log_cycle_times("END")

        finally:
            try:
                # Tabellenkopf
                self.log("=== PVCONTROL TABLE BEGIN ===")
                self.log(self.header_line)
                self.log(self.sep_line)

                # Tabellenzeilen
                for name in self.batteries.keys():
                    st = states.get(name, {})
                    cfg = st.get("cfg", self.batteries.get(name, {}))

                    ctrl_val = int(st.get("ctrl", 0))
                    ctrl_txt = "En" if ctrl_val in CONTROL_ENABLED_VALUES else "Un"
                    err_txt = ("ERR" if st.get("err") else "OK").ljust(COL_WIDTH["err"])

                    last_val = int(self.last_distribution.get(name, 0))
                    soll_val = int(getattr(self, "_last_distribution_debug", {}).get(name, 0))
                    bms_obj = self.bms_state.get(name, {})
                    bms_phase = bms_obj.get("phase", "idle")
                    
                    if bms_obj.get("safety_active"):
                        bms_txt = f"soc{SOC_SAFETY_MIN_SOC}->{SOC_SAFETY_TARGET_SOC}"
                    else:
                        bms_txt = LOGTXT.get(bms_phase, bms_phase)
                    pv_txt = "JA" if bool(getattr(self, "_last_pv_surplus", False)) else "NEI"

                    mode_reg = int(st.get("mode", 0))
                    if mode_reg == 1:
                        mode_txt = "CHG"
                    elif mode_reg == 2:
                        mode_txt = "DIS"
                    else:
                        mode_txt = "STP"

                    bp_val = int(st.get("bp", 0))
                    if bms_obj.get("safety_active"):
                        zustand_txt = f"Laden - SOC-SAFETY"
                    elif bms_phase in ("charging", "hold"):
                        zustand_txt = "Laden - BMS aktiv"
                    else:
                        if abs(bp_val) <= TOLERANCE_W:
                            zustand_txt = "Leerlauf"
                        elif bp_val > 0:
                            zustand_txt = "Laden"
                        else:
                            zustand_txt = "Entladen"
                            
                    bms = self.bms_state.get(name, {})
                    since = float(bms.get("min_soc_since") or 0)
                    
                    if since > 0:
                        soc11_txt = self._fmt_dh(int(self._now_epoch - since))
                    else:
                        soc11_txt = "-"


                    vals = [
                        name[-1].ljust(COL_WIDTH["bat"]),
                        f"{int(st.get('soc', 0))}%".ljust(COL_WIDTH["soc"]),
                        str(bp_val).ljust(COL_WIDTH["bp"]),
                        str(int(st.get("ac", 0))).ljust(COL_WIDTH["ac"]),
                        zustand_txt.ljust(COL_WIDTH["zustand"]),
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
                   
                # Klassische Logs – GENAU 1x
                if CALC_LOG:
                    self.log(
                        f"Zaehler:{consumption}W AC_sum:{sum_ac}W Netz:{actual_flow}W"
                    )
                    for m in calc_logs:
                        self.log(m)
                    for m in write_logs:
                        self.log(m)
                    for m in verify_logs:
                        self.log(m)
                self.log("=== PVCONTROL TABLE END ===")
            except Exception as e:
                self.error(f"Tabellen-Logging fehlgeschlagen: {e}")
                
            # ganz zum Schluss
        self._busy = False
