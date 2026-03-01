# -----------------------------------------------------------------------------
# Marstek Venus – Home Assistant / AppDaemon Skript
#
# Beschreibung:
# Dieses AppDaemon-Skript dient zur intelligenten Steuerung von
# Marstek-Venus-Batteriesystemen in Home Assistant ueber Modbus.
#
# Der Fokus liegt auf einer stabilen, batterieschonenden Lade- und
# Entladelogik mit klar definierten SoC-Grenzen, einem Notfall-Lademodus
# sowie nachvollziehbarem Logging fuer den produktiven Einsatz.
#
# Funktionen:
# - Dynamische Lade- und Entladesteuerung
# - SoC-Grenzen (min / max)
# - Notfall-Lademodus bei kritischem Ladezustand
# - Leistungsbegrenzung fuer Laden und Entladen
# - Modbus-Kommunikation (TCP / RTU)
# - Optionale MQTT-Anbindung
# - Debug- und Detail-Logging
#
# Voraussetzungen:
# - Home Assistant
# - AppDaemon (laufend)
# - pymodbus
# - paho-mqtt (optional)
#
# Autor:
# Neue Medien Becker
#
# Lizenz:
# MIT License
# -----------------------------------------------------------------------------

"""
pv_control.py

Dynamische Lade-, Entlade- und Stopp-Steuerung fuer Marstek-Venus-Batteriesysteme.

Technische Merkmale:
- ASCII-only (keine Umlaute)
- Restart-sicheres Verhalten
- Klare Trennung von Steuerlogik, Schutzmechanismen und Logging
- Vollstaendig eigenstaendig lauffaehig innerhalb von AppDaemon

BMS / Battery Care:
- Betrieb auch ohne PV-Ueberschuss moeglich
- Zyklusabbruch, wenn FULL_CHARGE_SOC bereits erreicht wurde
- Begrenzung der maximalen Ladezyklen pro Tag
- Haltephase nach Voll-Ladung (Hold)
- Nachgelagerte Cooldown-Phase ohne Entladung

Design-Entscheidungen:
- Das Skript ist bewusst monolithisch aufgebaut
- Keine externen Abhaengigkeiten zu apps.yaml notwendig
- Persistente Statusdaten werden in einer JSON-Datei gespeichert
- Pfade sind Add-on-sicher und restart-fest definiert

Stabilitaet:
- Getestet unter Home Assistant mit AppDaemon
- Keine fehlenden Attribute oder internen Methoden
- Klare TX/RX-Fehlererkennung
- Verifikation gesetzter Sollwerte (Setpoints)

Hinweis:
Dieses Skript greift aktiv in das Energiemanagement ein und sollte vor
dem produktiven Einsatz sorgfaeltig konfiguriert und getestet werden.
"""


import os
import json
import struct
import time
import random
from datetime import datetime
from appdaemon.plugins.hass.hassapi import Hass
from pymodbus.client import ModbusTcpClient
from pv_control_config import *  # noqa: F403,F401



# --- KONFIGURATION ----------------------------------------------------------
# -----------------------------------------------------------------------------
# HA-SENSOR EXPORT (alle Werte werden als sensor.* nach HA geschrieben)
# -----------------------------------------------------------------------------
HA_EXPORT_ENABLE = True
HA_EXPORT_PREFIX = "marstekvenus"   # wird pro battery erweitert, z.B. marstekvenus_1_*
HA_EXPORT_KEYS_PER_CYCLE = 4   # pro Batterie nur 4 Werte je Zyklus exportieren

# Pro Batterie nur ein Index (1/2/3) fuer Namen/Prefix.
# Kein kleinteiliges YAML pro Batterie mehr.
BATTERY_META = {
    "battery1": {"idx": 1},
    "battery2": {"idx": 2},
    "battery3": {"idx": 3},
}

# Register-Definitionen: 1x zentral, gilt fuer alle Batterien gleich
# type: u16, s16, u32, s32
# scale/offset/precision wie in YAML

HA_REGMAP = {
    "battery_voltage": {"addr": 32100, "cnt": 1, "type": "u16", "scale": 0.01, "unit": "V", "prec": 2, "name": "Battery Voltage"},
    "battery_soc": {"addr": 32104, "cnt": 1, "type": "u16", "scale": 1.0, "unit": "%", "prec": 0, "name": "Battery SoC"},
    "battery_energy": {"addr": 32105, "cnt": 1, "type": "u16", "scale": 0.0001, "unit": "kWh", "prec": 4, "name": "Battery Energy"},

    "ac_power": {"addr": 32202, "cnt": 2, "type": "s32", "scale": 1.0, "unit": "W", "prec": 0, "name": "AC Power"},
    "ac_offgrid_power": {"addr": 32302, "cnt": 2, "type": "s32", "scale": 1.0, "unit": "W", "prec": 0, "name": "AC Offgrid Power"},

    "total_charging_energy": {"addr": 33000, "cnt": 2, "type": "u32", "scale": 0.01, "unit": "kWh", "prec": 4, "name": "Total Charging Energy"},
    "total_discharging_energy": {"addr": 33002, "cnt": 2, "type": "u32", "scale": 0.01, "unit": "kWh", "prec": 4, "name": "Total Discharging Energy"},

    "temp_internal": {"addr": 35000, "cnt": 1, "type": "s16", "scale": 0.1, "unit": "C", "prec": 1, "name": "Temp Internal"},
    "temp_mos1": {"addr": 35001, "cnt": 1, "type": "s16", "scale": 0.1, "unit": "C", "prec": 1, "name": "Temp MOS1"},
    "temp_mos2": {"addr": 35002, "cnt": 1, "type": "s16", "scale": 0.1, "unit": "C", "prec": 1, "name": "Temp MOS2"},
    "temp_cell_max": {"addr": 35010, "cnt": 1, "type": "s16", "scale": 0.1, "unit": "C", "prec": 1, "name": "Temp Cell Max"},
    "temp_cell_min": {"addr": 35011, "cnt": 1, "type": "s16", "scale": 0.1, "unit": "C", "prec": 1, "name": "Temp Cell Min"},

    "inverter_state": {"addr": 35100, "cnt": 1, "type": "u16", "scale": 1.0, "unit": "", "prec": 0, "name": "Inverter State"},
    "chg_v_limit": {"addr": 35110, "cnt": 1, "type": "u16", "scale": 0.1, "unit": "V", "prec": 1, "name": "Charge V Limit"},

    "rs485_control": {"addr": 42000, "cnt": 1, "type": "u16", "scale": 1.0, "unit": "", "prec": 0, "name": "RS485 Control"},
    "charge_to_soc": {"addr": 42011, "cnt": 1, "type": "u16", "scale": 1.0, "unit": "%", "prec": 0, "name": "Charge To SoC"},
    "user_work_mode": {"addr": 43000, "cnt": 1, "type": "u16", "scale": 1.0, "unit": "", "prec": 0, "name": "User Work Mode"},
}

HA_ENTITY_ID_MAP = {
    # total_discharging_energy soll IMMER auf _2 gehen (wie in deiner Entity Registry)
    (1, "total_discharging_energy"): "sensor.marstekvenus_1_total_discharging_energy",
    (2, "total_discharging_energy"): "sensor.marstekvenus_2_total_discharging_energy_2",
    (3, "total_discharging_energy"): "sensor.marstekvenus_3_total_discharging_energy_2",

    # falls charging auch _2 sein soll, dann hier ebenfalls:
    (1, "total_charging_energy"): "sensor.marstekvenus_1_total_charging_energy_2",
    (2, "total_charging_energy"): "sensor.marstekvenus_2_total_charging_energy_2",
    (3, "total_charging_energy"): "sensor.marstekvenus_3_total_charging_energy_2",
}

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


# --- SIGN / INTERPRETATION --------------------------------------------------
# In deinen Logs wirkt es so:
#   AC < 0 = DISCHARGE, AC > 0 = CHARGE
# Wenn du aber intern "AC<0=CHARGE" erzwingen willst, dann invertiere hier.
AC_SIGN_INVERT = False  # <-- aktuell: False passt zu deinen Logs

# --- DIAG LOGGING -----------------------------------------------------------
LOG_DIAG_SIGN = True    # Loggt raw + normalized und leitet Laden/Entladen ab
LOG_DIAG_GRID = True    # Loggt grid_flow, ac_ctrl_now, A_set und Mode-Entscheidung

# --- MODBUS LOGGING ----------------------------------------------------------
MODBUS_LOG_OK              = False   # Modbus: erfolgreiche TX/RX (Senden/Empfangen) zusaetzlich anzeigen
MODBUS_LOG_TXRX            = False   # Modbus: jedes TX (Read/Write) als Debug ausgeben (sehr viel Log)
MODBUS_LOG_ERROR           = False    # Modbus: Fehler immer loggen (empfohlen: True)
MODBUS_DEBUG               = False    # Modbus: Detail-Logs Requests/Responses (nur einschalten wenn noetig)

CALC_LOG                   = True    # Berechnung: Details zu Moduswahl, Verteilung und Delta (Regel-Logik)
LOG_DISCHARGE              = True    # Entladen: Logs zu Discharge-Setpoints (wer bekommt wieviel Watt)

DEBUG_STATE                = True    # Debug: Rohdaten/Interpretation pro Batterie (SoC, AC, BP, Mode, Control)
DEBUG_WEIGHTS              = True    # Debug: Details, welche Batterie warum teilnimmt (Gewichte/Filter)
DEBUG_SETPOINTS            = True    # Debug: Vergleich last_distribution vs. desired/limits (Setpoint-Entscheidung)


# --- BASIS-PARAMETER (Zyklus / Stabilitaet) ---------------------------------
TOLERANCE_W                = 40      # Toleranz um 0W: innerhalb +/-40W keine aggressiven Aenderungen (Anti-Zappeln)
INTERVAL                   = 20      # Zykluszeit in Sekunden: wie oft gelesen/geregelt wird
STABLE_TIMER_S             = 20      # Mindestzeit stabil, bevor Moduswechsel erlaubt ist (Ping-Pong-Schutz)


# --- MODBUS CLIENT PARAMETER -------------------------------------------------
MB_TIMEOUT_S               = 3     # Modbus Timeout in Sekunden (wie lange auf Antwort gewartet wird)
MB_RETRIES                 = 1       # Modbus Wiederholungen bei Fehler (0..2 sinnvoll, zu hoch macht Last)


# --- SETPOINT VERIFIKATION (Pruefen ob Geraet reagiert) ----------------------
VERIFY_SETPOINT_EXEC       = False    # Verifikation aktiv: nach Setpoint pruefen ob Leistung sichtbar ist
VERIFY_MIN_SET_W           = 50      # Verifikation erst ab dieser Leistung (unter 50W ist Messung ungenau)
VERIFY_TOL_W               = 20      # Verifikations-Toleranz: +/-20W gelten noch als "ok" (Messrauschen)


# --- SOC-SAFETY (Anti-Totlaufen unter Min-SoC) -------------------------------
SOC_SAFETY_ENABLE          = True    # SoC-Schutz aktiv: verhindert tiefes Entladen unter Minimal-SoC
SOC_SAFETY_MIN_SOC         = 11      # Schutz aktiv bei SoC <= 11% (darunter wird Entladen verhindert)
SOC_SAFETY_TARGET_SOC      = 12      # Schutz-Ziel: bis SoC 12% sanft nachladen, dann wieder normal
SOC_SAFETY_AFTER_HOURS     = 48      # Erst reagieren, wenn SoC lange niedrig ist (z.B. 48 Stunden)
SOC_SAFETY_CHARGE_W        = 400     # Schutz-Ladeleistung (W): absichtlich klein und batterieschonend

SOC_SAFETY_LOG_ENABLE      = True    # SoC-Schutz Logging global ein/aus
SOC_SAFETY_LOG_EVERY_CYCLE = False   # Wenn True: jeden Zyklus loggen (sonst nur Ereignisse)


# --- LOG RETENTION (Logfiles aufraeumen) -------------------------------------
LOG_RETENTION_DAYS         = 365     # Logs behalten: nach X Tagen loeschen (z.B. 30/90/365)
LOG_CLEANUP_PATH           = "/config/appdaemon/logs"   # Pfad der AppDaemon Logs
LOG_CLEANUP_GLOB           = ".log"  # Nur Dateien mit dieser Endung anfassen (z.B. ".log")
LOG_CLEANUP_RUN_EVERY_H    = 3650    # Aufraeumen alle X Stunden (3650h ~ ca. 5 Monate)


# --- PV SURPLUS ZEITFILTER (Anti-Wolken-PingPong) ----------------------------
PV_SURPLUS_MIN_TIME        = 20      # PV-Ueberschuss muss X Sekunden stabil sein, bevor geladen wird
SOC_SAFETY_REQUIRE_PV      = False   # Wenn True: Schutzladung (SOC-SAFETY) nur bei PV-Ueberschuss


# --- GRID-FOLLOW (Stop/Deadband Stabilisierung) ------------------------------
GRID_DEADBAND_W            = 60      # Deadband um 0W Netzfluss: innerhalb +/-60W keine Nachregelung (Ruhezone)
GRID_STOP_AFTER_S          = 120     # Stop nach X Sekunden ohne Wirkung: wenn Setpoints nichts bewirken -> auf 0


# --- PV CHARGE DISTRIBUTION (Bucket-Logik fuer kleine PV-Leistungen) ---------
PV_BUCKET_ENABLE           = True    # Bucket-Logik aktiv: bei wenig PV nicht alle Batterien gleichzeitig laden
PV_BUCKET_1_TOTAL_W        = 300     # Unter 300W Gesamt-Target: nur 1 Batterie aktiv laden
PV_BUCKET_2_TOTAL_W        = 900     # Unter 900W Gesamt-Target: max. 2 Batterien aktiv laden (sonst alle)
PV_MIN_PER_BAT_W           = 150     # Mindestleistung pro aktiver Batterie, damit Marstek "anspringt"

PV_ROTATE_ENABLE           = True    # Rotation aktiv: bei Bucket 1/2 wechselnde Batterie(n), gleichmaessige Nutzung
PV_ROTATE_MODE             = "cycle" # Rotation: "cycle" = pro Zyklus rotieren, "daily" = einmal pro Tag


# --- BMS / BATTERY CARE (Voll-Ladung fuer Balancing / Pflege) ----------------
ENABLE_BMS_CARE            = True    # BMS-Pflege aktiv: gelegentliche Voll-Ladung fuer Balancing
PV_CHARGE_ALLOW_W          = -100    # PV-Ueberschuss-Schwelle: Netzfluss <= -100W gilt als "PV vorhanden"
FULL_CHARGE_SOC            = 99      # Voll-Lade-Ziel: bis SoC 99% (Balancing)
FULL_CHARGE_INTERVAL_DAYS  = 10      # Alle X Tage pro Batterie eine Voll-Ladung (Empfehlung 7..14)
FULL_CHARGE_MIN_HOLD_S     = 45*60   # Nach Voll-Ladung X Sekunden halten (z.B. 45 Minuten)
ALLOW_DISCHARGE_AFTER_S    = 30*60   # Nach Voll-Ladung X Sekunden nicht entladen (Cooldown/Schonung)

MAX_BMS_BATTERIES_PER_DAY  = 1       # Pro Tag maximal X Batterien fuer BMS-Pflege voll laden
BMS_FORCE_CHARGE_W         = 1200    # Pflege-Ladeleistung (W): unter max_charge, sanft

BMS_STATE_FILE             = "/config/appdaemon/data/pv_control_bms_state.json"   # Persistenter BMS-Status (JSON)


# --- CONTROL / ENABLE STATES -------------------------------------------------
CONTROL_ENABLE_CANDIDATES  = [21930, 21931]   # Werte, die "Steuerung aktiv" bedeuten koennen (je nach Firmware)
CONTROL_ENABLED_VALUES     = set(CONTROL_ENABLE_CANDIDATES)   # Set fuer schnellen Check: ist Control aktiv?
CONTROL_DISABLED_VALUES    = {21947, 0}       # Werte, die "Steuerung aus" bedeuten (Setpoints werden ignoriert)


# --- SMARTMETER (Home Assistant Entity) --------------------------------------
POWER_SENSOR = {
    "enabled": True,                                              # Smartmeter aktiv: Netzleistung aus HA lesen
    "entity":  "sensor.stromzaehler_sml_aktuelle_wirkleistung"     # Entity: aktuelle Wirkleistung (W)
}


# Modbus-Register (Adressen im Marstek/Venus Modbus-Registerplan; werden per read_holding_registers gelesen)

REGISTERS = {
    "soc":            32104,     							 						# SoC (State of Charge) in %: Ladezustand der Batterie, z.B. 55 = 55%.
    "battery_power":  (32102, 2), 													# Batterie-Leistung als signed 32-bit (2 Register): + = Batterie wird geladen, - = Batterie entlaedt.
    "control":        42000,      													# Freigabe/Steuerstatus (Control-Word): muss auf "enabled" stehen, sonst ignoriert das Geraet Setpoints.
    "mode":           42010,      													# Betriebsmodus: 0=STOP (keine Regelung), 1=CHARGE (Laden), 2=DISCHARGE (Entladen).
    "charge_set":     42020,      													# Lade-Sollwert in Watt: wie stark das Geraet laden soll (nur wirksam bei mode=1).
    "discharge_set":  42021,      													# Entlade-Sollwert in Watt: wie stark das Geraet entladen soll (nur wirksam bei mode=2).
    "ac_power":       (32202, 2), 													# AC-Leistung als signed 32-bit (2 Register): Leistung am AC-Port; je nach Geraet positiv/negativ (wird im Script normalisiert).
}

# Batterie-Konfiguration (pro Batterie ein Block; mehrere Systeme koennen parallel geregelt werden)
BATTERY_CONFIG = {
    "battery1": {
        "enabled":          True,            										# Batterie im Script aktivieren/deaktivieren (False = komplett ignorieren).
        "modbus":           True,            										# Modbus-Nutzung aktiv (False = Batterie wird nicht per Modbus gelesen/geschrieben).
        "host":             "192.168.100.201",										# IP-Adresse des Modbus-TCP Gateways/der Batterie im Netzwerk.
        "port":             502,             										# TCP-Port fuer Modbus (Standard: 502).
        "unit":             1,               										# Modbus Unit-ID (Slave-ID); bei TCP meist 1, kann je nach Geraet abweichen.
        "capacity_kwh":     5.6,             										# Kapazitaet in kWh; wird fuer die Leistungsverteilung (Gewichtung) genutzt.
        "max_charge_w":     2500,            										# Max. Ladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "max_discharge_w":  2500,            										# Max. Entladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "min_soc":          11,              										# Untere SoC-Grenze in %: darunter/gleich wird Entladen gestoppt (Batterieschutz).
        "max_soc":          99               										# Obere SoC-Grenze in %: ab hier wird Laden gestoppt (Batterieschutz/BMS-Logik).
    },
    "battery2": {
        "enabled":          True,            										# Batterie im Script aktivieren/deaktivieren (False = komplett ignorieren).
        "modbus":           True,            										# Modbus-Nutzung aktiv (False = Batterie wird nicht per Modbus gelesen/geschrieben).
        "host":             "192.168.100.202",										# IP-Adresse des Modbus-TCP Gateways/der Batterie im Netzwerk.
        "port":             502,             										# TCP-Port fuer Modbus (Standard: 502).
        "unit":             1,               										# Modbus Unit-ID (Slave-ID); bei TCP meist 1, kann je nach Geraet abweichen.
        "capacity_kwh":     5.6,             										# Kapazitaet in kWh; wird fuer die Leistungsverteilung (Gewichtung) genutzt.
        "max_charge_w":     2500,            										# Max. Ladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "max_discharge_w":  2500,            										# Max. Entladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "min_soc":          11,              										# Untere SoC-Grenze in %: darunter/gleich wird Entladen gestoppt (Batterieschutz).
        "max_soc":          99               										# Obere SoC-Grenze in %: ab hier wird Laden gestoppt (Batterieschutz/BMS-Logik).
    },
    "battery3": {
        "enabled":          True,            										# Batterie im Script aktivieren/deaktivieren (False = komplett ignorieren).
        "modbus":           True,            										# Modbus-Nutzung aktiv (False = Batterie wird nicht per Modbus gelesen/geschrieben).
        "host":             "192.168.100.203",										# IP-Adresse des Modbus-TCP Gateways/der Batterie im Netzwerk.
        "port":             502,             										# TCP-Port fuer Modbus (Standard: 502).
        "unit":             1,               										# Modbus Unit-ID (Slave-ID); bei TCP meist 1, kann je nach Geraet abweichen.
        "capacity_kwh":     5.6,             										# Kapazitaet in kWh; wird fuer die Leistungsverteilung (Gewichtung) genutzt.
        "max_charge_w":     2500,            										# Max. Ladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "max_discharge_w":  2500,            										# Max. Entladeleistung in Watt; Script setzt niemals hoeher als diesen Wert.
        "min_soc":          11,              										# Untere SoC-Grenze in %: darunter/gleich wird Entladen gestoppt (Batterieschutz).
        "max_soc":          99               										# Obere SoC-Grenze in %: ab hier wird Laden gestoppt (Batterieschutz/BMS-Logik).
    },
}


COLUMNS = [
    "bat", "soc",  "ac", "zustand",
    "ctrl", "mode",
    "last", "soll",
    "maxch", "maxdis", "minsoc", "maxsoc",
    "bms", "pv", "soc11", "err"
]


HEADERS = [
    "Bat", "SoC",  "AC(W)", "Zustand",
    "Ctrl", "Mode",
    "Last", "Soll",
    "MxCh", "MxDs", "MinSoC", "MaxSoC",
    "BMS", "PV", "SOC11_SEIT", "Err"
]

COL_WIDTH = {
    "bat": 5, "soc": 5,  "ac": 7,
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

def _u32_from_u16_be(words):
    if not words or len(words) != 2:
        return 0
    return struct.unpack(">I", struct.pack(">HH", words[0], words[1]))[0]

def _s16_from_u16(word):
    # uint16 -> int16
    return struct.unpack(">h", struct.pack(">H", int(word) & 0xFFFF))[0]

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
                exc = getattr(rr, "exception_code", None)
                fcode = getattr(rr, "function_code", None)
                log(f"{prefix}[RX FEHLER] isError={rr} exc={exc} fcode={fcode}")
            return None

        regs = getattr(rr, "registers", None)
        if regs is None:
            if MODBUS_LOG_ERROR:
                log(f"{prefix}[RX FEHLER] keine 'registers' im Response")
            return None

        if MODBUS_LOG_OK:
            log(f"{prefix}[RX OK] {regs}")

        time.sleep(0.03)  # etwas mehr Luft als 0.02
        return regs

    except Exception as e:
        if MODBUS_LOG_ERROR:
            log(f"{prefix}[RX FEHLER] Exception: {e}")
        return None


def _modbus_write_generic(client, unit, addr, value, log, name):
    prefix = f"[{name}] " if name else ""
    v = int(value)

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


# ----------------------------------------------------------------------------
# Kernklasse
# ----------------------------------------------------------------------------


class PVControlApp(Hass):

    # ------------------------- Init ----------------------------------------

    
    def _ha_entity(self, battery_name, key):
        idx = int(BATTERY_META.get(battery_name, {}).get("idx", 0) or 0)
    
        forced = HA_ENTITY_ID_MAP.get((idx, key))
        if forced:
            return forced
    
        return f"sensor.{HA_EXPORT_PREFIX}_{idx}_{key}" if idx else f"sensor.{HA_EXPORT_PREFIX}_{battery_name}_{key}"
    
    def _set_ha_sensor(self, entity_id, value, unit="", attrs=None):
        if not HA_EXPORT_ENABLE:
            return
    
        # HA akzeptiert keinen None/NaN/Inf als state
        if value is None:
            return
        try:
            if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
                return
        except Exception:
            return
    
        try:
            a = dict(attrs or {})
    
            # Einheit setzen
            if unit:
                a["unit_of_measurement"] = str(unit)
    
            # ---- ENERGY META fuer kWh ----
            # Damit Home Assistant es als Energie erkennt (Energy Dashboard / Statistics)
            if str(unit).lower() == "kwh":
                a["device_class"] = "energy"
                a["state_class"] = "total_increasing"
    
            # state IMMER als STRING, aber numerisch formatiert (kein "kWh" im state!)
            if isinstance(value, (int, float)):
                state = str(value)
            else:
                # Falls doch mal String reinkommt
                state = str(value).strip()
    
            self.set_state(entity_id, state=state, attributes=a)
    
        except Exception as e:
            self.error(f"HA-EXPORT: set_state failed for {entity_id}: {e}")

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
        if not HA_EXPORT_ENABLE:
            return
    
        # Backoff beruecksichtigen (damit Export nicht extra Last erzeugt)
        now_ts = float(self._now_epoch or time.time())
        if now_ts < float(self._mb_backoff_until.get(battery_name, 0) or 0):
            return
    
        client = _mb_client(cfg["host"], cfg["port"])
        if not client.connect():
            self._mb_backoff_until[battery_name] = time.time() + 30
            return
    
        try:
            # ---- CHUNKED EXPORT: pro Zyklus nur X Keys ----
            keys = list(HA_REGMAP.keys())
            
            prio = ["battery_soc", "total_charging_energy", "total_discharging_energy"]
            keys = [k for k in prio if k in keys] + [k for k in keys if k not in prio]
    
            pos_key = f"_ha_export_pos_{battery_name}"
            pos = int(getattr(self, pos_key, 0) or 0)
    
            chunk = keys[pos:pos + HA_EXPORT_KEYS_PER_CYCLE]
            if not chunk:
                pos = 0
                chunk = keys[:HA_EXPORT_KEYS_PER_CYCLE]
    
            setattr(self, pos_key, (pos + HA_EXPORT_KEYS_PER_CYCLE) % max(1, len(keys)))
    
            for key in chunk:
                spec = HA_REGMAP[key]
    
                raw = self._read_reg_value(client, cfg["unit"], spec, battery_name)
                if raw is None:
                    # leichter Backoff, damit nicht jede Sekunde wieder knallt
                    self._mb_backoff_until[battery_name] = time.time() + 10
                    continue
    
                scale = float(spec.get("scale", 1.0))
                offset = float(spec.get("offset", 0.0))
                prec = int(spec.get("prec", 0))
                unit = str(spec.get("unit", ""))
    
                val = (raw * scale) + offset
                if prec >= 0:
                    val = round(val, prec)
    
                ent = self._ha_entity(battery_name, key)
                
                friendly = f"MarstekVenus {BATTERY_META[battery_name]['idx']} {spec.get('name', key.replace('_',' ').title())}"
                
                self._set_ha_sensor(
                    ent,
                    val,
                    unit=unit,
                    attrs={
                        "friendly_name": friendly,
                        "source": "appdaemon_pv_control",
                        "reg": int(spec["addr"]),
                        "type": str(spec.get("type")),
                    },
                )
                    
                friendly = f"MarstekVenus {BATTERY_META[battery_name]['idx']} {spec.get('name', key.replace('_',' ').title())}"
                

        finally:
            try:
                client.close()
            except Exception:
                pass


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
        self._grid_zero_since = None


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
        
        # PV-Rotation State (persistiert in BMS_STATE_FILE)
        self.pv_rotate = {
            "order": list(self.batteries.keys()),  # Reihenfolge der Batterien
            "idx": 0,                              # aktueller Start-Index
            "day": datetime.now().strftime("%Y-%m-%d"),
        }
        self._load_pv_rotate_state()

        # 🔥 HIER ist der entscheidende Teil
        self.log(
            f"Starte PVControlApp, INTERVAL={INTERVAL}s, BMS_CARE={'ON' if ENABLE_BMS_CARE else 'OFF'}")
        self._log_bms_schedule()

    
        # Backoff: nach Fehlern Batterie fuer X Sekunden nicht anfassen
        self._mb_backoff_until = {name: 0.0 for name in self.batteries}
        
        
        # PV-Regel-Loop
        self.run_every(self.read_and_log, self.datetime(), INTERVAL)

        # Log-Retention
        self.run_every(
            self._cleanup_old_logs,
            self.datetime(),
            int(LOG_CLEANUP_RUN_EVERY_H) * 3600
        )


    def _select_pv_charge_subset(self, target_w, states_no_bms):
        """
        Waehlt, welche Batterien beim PV-Laden (non-BMS) teilnehmen.
        - Beruecksichtigt Buckets: 1 Batterie / 2 Batterien / alle
        - Beruecksichtigt PV_MIN_PER_BAT_W, damit Setpoints nicht zu klein werden
        - Rotation/Persistenz, damit nicht immer dieselbe Batterie geladen wird
    
        WICHTIG:
        - states_no_bms enthaelt schon NICHT: BMS-active und NICHT: SOC-SAFETY-active
        - Also stoeren wir diese niemals.
        """
        bats = list(states_no_bms.keys())
        if not bats:
            return []
    
        # Buckets nur wenn aktiviert
        if not PV_BUCKET_ENABLE:
            return bats
    
        # Wie viele Batterien duerfen max. aktiv sein?
        if target_w < PV_BUCKET_1_TOTAL_W:
            wanted = 1
        elif target_w < PV_BUCKET_2_TOTAL_W:
            wanted = 2
        else:
            wanted = len(bats)
    
        # Mindestleistung pro Batterie beachten:
        # z.B. target=250W und wanted=2 geht nicht sinnvoll -> runter auf 1
        if PV_MIN_PER_BAT_W > 0:
            max_possible = max(1, int(target_w // PV_MIN_PER_BAT_W))
            wanted = min(wanted, max_possible)
    
        wanted = max(1, min(wanted, len(bats)))
    
        # Rotation vorbereiten
        self._pv_rotate_maybe_refresh()
        order = self.pv_rotate.get("order") or list(self.batteries.keys())
        idx = int(self.pv_rotate.get("idx", 0))
    
        # Nur Batterien nehmen, die in states_no_bms enthalten sind (eligible)
        order = [x for x in order if x in states_no_bms]
        if not order:
            order = bats
    
        # Startpunkt je nach Rotation
        if PV_ROTATE_ENABLE:
            rotated = order[idx:] + order[:idx]
        else:
            rotated = order
    
        # Zusatzlogik: bei Auswahl 1/2 sollen bevorzugt niedrigere SoC drankommen,
        # aber Rotation sorgt dafuer, dass gleiche SoCs nicht immer dieselbe Batterie treffen.
        rotated_sorted = sorted(
            rotated,
            key=lambda n: (int(states_no_bms[n].get("soc", 0)), rotated.index(n))
        )
    
        subset = rotated_sorted[:wanted]
    
        # Bei cycle-Rotation: nach Nutzung weiterdrehen (nur wenn nicht "alle")
        if PV_ROTATE_ENABLE and PV_ROTATE_MODE == "cycle" and wanted < len(bats):
            self._pv_rotate_step()
    
        return subset


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


    # ------------------------- Persistenz (PV-Rotation) ---------------------

    def _load_pv_rotate_state(self):
        """
        Laedt PV-Rotation aus derselben JSON-Datei wie BMS_STATE_FILE.
        Wenn nichts vorhanden: Defaults nutzen.
        """
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
            idx = pv.get("idx", 0)
            day = pv.get("day", None)

            bats = list(self.batteries.keys())

            # Order muss zu aktuellen Batterien passen
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
            self.pv_rotate["idx"] = int(idx or 0) % max(1, len(order))
            if day:
                self.pv_rotate["day"] = str(day)

        except Exception:
            # keine harte Abhaengigkeit
            return

    def _save_pv_rotate_state(self):
        """
        Schreibt pv_rotate in dieselbe JSON-Datei (BMS_STATE_FILE).
        Robust: bestehende Struktur bleibt erhalten.
        """
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
        """
        Rotation ggf. taeglich neu mischen (wenn PV_ROTATE_MODE='daily').
        """
        if not PV_ROTATE_ENABLE:
            return

        today = datetime.now().strftime("%Y-%m-%d")
        if PV_ROTATE_MODE == "daily" and self.pv_rotate.get("day") != today:
            order = list(self.batteries.keys())
            random.shuffle(order)
            self.pv_rotate = {"order": order, "idx": 0, "day": today}
            self._save_pv_rotate_state()

    def _pv_rotate_step(self):
        """
        Rotation einen Schritt weiterdrehen (wenn PV_ROTATE_MODE='cycle').
        """
        if not PV_ROTATE_ENABLE:
            return

        order = self.pv_rotate.get("order") or list(self.batteries.keys())
        if not order:
            return

        self.pv_rotate["idx"] = (int(self.pv_rotate.get("idx", 0)) + 1) % len(order)
        self._save_pv_rotate_state()



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
        """Protokolliert pro Batterie: letzter Zyklus, naechster Zyklus, Status/Timer (Deutsch)."""
        if not ENABLE_BMS_CARE:
            return
    
        now_ts = float(self._now_epoch or time.time())
        self.log("BMS-ZEITPLAN: Letzter Zyklus / Naechste Faelligkeit / Status (Hold/Cooldown)")
    
        for name in self.batteries.keys():
            bms = self.bms_state.get(name, {})
            phase = bms.get("phase", "idle")
            last_ts = float(bms.get("last_full_ts") or 0)
    
            next_ts = self._bms_next_due_ts(name)
            hold_until = float(bms.get("holding_until") or 0)
            cool_until = float(bms.get("cooldown_until") or 0)
    
            # --- Naechste Faelligkeit ---
            if next_ts > 0:
                rem_s = int(next_ts - now_ts)
                if rem_s <= 0:
                    next_info = f"{self._fmt_ts(next_ts)} (faellig)"
                else:
                    next_info = f"{self._fmt_ts(next_ts)} (in {self._fmt_dh(rem_s)})"
            else:
                next_info = "sofort moeglich (kein letzter Zyklus gespeichert)"
    
            # --- Hold ---
            if hold_until > now_ts:
                hold_info = f"aktiv bis {self._fmt_ts(hold_until)} ({self._fmt_dh(int(hold_until - now_ts))})"
            elif hold_until > 0:
                hold_info = f"beendet am {self._fmt_ts(hold_until)}"
            else:
                hold_info = "kein Hold aktiv"
    
            # --- Cooldown ---
            if cool_until > now_ts:
                cool_info = f"aktiv bis {self._fmt_ts(cool_until)} ({self._fmt_dh(int(cool_until - now_ts))})"
            elif cool_until > 0:
                cool_info = f"beendet am {self._fmt_ts(cool_until)}"
            else:
                cool_info = "kein Cooldown aktiv"
    
            # --- Phase deutsch ---
            phase_de = {
                "idle": "Bereitschaft",
                "charging": "Volladung aktiv",
                "hold": "Voll geladen (Haltephase)"
            }.get(phase, phase)
    
            self.log(
                f"BMS {name}: "
                f"Letzte Voll-Ladung: {self._fmt_ts(last_ts)} | "
                f"Naechste Faelligkeit: {next_info} | "
                f"Phase: {phase_de} | "
                f"Hold: {hold_info} | "
                f"Cooldown: {cool_info}"
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
            if ac_raw is None:
                ac = None
            else:
                ac = (-ac_raw) if AC_SIGN_INVERT else ac_raw
                
                
            if LOG_DIAG_SIGN and ac is not None:
                meaning = "ZERO"
                if ac < -VERIFY_TOL_W:
                    meaning = "CHARGE"
                elif ac > VERIFY_TOL_W:
                    meaning = "DISCHARGE"
                verify_logs.append(f"{name} DIAG VERIFY AC raw={ac_raw} -> ac={ac} meaning={meaning}")

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
                # erlaubt PV oder "already charging" (oder spaeter Netzladen falls du willst)
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
                f"{LOGTXT['no_dist']} ({'Laden' if effective_mode=='charge' else 'Entladen'}): "
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
        
        # Safety: falls AppDaemon reload / init race -> Backoff-Dict sicher vorhanden
        if not hasattr(self, "_mb_backoff_until") or not isinstance(self._mb_backoff_until, dict):
            self._mb_backoff_until = {name: 0.0 for name in self.batteries}
        else:
            # neue Batterien nachtragen (falls Konfig geaendert wurde)
            for _n in self.batteries:
                if _n not in self._mb_backoff_until:
                    self._mb_backoff_until[_n] = 0.0


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
            ac_ctrl_now = 0.0
            A_set = 0.0
            grid_flow = 0.0

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

            # 1b) HA Sensor Export: EINMAL pro Zyklus (zentraler Modbus-Poller ersetzt HA modbus:)
            if HA_EXPORT_ENABLE:
                for _bn, _cfg in self.batteries.items():
                    self._poll_and_export_ha_sensors(_bn, _cfg)

            # 2) Batteriewerte auslesen (Regel-Read)
            states, sum_ac = {}, 0
            
            for name, cfg in self.batteries.items():
                states[name] = dict(
                    ac=0, bp=0, soc=0,
                    ctrl=0, mode=0, cfg=cfg, err=0
                )
            
                # Backoff aktiv? Dann diese Batterie im Read ueberspringen
                now_ts = float(self._now_epoch or time.time())
                if now_ts < float(self._mb_backoff_until.get(name, 0) or 0):
                    states[name]["err"] = 1
                    states[name]["backoff"] = True
                    continue
                else:
                    states[name]["backoff"] = False
            
                client = _mb_client(cfg["host"], cfg["port"])
                if not client.connect():
                    self._mb_backoff_until[name] = time.time() + 60
                    self.error(f"[{name}] Verbindung fehlgeschlagen")
                    states[name]["err"] = 1
                    try:
                        client.close()
                    except Exception:
                        pass
                    continue
            
                try:
                    # --- AC ---
                    regs = self._modbus_read(
                        client, cfg["unit"],
                        REGISTERS["ac_power"][0], REGISTERS["ac_power"][1],
                        name
                    )
                    if regs is None:
                        states[name]["err"] = 1
                        self._mb_backoff_until[name] = time.time() + 30
                        continue
            
                    ac_raw = _i32_from_u16_be(regs)
                    ac = (-ac_raw) if AC_SIGN_INVERT else ac_raw
            
                    if LOG_DIAG_SIGN:
                        meaning = "ZERO"
                        if ac < -TOLERANCE_W:
                            meaning = "CHARGE"
                        elif ac > TOLERANCE_W:
                            meaning = "DISCHARGE"
                        self.log(f"{name}: DIAG AC raw={ac_raw} -> ac={ac} meaning={meaning}")
            
                    # --- BP ---
                    regs = self._modbus_read(
                        client, cfg["unit"],
                        REGISTERS["battery_power"][0], REGISTERS["battery_power"][1],
                        name
                    )
                    if regs is None:
                        states[name]["err"] = 1
                        self._mb_backoff_until[name] = time.time() + 30
                        continue
            
                    bp = _i32_from_u16_be(regs)
            
                    if LOG_DIAG_SIGN:
                        bp_meaning = "ZERO"
                        if bp < -TOLERANCE_W:
                            bp_meaning = "DISCHARGE"
                        elif bp > TOLERANCE_W:
                            bp_meaning = "CHARGE"
                        self.log(f"{name}: DIAG BP bp={bp} meaning={bp_meaning}")
            
                    # --- SOC ---
                    regs = self._modbus_read(client, cfg["unit"], REGISTERS["soc"], 1, name)
                    if not regs:
                        states[name]["err"] = 1
                        self._mb_backoff_until[name] = time.time() + 15
                        soc = int(states[name].get("soc", 0))
                        states[name]["soc_valid"] = False
                    else:
                        soc = int(regs[0])
                        states[name]["soc_valid"] = True
            
                    # --- CTRL + MODE in EINEM Read (42000..42010) ---
                    regs_ctrl = self._modbus_read(client, cfg["unit"], REGISTERS["control"], 1, name)  # 42000
                    regs_mode = self._modbus_read(client, cfg["unit"], REGISTERS["mode"],    1, name)  # 42010
                    
                    if regs_ctrl:
                        ctrl = int(regs_ctrl[0])
                    else:
                        states[name]["err"] = 1
                        self._mb_backoff_until[name] = time.time() + 15
                        ctrl = int(states[name].get("ctrl", 0))
                    
                    if regs_mode:
                        mode = int(regs_mode[0])
                    else:
                        states[name]["err"] = 1
                        self._mb_backoff_until[name] = time.time() + 15
                        mode = int(states[name].get("mode", 0))
                                
                    sum_ac += ac
                    states[name].update(ac=ac, bp=bp, soc=soc, ctrl=ctrl, mode=mode)
            
                    if ctrl not in CONTROL_ENABLED_VALUES:
                        states[name]["err"] = 1
            
                    if DEBUG_STATE and CALC_LOG:
                        self.log(
                            f"{name}: {LOGTXT['raw_state']} soc={soc}%, bp={bp}W, ac={ac}W, ctrl={ctrl}, mode_reg={mode}, "
                            f"last_dist={self.last_distribution.get(name, 0)}"
                        )
            
                except Exception as e:
                    self.error(f"[{name}] Modbus-Lesefehler: {e}")
                    states[name]["err"] = 1
                    self._mb_backoff_until[name] = time.time() + 30
            
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass



            # 3) Grid-Follow Basiswerte (SAUBER ueber AC, symmetrisch)
            # grid_flow: + = Bezug, - = Einspeisung (Smartmeter)
            grid_flow = float(consumption or 0.0)
            actual_flow = grid_flow
            
            # BMS/SOC-SAFETY Batterien sind "fixed" und duerfen nicht ueberschrieben werden.
            # Fuer die Regelung zaehlt nur die aktuell steuerbare Batterie-AC-Leistung.
            bms_active = set()
            if ENABLE_BMS_CARE:
                for _n, _b in self.bms_state.items():
                    if _b.get("phase") in ("charging", "hold"):
                        bms_active.add(_n)
            
            soc_safety_active = set()
            if SOC_SAFETY_ENABLE:
                for _n, _b in self.bms_state.items():
                    if _b.get("safety_active"):
                        soc_safety_active.add(_n)
            
            # Summe "ac" NUR ueber steuerbare Batterien:
            # ac > 0 = discharge (liefert), ac < 0 = charge (nimmt)
            ac_ctrl_now = 0.0
            for n, st in states.items():
                if n in bms_active:
                    continue
                if n in soc_safety_active:
                    continue
                ac_ctrl_now += float(st.get("ac", 0) or 0)
            
            # Symmetrische Grid-Follow-Formel:
            # Zielsumme AC der steuerbaren Batterien, um Netzfluss -> 0 zu regeln:
            # A_set = A_now + grid_flow
            A_set = ac_ctrl_now + grid_flow

            if LOG_DIAG_GRID:
                self.log(
                    "DIAG GRID: "
                    f"grid_flow={grid_flow:.1f}W "
                    f"ac_ctrl_now={ac_ctrl_now:.1f}W "
                    f"A_set={A_set:.1f}W "
                    f"deadband={GRID_DEADBAND_W}W"
                )


            # Zeitfilter fuer PV-Ueberschuss (Anti-PingPong bei Wolken)
            if grid_flow <= PV_CHARGE_ALLOW_W:
                if self._pv_ok_since is None:
                    # sofortige Freigabe nach Restart (optional)
                    self._pv_ok_since = time.time() - PV_SURPLUS_MIN_TIME
            else:
                # PV-Surplus Zeitfilter
                self._pv_ok_since = None
                # Grid-Follow STOP Timer (monotonic)
                self._grid_zero_since = None
           
            
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
            
            pv_pending = (self._pv_ok_since is not None) and (not pv_surplus)
            
            self.pv_surplus = bool(pv_surplus)
            self.pv_pending = bool(pv_pending)
            self._last_pv_surplus = self.pv_surplus

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

            now = time.monotonic()

            # vorheriger Modus (restart-sicher)
            prev = self.mode_state or "stop"
            stable_for = now - float(self.mode_since or now)
            effective_mode = prev or "stop"


            # ---------------------------------------------------------
            # MODE-ENTSCHEIDUNG (Grid-Follow ueber A_set = ac_ctrl_now + grid_flow)
            # STOP erst nach "nahe 0" stabil
            # ---------------------------------------------------------
            
            soc_safety_active_any = any(
                b.get("safety_active") for b in self.bms_state.values()
            )
            
            new_mode = prev or "stop"
            
            # Deadband um 0: erst ausserhalb umschalten
            if A_set > GRID_DEADBAND_W:
                new_mode = "discharge"
                self._grid_zero_since = None
            
            elif A_set < -GRID_DEADBAND_W:
                new_mode = "charge"
                self._grid_zero_since = None
            
            else:
                # innerhalb Deadband: STOP erst nach GRID_STOP_AFTER_S, sonst Modus halten
                if self._grid_zero_since is None:
                    self._grid_zero_since = time.monotonic()
            
                zero_for = time.monotonic() - self._grid_zero_since
            
                # SOC-SAFETY soll nie durch STOP-Timer "weggedrueckt" werden
                if soc_safety_active_any:
                    new_mode = prev or "charge"  # SOC-SAFETY ist immer charge
                else:
                    if zero_for >= GRID_STOP_AFTER_S:
                        new_mode = "stop"
                    else:
                        new_mode = prev or "stop"



            # -------------------------
            # Modus-Stabilisierung
            # -------------------------

            if new_mode != prev:
                if stable_for >= STABLE_TIMER_S:
                    self.mode_state = new_mode
                    self.mode_since = now
                    effective_mode = new_mode
                    if CALC_LOG:
                        calc_logs.append(
                            f"Moduswechsel: {MODE_DE.get(prev,'stopp')} -> {MODE_DE.get(new_mode,'stopp')}"
                        )
                else:
                    effective_mode = prev
            else:
                effective_mode = prev

            # -------------------------
            # Status-Log (einmalig)
            # -------------------------

            if CALC_LOG:
                calc_logs.append(
                    f"Netzfluss = {actual_flow:.1f} W | "
                    f"Modus: {MODE_DE.get(prev)} -> {MODE_DE.get(new_mode)} | "
                    f"stabil seit {int(stable_for)} s | "
                    f"wirksam: {MODE_DE.get(effective_mode)}"
                )


            # -----------------------------------------------------------------
            # HARD OVERRIDE: Wenn Discharge effektiv nicht moeglich (SoC<=Min),
            # dann effective_mode nicht "discharge" lassen (Anzeige + Logik).
            # -----------------------------------------------------------------
                        
            # Batterien ohne BMS/SOC-SAFETY für normale Logik
            states_no_bms = {}
            for n, st in states.items():
                if n in bms_active:
                    continue
                if SOC_SAFETY_ENABLE and self.bms_state.get(n, {}).get("safety_active"):
                    continue
                states_no_bms[n] = st
            
            if effective_mode == "discharge":
                any_discharge_possible = False
                for n, st in states_no_bms.items():
                    soc = int(st.get("soc", 0))
                    mn = int(st["cfg"].get("min_soc", 0))
                    if soc > mn and not self._bms_battery_block_discharge(n):
                        any_discharge_possible = True
                        break

                if not any_discharge_possible:
                    # wir koennen physisch nicht entladen -> stop (oder charge wenn bp_ctrl_target<0)
                    forced = "charge" if A_set < -GRID_DEADBAND_W else "stop"
                    if CALC_LOG:
                        calc_logs.append(
                            f"HARD-OVERRIDE: discharge nicht moeglich (alle SoC<=MinSoC oder BMS block) -> setze wirksam auf {MODE_DE.get(forced)}"
                        )
                    effective_mode = forced
                    self.mode_state = forced
                    self.mode_since = time.monotonic()



            # -------------------------
            # Verteilung (steuerbare Batterien, ohne BMS/SOC-SAFETY)
            # target basiert auf A_set
            # -------------------------
            
            distribution = {}
            
            if effective_mode == "discharge":
            
                target_w = int(round(max(0.0, A_set)))
            
                if CALC_LOG:
                    calc_logs.append(
                        f"GRID-FOLLOW(DISCHARGE): "
                        f"grid={grid_flow:.1f}W "
                        f"ac_ctrl_now={ac_ctrl_now:.1f}W "
                        f"A_set={A_set:.1f}W "
                        f"target={target_w}W"
                    )
            
                distribution = self._distribute_waterfill(
                    target_w,
                    states_no_bms,
                    "discharge",
                    calc_logs
                )
            
            elif effective_mode == "charge":
            
                target_w = int(round(max(0.0, -A_set)))
            
                allow_charge = bool(self.pv_surplus) or (ac_ctrl_now < -TOLERANCE_W)

                # WICHTIG: _calc_weights() nutzt getattr(self,"allow_charge",False)
                # -> daher hier pro Zyklus setzen
                self.allow_charge = bool(allow_charge)
            
                if CALC_LOG:
                    calc_logs.append(
                        f"GRID-FOLLOW(CHARGE): "
                        f"grid={grid_flow:.1f}W "
                        f"ac_ctrl_now={ac_ctrl_now:.1f}W "
                        f"A_set={A_set:.1f}W "
                        f"target={target_w}W "
                        f"allow={'YES' if allow_charge else 'NO'}"
                    )
            
                if allow_charge:
                    subset = (
                        self._select_pv_charge_subset(target_w, states_no_bms)
                        if PV_BUCKET_ENABLE
                        else list(states_no_bms.keys())
                    )
            
                    states_subset = {n: states_no_bms[n] for n in subset}
            
                    distribution = self._distribute_waterfill(
                        target_w,
                        states_subset,
                        "charge",
                        calc_logs
                    )
                else:
                    distribution = {}
                    if CALC_LOG:
                        if getattr(self, "pv_pending", False):
                            calc_logs.append(
                                "CHARGE PENDING: PV noch nicht stabil -> halte aktuelle Setpoints (kein STOP)."
                            )
                        else:
                            calc_logs.append(
                                "CHARGE gesperrt: kein PV-Ueberschuss."
                            )
            
            else:
                # STOP
                distribution = {}
            
            self._last_distribution_debug = dict(distribution)




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
            
                if st.get("err"):
                    if CALC_LOG:
                        calc_logs.append(f"{name}: skip write (read/connect error)")
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
                            # PV pending: KEIN STOP/0W schreiben -> aktuelle Setpoints halten
                            if getattr(self, "pv_pending", False):
                                need_write = False
                            else:
                                need_write = (old != 0) or (int(st.get("mode", 0)) != 0) or (abs(int(st.get("bp", 0))) > TOLERANCE_W)
                    
                        if need_write:
                            self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 1 if limit > 0 else 0, name)
                            time.sleep(0.03)
                            self._modbus_write(client, cfg["unit"], REGISTERS["charge_set"], limit, name)
                            write_logs.append(f"{name}: laden {old}->{limit}")
                            self.last_distribution[name] = limit
                    
                            self._verify_setpoint_execution(client, cfg, name, "charge", limit, verify_logs)
                    
                        continue


                    
                    # ---------- DISCHARGE ----------
                    elif effective_mode == "discharge":
                        limit = min(desired, int(cfg["max_discharge_w"]))
                        if int(st.get("soc", 0)) <= mn:
                            limit = 0

                        need_write = abs(limit - old) > TOLERANCE_W

                        # Wenn limit=0: nicht sofort STOP schreiben, nur wenn wirklich "still" werden soll
                        if limit == 0:
                            need_write = (old != 0) or (int(st.get("mode", 0)) != 0) or (abs(int(st.get("bp", 0))) > TOLERANCE_W)

                        if need_write:
                            # Wichtig: Mode nur setzen, wenn wir wirklich aktiv entladen sollen,
                            # ansonsten 0 (STOP) nur dann, wenn oben need_write True ist.
                            self._modbus_write(client, cfg["unit"], REGISTERS["mode"], 2 if limit > 0 else 0, name)
                            time.sleep(0.03)
                            self._modbus_write(client, cfg["unit"], REGISTERS["discharge_set"], limit, name)
                            write_logs.append(f"{name}: entladen {old}->{limit}")
                            self.last_distribution[name] = limit

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

                    pv_txt = "JA" if bool(getattr(self, "_last_pv_surplus", False)) else "NEI"

                    # Mode aus Register
                    mode_reg = int(st.get("mode", 0))
                    if mode_reg == 1:
                        mode_txt = "CHG"
                    elif mode_reg == 2:
                        mode_txt = "DIS"
                    else:
                        mode_txt = "STP"

                    # Zustand nur über Mode
                    if bms_obj.get("safety_active"):
                        zustand_txt = "Laden - SOC-SAFETY"

                    elif bms_phase in ("charging", "hold"):
                        zustand_txt = "Laden - BMS aktiv"

                    else:
                        # Zustand nach echter Leistung (AC) anzeigen:
                        # AC < 0 = Laden, AC > 0 = Entladen/Einspeisen, nahe 0 = Leerlauf
                        ac_val = int(st.get("ac", 0) or 0)
                    
                        if abs(ac_val) <= TOLERANCE_W:
                            zustand_txt = "Leerlauf"
                        elif ac_val < 0:
                            zustand_txt = "Laden"
                        else:
                            zustand_txt = "Entladen"

                    # SOC11 Zeit
                    since = float(bms_obj.get("min_soc_since") or 0)
                    if since > 0:
                        soc11_txt = self._fmt_dh(int(self._now_epoch - since))
                    else:
                        soc11_txt = "-"

                    vals = [
                        name[-1].ljust(COL_WIDTH["bat"]),
                        f"{int(st.get('soc', 0))}%".ljust(COL_WIDTH["soc"]),
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

                if CALC_LOG:
                    self.log(
                        f"Zaehler:{consumption}W AC_sum:{sum_ac}W Netz:{actual_flow}W | "
                        f"ac_ctrl_now:{ac_ctrl_now:.1f}W A_set:{A_set:.1f}W "
                        f"(ctrl=BMS/SOC-SAFETY ausgeschlossen)"
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

        self._busy = False
