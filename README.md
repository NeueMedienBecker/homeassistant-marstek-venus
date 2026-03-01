
# Marstek Venus PVControl – Home Assistant / AppDaemon Skript

Python-/AppDaemon-Skript zur intelligenten Steuerung von **Marstek-Venus-Batteriesystemen**
in **Home Assistant** ueber **Modbus**.

Das Skript realisiert eine stabile Grid-Follow-Regelung (laden/entladen/stopp), beruecksichtigt
SoC-Grenzen, enthaelt BMS/Battery-Care (Balancing-Zyklen), eine SOC-SAFETY (Anti-Totlaufen am Min-SoC)
und liefert ausfuehrliches Logging inkl. Status-Tabelle.

> Hinweis: Das Skript ist bewusst **monolithisch** aufgebaut und benoetigt **keine apps.yaml-Parameter**.
> Konfiguration erfolgt direkt im Script (z.B. BATTERY_CONFIG, POWER_SENSOR, HA_REGMAP).

---

## Funktionen (Highlights)

### Grid-Follow Regelung
- Automatische Umschaltung zwischen **laden / entladen / stopp**
- **STOP nur, wenn wirklich nichts zu tun ist** (Grid ~ 0, Batterie ~ 0, kein Zwang aktiv)
- Ping-Pong-Schutz ueber `STABLE_TIMER_S`

### PV-Surplus Zeitfilter (Anti-Wolken-PingPong)
- PV-Ueberschuss wird erst nach `PV_SURPLUS_MIN_TIME` Sekunden als stabil gewertet
- reduziert hektisches Umschalten bei kurzen PV-Schwankungen

### Multi-Battery Power Allocation (Waterfill)
- Leistungsverteilung per **Waterfill** auf alle teilnehmenden Batterien
- Gewichtung nach **verfuegbarer Energie (rest_kWh)** oberhalb/unterhalb der SoC-Grenzen
- Beruecksichtigt Limits (`max_charge_w` / `max_discharge_w`), SoC-Sperren und BMS/Cooldown

### BMS / Battery Care (Balancing)
- Periodische Volladung bis `FULL_CHARGE_SOC` (z.B. alle `FULL_CHARGE_INTERVAL_DAYS` Tage)
- Haltephase `FULL_CHARGE_MIN_HOLD_S` und anschliessender Cooldown `ALLOW_DISCHARGE_AFTER_S`
- Maximal `MAX_BMS_BATTERIES_PER_DAY` pro Tag
- **Weicher BMS-Modus**: BMS laeuft fuer eine Batterie, andere duerfen weiter normal regeln
- Persistenz in `BMS_STATE_FILE`

### SOC-SAFETY (Anti-Totlaufen unter Min-SoC)
- Wenn eine Batterie sehr lange am/unter `SOC_SAFETY_MIN_SOC` haengt, startet eine **sanfte Schutzladung**
- Startbedingung: Dauer >= `SOC_SAFETY_AFTER_HOURS`
- Ziel: `SOC_SAFETY_TARGET_SOC`
- Optional: Schutzladung nur bei PV-Ueberschuss (`SOC_SAFETY_REQUIRE_PV`)

### Setpoint Execution Verification (optional)
- Nach Writes kann AC/Battery-Power gelesen werden (`VERIFY_SETPOINT_EXEC`)
- Warnung, wenn Setpoint offenbar ignoriert wird (MinSoC/Modus/Internes Limit)

### Logging & Status Table
- Pro Zyklus eine PVCONTROL Tabelle (SoC, BP, AC, Mode, Soll/Last, BMS/Safety, Errors)
- Debug-Flags fuer Rohdaten, Gewichte, Setpoints, Modbus TX/RX

---

## Voraussetzungen

Pflicht:
- Home Assistant (Core / OS / Supervised)
- AppDaemon Add-on (installiert **und laufend**)
- Marstek-Venus Batteriesystem(e)
- Modbus-Zugriff (TCP oder RTU)
- Netzwerkverbindung zur Batterie (TCP) bzw. RS485/Adapter (RTU)
- Schreibrechte fuer `/config/appdaemon/`

Python Pakete:
- `pymodbus`
- optional: `paho-mqtt` (nur falls du MQTT-Teile ergaenzt/nutzt)

> Wichtig: Das Skript wird nur geladen und ausgefuehrt, wenn AppDaemon aktiv laeuft.

---

## Verzeichnisstruktur (WICHTIG)

Empfohlene Struktur:

```
/config/appdaemon/apps/pv_control/
  pv_control.py
  __init__.py
```

Wichtig:
- Keine Leerzeichen in Datei- oder Ordnernamen
- Dateiname exakt: `pv_control.py`
- `__init__.py` wird empfohlen (AppDaemon-Package sauber)

---

## Installation

### 1) AppDaemon installieren und starten
1. Home Assistant -> Einstellungen -> Add-ons
2. Add-on Store -> **AppDaemon** installieren
3. AppDaemon starten
4. Pruefen:
   - Weboberflaeche erreichbar (Standard-Port: 5050)
   - Status: laeuft

### 2) Python Pakete in AppDaemon installieren
In der AppDaemon Add-on Konfiguration unter **Python packages** eintragen:

- `pymodbus`
- optional: `paho-mqtt`

Dann:
- Speichern
- AppDaemon neu starten

### 3) Script ablegen
`pv_control.py` nach:

- `/config/appdaemon/apps/pv_control/pv_control.py`

### 4) apps.yaml (minimal)
Auch wenn die Konfiguration im Script liegt, brauchst du einen minimalen Eintrag, damit AppDaemon die App startet:

Datei: `/config/appdaemon/apps/apps.yaml`

```yaml
pv_control:
  module: pv_control
  class: PVControlApp
```

AppDaemon neu starten und Logs pruefen.

---

## Konfiguration im Script

### Smartmeter / Netzfluss (HA Entity)
```python
POWER_SENSOR = {
    "enabled": True,
    "entity": "sensor.stromzaehler_sml_aktuelle_wirkleistung"
}
```

Vorzeichen:
- `grid_flow > 0`  => Netzbezug
- `grid_flow < 0`  => Einspeisung

### Batterien
```python
BATTERY_CONFIG = {
  "battery1": {
    "enabled": True,
    "modbus": True,
    "host": "192.168.100.201",
    "port": 502,
    "unit": 1,
    "capacity_kwh": 5.6,
    "max_charge_w": 2500,
    "max_discharge_w": 2500,
    "min_soc": 11,
    "max_soc": 99
  },
  "battery2": { ... },
  "battery3": { ... }
}
```

### PV Surplus Stabilisierung
```python
PV_CHARGE_ALLOW_W = -150
PV_SURPLUS_MIN_TIME = 30
```

Interpretation:
- PV-Ueberschuss wird angenommen wenn `grid_flow < PV_CHARGE_ALLOW_W`
- erst nach `PV_SURPLUS_MIN_TIME` Sekunden wird `pv_surplus=True`

### BMS Battery Care
```python
ENABLE_BMS_CARE = True
FULL_CHARGE_SOC = 99
FULL_CHARGE_INTERVAL_DAYS = 10
FULL_CHARGE_MIN_HOLD_S = 45 * 60
ALLOW_DISCHARGE_AFTER_S = 30 * 60
MAX_BMS_BATTERIES_PER_DAY = 1
BMS_FORCE_CHARGE_W = 1200
BMS_STATE_FILE = "/config/appdaemon/data/pv_control_bms_state.json"
```

### SOC-SAFETY
```python
SOC_SAFETY_ENABLE = True
SOC_SAFETY_MIN_SOC = 11
SOC_SAFETY_TARGET_SOC = 12
SOC_SAFETY_AFTER_HOURS = 48
SOC_SAFETY_CHARGE_W = 400
SOC_SAFETY_REQUIRE_PV = False
```

### Register Map (zentral) – `HA_REGMAP`
Die neue Version nutzt eine zentrale Registerbeschreibung fuer konsistente Readouts/Sensoren
und kuenftige Erweiterungen.

```python
# Register-Definitionen: 1x zentral, gilt fuer alle Batterien gleich
# type: u16, s16, u32, s32
# scale/offset/precision wie in YAML
HA_REGMAP = {
  "soc": {"addr": 32104, "count": 1, "type": "u16", "scale": 1, "offset": 0, "precision": 0, "unit": "%"},
  "battery_power": {"addr": 32102, "count": 2, "type": "s32", "scale": 1, "offset": 0, "precision": 0, "unit": "W"},
  "ac_power": {"addr": 32202, "count": 2, "type": "s32", "scale": 1, "offset": 0, "precision": 0, "unit": "W"},
  "control": {"addr": 42000, "count": 1, "type": "u16", "scale": 1, "offset": 0, "precision": 0},
  "mode": {"addr": 42010, "count": 1, "type": "u16", "scale": 1, "offset": 0, "precision": 0},
  "charge_set": {"addr": 42020, "count": 1, "type": "u16", "scale": 1, "offset": 0, "precision": 0, "unit": "W"},
  "discharge_set": {"addr": 42021, "count": 1, "type": "u16", "scale": 1, "offset": 0, "precision": 0, "unit": "W"},
}
```

---

## Home Assistant Entities

### BMS Timeline (dynamisch gesetzt)
Pro Batterie:
- `sensor.bms_<battery>_phase`

Attribute:
- `last_full`
- `hold_until`
- `cooldown_until`

Beispiel:
- `sensor.bms_battery1_phase`

---

## Troubleshooting (typische Fehler)

| Fehler | Ursache | Loesung |
|------|---------|--------|
| `ModuleNotFoundError: pymodbus` | Python Paket fehlt | AppDaemon Add-on -> Python packages -> `pymodbus` eintragen, Neustart |
| Keine Logausgabe | AppDaemon laeuft nicht / App nicht geladen | AppDaemon Status pruefen, `apps.yaml` Eintrag pruefen |
| `Entity ... not found` | POWER_SENSOR Entity stimmt nicht | Entity-ID in HA pruefen und in `POWER_SENSOR["entity"]` korrigieren |
| Viele `Verbindung fehlgeschlagen` | IP/Port falsch, Netzwerk/VLAN/Firewall | Ping/Port 502 pruefen, Routing/ACL checken |
| Setpoint wird ignoriert | Internes Limit / Modus / MinSoC / Control nicht enabled | `control` Status pruefen, `VERIFY_SETPOINT_EXEC` Logs lesen |

---

## Changelog

### 2026-03-01 (New Version)

#### Added
- Zentrale Registerdefinition `HA_REGMAP` (type/scale/offset/precision wie YAML)
- PV-Surplus Zeitfilter `PV_SURPLUS_MIN_TIME` (Anti-Wolken-PingPong)
- SOC-SAFETY mit Persistenz (`min_soc_since`, `safety_active`, `safety_done`)
- Option `SOC_SAFETY_REQUIRE_PV` (Schutzladung nur bei PV-Ueberschuss)
- HA Timeline Sensoren: `sensor.bms_<battery>_phase` inkl. Attribute (last_full/hold_until/cooldown_until)
- Setpoint Execution Verification (optional): `VERIFY_SETPOINT_EXEC`, `VERIFY_MIN_SET_W`, `VERIFY_TOL_W`

#### Changed
- Mode-Entscheidung (Grid-Follow): STOP nur bei realem 0-Case, Discharge sobald Last oder Batterie laeuft
- BMS-Care als **weicher Modus**: eine Batterie im BMS, andere laufen weiter normal
- Waterfill-Verteilung: bessere Gruende/Logs bei "keine Verteilung moeglich"
- Schreiblogik: weniger Writes durch Toleranz und "need_write" bei limit=0
- Logging/Finally: unbound-sicher, Tabelle immer stabil

#### Fixed
- Stabilere Fehlerbehandlung bei Modbus Read/Write (kein Crash bei None/fehlenden Keys)
- Reduziert Ping-Pong bei PV-Events durch Surplus-Stabilisierung

### Older Version
- Basis-Grid-Follow Regelung
- BMS-Care (weniger weicher Ablauf)
- Logging Tabelle und grundlegende Debug-Ausgaben

---

## Lizenz

MIT License – freie Nutzung, Aenderung und Weitergabe erlaubt.

---

## Autor & Hinweis

Dieses Projekt wurde von **Neue Medien Becker** erstellt, gepflegt und dokumentiert.

Die Konzeption, Umsetzung und Dokumentation basieren auf praktischer Erfahrung aus realen
Home-Assistant- und Energiemanagement-Installationen.

---

## Unterstuetzung

Wenn dir dieses Projekt hilft und du die Weiterentwicklung unterstuetzen moechtest,
freue ich mich ueber eine freiwillige Spende.

Kontakt:
- **info@neuemedienbecker.de**
