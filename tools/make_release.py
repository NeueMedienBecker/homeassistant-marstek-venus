#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nur fuer den Maintainer: baut aus der laufenden Installation die GitHub-Version.

  python3 tools/make_release.py --live /addon_configs/a0d7b954_appdaemon/apps/Marstek-Venus-E \
          --dashboard /config/.storage/lovelace.dashboard_marstek --version 2.0.0

Schritte:
  * pv_control.py / pv_mqtt.py aus der Installation uebernehmen (+ Versionskennung)
  * pv_config.py als neutrale Vorlage (Beispiel-Batterie, keine eigenen IPs / Sensoren)
  * Dashboard als Vorlage (eigene Sensoren durch Platzhalter ersetzt)
"""
import argparse
import json
import re as _re
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "installer"))
import ha_setup as H  # noqa: E402


def rd(p):
    with open(p, encoding="utf-8") as f:
        return f.read()


def wr(p, s):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(s if s.endswith("\n") else s + "\n")


def ind(s):
    return s[:len(s) - len(s.lstrip())]


def find(lines, text, start=0, startswith=False):
    for i in range(start, len(lines)):
        t = lines[i].strip()
        if (t.startswith(text) if startswith else t == text):
            return i
    raise SystemExit("ANKER NICHT GEFUNDEN: " + text)


def build_control(src, version):
    L = src.split("\n")
    if any("FIX 39" in x for x in L):
        return src
    i = find(L, "- FIX 38:", startswith=True)
    L.insert(i + 1, ind(L[i]) + f"- FIX 39: Version {version} (GitHub). Netz-Zaehler: POWER_SENSOR \"invert\" "
                                "(Vorzeichen drehen) und kW-Werte automatisch in W.")
    i = find(L, "from pymodbus.client import ModbusTcpClient")
    L.insert(i + 1, f'PV_CONTROL_VERSION = "{version}"  # FIX 39')
    i = find(L, "def initialize(self):")
    L.insert(i + 1, ind(L[i]) + "    " + 'self.log(f"Marstek Venus Steuerung {PV_CONTROL_VERSION} startet")  # FIX 39')
    i = find(L, '_mval = float(_mfull.get("state"))')
    b = ind(L[i])
    L[i + 1:i + 1] = [
        b + '_munit = str((_mfull.get("attributes") or {}).get("unit_of_measurement") or "W").strip().lower()  # FIX 39',
        b + 'if _munit == "kw":',
        b + '    _mval *= 1000.0',
        b + 'if POWER_SENSOR.get("invert"):',
        b + '    _mval = -_mval',
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


READ_ONLY_DOC = (
    "# FIX 40: Beobachtungsmodus. True = Werte lesen und anzeigen, aber KEINE Befehle an die Batterien\n"
    "# senden (zum Testen, wenn noch eine andere Steuerung laeuft oder der Netz-Zaehler noch fehlt).\n"
    "# Auch im Dashboard schaltbar (Konfiguration -> Hauptschalter & Batterien -> Nur beobachten).\n"
    "READ_ONLY = False\n"
)


def fix40_control(src):
    """FIX 40: Beobachtungsmodus READ_ONLY - nichts an die Batterien schreiben."""
    if "FIX 40" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 39:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 40: Beobachtungsmodus READ_ONLY (nur lesen, keine Befehle an die Batterien), "
                                "z. B. wenn noch eine andere Steuerung laeuft oder der Netz-Zaehler fehlt.")
    i = find(L, "# Rueckwaertskompatibilitaet: neue Spalten auch bei alter config sicherstellen.")
    L[i:i] = ["# FIX 40: Beobachtungsmodus (Standard aus, falls eine alte pv_config.py ihn nicht kennt)",
              'if "READ_ONLY" not in globals():',
              "    READ_ONLY = False",
              ""]
    # Modbus: beide Schreib-Funktionen
    for start in ("def _modbus_write_generic(", "def _write_raw("):
        i = find(L, start, startswith=True)
        j = find(L, "v = int(value)", i)
        b = ind(L[j])
        L[j + 1:j + 1] = [b + 'if globals().get("READ_ONLY", False):  # FIX 40: Beobachtungsmodus - nichts schreiben',
                          b + "    return True"]
    # API: Sollwert senden
    i = find(L, "def _send_power(self, power, why=\"\"):")
    j = i + 1
    while not L[j].strip().startswith('"""'):
        j += 1
    b = ind(L[j])
    L[j + 1:j + 1] = [b + 'if globals().get("READ_ONLY", False):  # FIX 40: Beobachtungsmodus - nichts senden',
                      b + "    return True"]
    i = find(L, 'self.log(f"Marstek Venus Steuerung {PV_CONTROL_VERSION} startet")  # FIX 39')
    b = ind(L[i])
    L[i + 1:i + 1] = [b + 'if globals().get("READ_ONLY", False):  # FIX 40',
                      b + '    self.log("BEOBACHTUNGSMODUS: es werden keine Befehle an die Batterien gesendet '
                          '(nur lesen). Ausschalten im Dashboard: Nur beobachten", level="WARNING")']
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix40_mqtt(src):
    if '"READ_ONLY"' in src:
        return src
    L = src.split("\n")
    i = find(L, '("CONTROL_ENABLE",', startswith=True)
    L.insert(i + 1, ind(L[i]) + '("READ_ONLY",                 "switch", "Nur beobachten (keine Befehle an Batterien)", '
                                'None, None, None, None, "mdi:eye-outline", None),  # FIX 40')
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix40_config(src):
    if re.search(r"(?m)^READ_ONLY\s*=", src):
        return src
    m = re.search(r"(?m)^CONTROL_ENABLE = .*\n", src)
    if not m:
        raise SystemExit("ANKER NICHT GEFUNDEN: CONTROL_ENABLE")
    s = src[:m.end()] + READ_ONLY_DOC + src[m.end():]
    compile(s, "pv_config.py", "exec")
    return s


RO_ROW = {"entity": "switch.marstek_read_only", "name": "Nur beobachten (keine Befehle an die Batterien)"}
RO_TILE = {"type": "tile", "entity": "switch.marstek_read_only", "name": "Nur beobachten", "icon": "mdi:eye-outline",
           "color": "orange", "visibility": [{"condition": "state", "entity": "switch.marstek_read_only", "state": "on"}]}
RO_BANNER = {
    "type": "conditional",
    "conditions": [{"condition": "state", "entity": "switch.marstek_read_only", "state": "on"}],
    "card": {"type": "markdown", "content": (
        "### 👁 Beobachtungsmodus\n"
        "Die Steuerung liest die Batterien nur aus und **sendet keine Befehle**. "
        "Das ist so gewollt, wenn noch eine andere Steuerung die Batterien regelt oder der Netz-Zähler fehlt.\n\n"
        "**Ausschalten:** Konfiguration → Hauptschalter & Batterien → „Nur beobachten“ aus.")},
}


def fix40_dashboard(cfg):
    """Schalter 'Nur beobachten' als Zeile unter dem Hauptschalter (Konfiguration), Kachel in der Uebersicht
    (nur sichtbar, wenn an) und Hinweis-Banner. Repariert auch eine fehlerhafte fruehere Einfuegung."""
    def walk(o, key=None):
        if isinstance(o, dict):
            for k, v in o.items():
                walk(v, k)
        elif isinstance(o, list):
            # alte, fehlerhafte Einfuegung (Zeile ohne "type" in einer Kartenliste) entfernen
            if key == "cards":
                o[:] = [it for it in o if not (isinstance(it, dict) and "type" not in it)]
            for k, it in enumerate(list(o)):
                eid = it if isinstance(it, str) else (it.get("entity") if isinstance(it, dict) else None)
                if eid == "switch.marstek_control_enable":
                    nxt = o[k + 1] if k + 1 < len(o) else None
                    nxt_eid = nxt.get("entity") if isinstance(nxt, dict) else nxt
                    if nxt_eid != "switch.marstek_read_only":
                        if key == "entities":
                            o.insert(k + 1, dict(RO_ROW))
                        elif key == "cards" and isinstance(it, dict) and it.get("type") == "tile":
                            o.insert(k + 1, json.loads(json.dumps(RO_TILE)))
                    break
            for it in o:
                walk(it, key)

    walk(cfg)
    s = json.dumps(cfg, ensure_ascii=False)
    if "switch.marstek_read_only" not in s:
        raise SystemExit("ANKER NICHT GEFUNDEN: switch.marstek_control_enable im Dashboard")
    v0 = cfg["views"][0]
    if "Beobachtungsmodus" not in json.dumps(v0, ensure_ascii=False):
        if v0.get("sections"):
            cards = v0["sections"][0].setdefault("cards", [])
            pos = 1 if cards and cards[0].get("type") == "heading" else 0
            cards.insert(pos, json.loads(json.dumps(RO_BANNER)))
        else:
            v0.setdefault("cards", []).insert(0, json.loads(json.dumps(RO_BANNER)))
    return cfg


MODBUS_ONLY = re.compile(r"^sensor\.marstekvenus_(\d+)_(inverter_state|battery_voltage|temp_mos1|temp_mos2)$")


def fix41_dashboard(cfg):
    """Werte, die es nur per Modbus gibt (Wechselrichter-Status, Spannung, MOS-Temperaturen), nur zeigen,
    wenn die Batterie gerade per Modbus angebunden ist - sonst 'Entitaet nicht gefunden' bei WLAN-Batterien."""
    def wrap(row):
        eid = row.get("entity") if isinstance(row, dict) else row
        m = MODBUS_ONLY.match(eid or "")
        if not m:
            return row
        return {"type": "conditional",
                "conditions": [{"condition": "state", "entity": f"sensor.pv_battery{m.group(1)}_comm_mode",
                                "state": "Modbus"}],
                "row": row if isinstance(row, dict) else {"entity": row}}

    def walk(o):
        if isinstance(o, dict):
            if o.get("type") == "entities" and isinstance(o.get("entities"), list):
                o["entities"] = [wrap(r) for r in o["entities"]]
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(cfg)
    return cfg


def fix45_dashboard(cfg):
    """FIX 45: im Batterien-Tab zeigen, wann die BMS-Pflege zuletzt lief und wann sie wieder ansteht."""
    def walk(o):
        if isinstance(o, dict):
            if o.get("type") == "entities" and isinstance(o.get("entities"), list):
                new = []
                for r in o["entities"]:
                    new.append(r)
                    eid = r.get("entity") if isinstance(r, dict) else r
                    m = re.match(r"^sensor\.bms_battery(\d+)_phase$", eid or "")
                    if m and isinstance(r, dict) and r.get("type") is None:
                        has = any(isinstance(x, dict) and x.get("attribute") == "next_due" and x.get("entity") == eid
                                  for x in o["entities"])
                        if not has:
                            new.append({"type": "attribute", "entity": eid, "attribute": "last_full",
                                        "name": "BMS-Pflege zuletzt", "icon": "mdi:calendar-check"})
                            new.append({"type": "attribute", "entity": eid, "attribute": "next_due",
                                        "name": "BMS-Pflege wieder faellig", "icon": "mdi:calendar-clock"})
                o["entities"] = new
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(cfg)
    return cfg


EXT_HELP_MD = """<details><summary>ℹ️ <b>Zähler &amp; zweiter Speicher: Was bedeutet das?</b></summary>

**Netz-Zähler (Sensor):** Der Home-Assistant-Sensor, der die Leistung am Hausanschluss misst (in W oder kW). Die Regelung versucht, diesen Wert auf 0 zu halten. **Positiv muss Bezug aus dem Netz sein, negativ Einspeisung.** Zeigt dein Zähler es andersherum, schalte **Vorzeichen drehen** ein.

Aktuell: `{{ states('text.marstek_grid_sensor') }}` → **{{ states(states('text.marstek_grid_sensor')) }} {{ state_attr(states('text.marstek_grid_sensor'),'unit_of_measurement') }}**

**Zweiter Speicher:** Hast du noch einen anderen Speicher, der selbst auf 0 am Hausanschluss regelt (z. B. ein Hybrid-Wechselrichter wie SMA, Fronius, Huawei), dann sieht die Venus am Zähler nur noch 0 W und weiß nicht, dass der andere Speicher gerade aushilft. Trage hier dessen **Entladeleistung**, **Ladeleistung** und die **PV-Leistung** ein (alles Sensoren in W oder kW). Die Regelung rechnet dann so:

- **Abends:** Entlädt der andere Speicher, zählt das wie Netzbezug → die Venus übernimmt den Hausverbrauch **zuerst**, der andere Speicher bleibt Reserve.
- **Tagsüber:** Lädt der andere Speicher aus der PV, bleibt das unsichtbar → **erst er wird voll**, die Venus lädt nur mit dem, was dann noch übrig ist. Ist die Sonne weg, hört die Venus auf.
- Lädt der andere Speicher mehr, als die PV liefert, kommt der Strom aus der Venus → das zählt wie Einspeisung und die Venus regelt zurück.

Gibt es nur **einen** Sensor für den anderen Speicher (+ = Entladen, − = Laden), trage ihn bei **Entladeleistung** ein und lass **Ladeleistung** leer. Ohne zweiten Speicher: alle drei Felder leer lassen.

Änderungen wirken sofort. **Einstellungen fest übernehmen** (unten) schreibt sie dauerhaft in `pv_config.py`.

</details>"""


def fix46_control(src):
    """FIX 46: Netz-Zaehler und zweiter Speicher (Hybrid-Wechselrichter) im Dashboard einstellbar;
    BMS-Pflege: Wartezeit auf Sonne zaehlt ab Faelligkeit, danach volle Leistung ohne PV."""
    if "FIX 46" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 45:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 46: Netz-Zaehler (Sensor, Vorzeichen) und ein zweiter Speicher (Entlade-/Ladeleistung, PV) sind im "
                                "Dashboard einstellbar (GRID_SENSOR, GRID_SENSOR_INVERT, EXT_STORAGE_*_SENSOR, EXT_PV_SENSOR); "
                                "BMS-Pflege: 'max. Warten auf Sonne' zaehlt ab Faelligkeit, danach wird ohne PV durchgeladen.")
    # 1) Defaults nach dem READ_ONLY-Block
    i = find(L, "READ_ONLY = False")
    assert L[i - 1].strip() == 'if "READ_ONLY" not in globals():'
    L[i + 1:i + 1] = [
        "",
        "# FIX 46: Zaehler und zweiter Speicher - Standard aus POWER_SENSOR, im Dashboard aenderbar",
        'if "GRID_SENSOR" not in globals():',
        '    GRID_SENSOR = str((POWER_SENSOR or {}).get("entity") or "")',
        'if "GRID_SENSOR_INVERT" not in globals():',
        '    GRID_SENSOR_INVERT = bool((POWER_SENSOR or {}).get("invert"))',
        'for _k46 in ("EXT_STORAGE_DISCHARGE_SENSOR", "EXT_STORAGE_CHARGE_SENSOR", "EXT_PV_SENSOR"):',
        "    if _k46 not in globals():",
        '        globals()[_k46] = ""',
    ]
    # 2) Zaehler lesen
    i = find(L, '_mfull = self.get_state(POWER_SENSOR["entity"], attribute="all") or {}')
    L[i] = L[i].replace('POWER_SENSOR["entity"]', '(globals().get("GRID_SENSOR") or POWER_SENSOR.get("entity"))') + "  # FIX 46"
    i = find(L, 'if POWER_SENSOR.get("invert"):')
    L[i] = L[i].replace('POWER_SENSOR.get("invert")', 'globals().get("GRID_SENSOR_INVERT", POWER_SENSOR.get("invert"))') + "  # FIX 46"
    assert L[i + 1].strip() == "_mval = -_mval"
    b = ind(L[i])
    L[i + 2:i + 2] = [
        b + "# FIX 46: zweiter Speicher (z. B. Hybrid-Wechselrichter) herausrechnen",
        b + "_ext_dis = self._ext_power(globals().get('EXT_STORAGE_DISCHARGE_SENSOR'))",
        b + "_ext_chg = self._ext_power(globals().get('EXT_STORAGE_CHARGE_SENSOR'))",
        b + "if not str(globals().get('EXT_STORAGE_CHARGE_SENSOR') or '').strip() and _ext_dis < 0:",
        b + "    _ext_chg, _ext_dis = -_ext_dis, 0.0  # ein gemeinsamer Sensor: + Entladen, - Laden",
        b + "if _ext_dis or _ext_chg:",
        b + "    _ext_pv = self._ext_power(globals().get('EXT_PV_SENSOR'))",
        b + "    _mval += max(0.0, _ext_dis) - max(0.0, _ext_chg - max(0.0, _ext_pv))",
    ]
    # 3) Hilfsmethode
    i = find(L, "def _fmt_ts(self, ts):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _ext_power(self, eid):",
        b + '    """FIX 46: Leistung eines optionalen Sensors in W (kW wird umgerechnet), 0.0 wenn leer/unlesbar."""',
        b + "    eid = str(eid or '').strip()",
        b + "    if not eid:",
        b + "        return 0.0",
        b + "    try:",
        b + "        full = self.get_state(eid, attribute='all') or {}",
        b + "        v = float(full.get('state'))",
        b + "        unit = str((full.get('attributes') or {}).get('unit_of_measurement') or 'W').strip().lower()",
        b + "        return v * 1000.0 if unit == 'kw' else v",
        b + "    except Exception:",
        b + "        return 0.0",
        "",
    ]
    # 4) BMS: Wartezeit ab Faelligkeit
    i = find(L, "if not bms.get('charging_since'):")
    assert "_wait_s = float(globals().get('BMS_CARE_PV_WAIT_H', 48)) * 3600" in L[i + 3]
    assert "if (now_ts - float(bms.get('charging_since') or now_ts)) < _wait_s:" in L[i + 4]
    b = ind(L[i])
    L[i:i + 5] = [
        b + "_wait_s = float(globals().get('BMS_CARE_PV_WAIT_H', 48)) * 3600",
        b + "_due_ts = float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400",
        b + "if now_ts < _due_ts + _wait_s:  # FIX 46: 'max. Warten auf Sonne' zaehlt ab Faelligkeit, danach ohne PV durchladen",
    ]
    i = find(L, "verify_logs.append(f\"{name}: BMS CHARGE set={_w}W\" + (' (PV-Ueberschuss)' if _budget is not None else '') + f\" bis SoC>={FULL_CHARGE_SOC}%\")")
    L[i] = L[i].replace("else ''", "else ' (Wartezeit auf Sonne abgelaufen - ohne PV)'")
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix46_mqtt(src):
    if '"GRID_SENSOR"' in src:
        return src
    L = src.split("\n")
    i = find(L, '("READ_ONLY",', startswith=True)
    L[i + 1:i + 1] = [
        ind(L[i]) + "# FIX 46: Zaehler & zweiter Speicher",
        ind(L[i]) + '("GRID_SENSOR",                "text",   "Netz-Zaehler (Sensor)",                       None, None, None, None, "mdi:transmission-tower", "config"),',
        ind(L[i]) + '("GRID_SENSOR_INVERT",         "switch", "Netz-Zaehler: Vorzeichen drehen",             None, None, None, None, "mdi:swap-vertical",      "config"),',
        ind(L[i]) + '("EXT_STORAGE_DISCHARGE_SENSOR", "text", "Zweiter Speicher: Entladeleistung (Sensor)",  None, None, None, None, "mdi:battery-arrow-down-outline", "config"),',
        ind(L[i]) + '("EXT_STORAGE_CHARGE_SENSOR",  "text",   "Zweiter Speicher: Ladeleistung (Sensor)",     None, None, None, None, "mdi:battery-arrow-up-outline",   "config"),',
        ind(L[i]) + '("EXT_PV_SENSOR",              "text",   "Zweiter Speicher: PV-Leistung (Sensor)",      None, None, None, None, "mdi:solar-power",        "config"),',
    ]
    i = find(L, "v = str(raw).strip()[:64]")
    b = ind(L[i])
    L[i:i + 1] = [
        b + "v = str(raw).strip()[:128 if key.endswith('_SENSOR') else 64]",
        b + "if key.endswith('_SENSOR'):  # FIX 46: Entity-ID oder leer",
        b + "    return v if (v == '' or re.match(r'^[a-z_]+\\.[a-z0-9_]+$', v)) else str(self.g.get(key) or '')",
    ]
    i = find(L, 'cfg.update({"min": 0, "max": 64, "mode": "text"})')
    L[i] = L[i].replace('"max": 64', '"max": 128 if key.endswith("_SENSOR") else 64')
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix46_dashboard(cfg):
    """FIX 46: Konfiguration -> Karte 'Zaehler & zweiter Speicher' + Hilfetext."""
    if "text.marstek_grid_sensor" in json.dumps(cfg):
        return cfg
    card = {"type": "entities", "show_header_toggle": False, "state_color": True,
            "grid_options": {"columns": "full"},
            "entities": [{"type": "custom:fold-entity-row",
                          "head": {"type": "text", "name": "Zähler & zweiter Speicher", "text": " "},
                          "padding": 12,
                          "entities": [
                              {"entity": "text.marstek_grid_sensor", "name": "Netz-Zähler (Sensor)"},
                              {"entity": "switch.marstek_grid_sensor_invert", "name": "Vorzeichen drehen"},
                              {"type": "section", "label": "Zweiter Speicher (z. B. Hybrid-Wechselrichter) – leer lassen, wenn keiner da ist"},
                              {"entity": "text.marstek_ext_storage_discharge_sensor", "name": "Entladeleistung (Sensor)"},
                              {"entity": "text.marstek_ext_storage_charge_sensor", "name": "Ladeleistung (Sensor)"},
                              {"entity": "text.marstek_ext_pv_sensor", "name": "PV-Leistung (Sensor)"},
                          ]}]}
    helpc = {"type": "markdown", "content": EXT_HELP_MD}
    for v in cfg.get("views", []):
        if v.get("path") != "konfiguration":
            continue
        cards = (v.get("sections") or [{}])[0].get("cards", [])
        pos = None
        for i, c in enumerate(cards):
            if c.get("type") == "markdown" and "Verbindung (IP-Adressen" in c.get("content", ""):
                pos = i + 1
        if pos is None:
            pos = 3
        cards[pos:pos] = [card, helpc]
        break
    return cfg


METER_HELP_MD = """**Zähler-Überwachung:** Kommt vom Netz-Zähler länger kein neuer Wert, weiß die Steuerung nicht mehr, was am Hausanschluss passiert – dann gehen die Batterien zur Sicherheit auf **STOP**. **STOP wenn Zählerwert älter als** legt fest, nach wie vielen Sekunden das passiert (Standard 60 s, 0 = Prüfung aus).

Manche Zähler (z. B. EcoTracker, Shelly, Tibber Pulse) schicken nur alle paar Sekunden oder nur bei einer Änderung einen neuen Wert. Unter **Zähler-Info** siehst du, wie oft wirklich neue Werte kommen, die längste Pause und wie oft heute schon wegen eines alten Werts gestoppt wurde. Steht dort ein **Vorschlag**, stelle die Wartezeit auf diesen Wert. Bitte nicht viel höher als nötig: Fällt der Zähler wirklich aus, regelt die Steuerung so lange mit dem letzten Wert weiter.

"""


def fix47_control(src):
    """FIX 47: Zaehler-Info-Sensor (Alter, Abstand neuer Werte, STOPs, Vorschlag fuer METER_MAX_AGE_S)."""
    if "FIX 47" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 46:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 47: 'STOP wenn Zaehlerwert aelter als' (METER_MAX_AGE_S) ist im Dashboard einstellbar; "
                                "sensor.marstek_meter_info zeigt Alter des Zaehlerwerts, typischen Abstand neuer Werte, "
                                "STOPs der letzten 24 h und einen Vorschlag fuer die Wartezeit.")
    i = find(L, '_mage = (time.time() - _mts) if _mts > 0 else 0.0')
    b = ind(L[i])
    L[i + 1:i + 1] = [
        b + "# FIX 47: Abstand neuer Zaehlerwerte merken (fuer die Zaehler-Info im Dashboard)",
        b + "if _mts > 0 and _mts != getattr(self, '_meter_prev_ts', 0.0):",
        b + "    if getattr(self, '_meter_prev_ts', 0.0) > 0 and 0 < _mts - self._meter_prev_ts < 3600:",
        b + "        self._meter_intervals = (getattr(self, '_meter_intervals', []) + [_mts - self._meter_prev_ts])[-40:]",
        b + "    self._meter_prev_ts = _mts",
    ]
    i = find(L, "self._meter_last_good = (_mval, time.time())")
    L.insert(i + 1, ind(L[i])[:-4] + "self._meter_info(_mage, _mmax_age)  # FIX 47")
    i = find(L, 'self.log(f"Zaehler nicht lesbar ({e}){\' -> Batterien auf STOP\' if self._meter_stale else \'\'}", level="WARNING")')
    L.insert(i + 1, ind(L[i]) + "self._meter_info(None, _mmax_age)  # FIX 47")
    i = find(L, "def _ext_power(self, eid):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _meter_info(self, age, max_age):",
        b + "    \"\"\"FIX 47: sensor.marstek_meter_info fuers Dashboard - nur Anzeige, aendert nichts an der Regelung.\"\"\"",
        b + "    try:",
        b + "        now = time.time()",
        b + "        stale = bool(getattr(self, '_meter_stale', False))",
        b + "        st = [g for g in getattr(self, '_meter_stops', []) if now - g[0] < 86400]  # [Beginn, laengstes Alter]",
        b + "        if stale and not getattr(self, '_meter_stale_prev', False):",
        b + "            st.append([now, 0.0])",
        b + "        if stale and st and age is not None:",
        b + "            st[-1][1] = max(st[-1][1], float(age))",
        b + "        self._meter_stops, self._meter_stale_prev = st, stale",
        b + "        iv = sorted(x for x in getattr(self, '_meter_intervals', []) if x <= 900)",
        b + "        avg = int(round(sum(iv) / len(iv))) if iv else None",
        b + "        mx = int(round(max([iv[-1] if iv else 0.0] + [g[1] for g in st]))) or None",
        b + "        sug = None",
        b + "        if mx and mx <= 300 and (len(iv) >= 5 or st):",
        b + "            sug = int(max(60, -(-int(mx * 1.3) // 30) * 30))",
        b + "        ma = int(round(float(max_age or 0)))",
        b + "        if ma <= 0:",
        b + "            status = 'Pruefung aus'",
        b + "        elif stale:",
        b + "            status = 'STOP - Zaehlerwert zu alt'",
        b + "        elif mx and mx > 300:",
        b + "            status = f'Zaehler faellt zeitweise aus (bis {mx} s) - Zaehler bzw. WLAN pruefen'",
        b + "        elif sug and sug > ma:",
        b + "            status = f'OK - Vorschlag: Wartezeit auf {sug} s stellen'",
        b + "        else:",
        b + "            status = 'OK'",
        b + "        self.set_state('sensor.marstek_meter_info', state=(str(int(round(age))) if age is not None else 'unknown'), attributes={",
        b + "            'friendly_name': 'Zaehler: Alter des letzten Werts', 'unit_of_measurement': 's',",
        b + "            'icon': 'mdi:timer-outline', 'status': status, 'avg_interval_s': avg, 'max_gap_s': mx,",
        b + "            'stop_after_s': ma, 'stops_24h': len(st), 'suggested_s': sug, 'samples': len(iv),",
        b + "            'meter': str(globals().get('GRID_SENSOR') or POWER_SENSOR.get('entity') or '')})",
        b + "    except Exception as e:",
        b + "        self.log(f'ZAEHLER-INFO: {e}')",
        "",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix47_mqtt(src):
    if '"METER_MAX_AGE_S"' in src:
        return src
    L = src.split("\n")
    i = find(L, '("GRID_SENSOR_INVERT",', startswith=True)
    L.insert(i + 1, ind(L[i]) + '("METER_MAX_AGE_S",            "number", "STOP wenn Zaehlerwert aelter als",           0, 900, 5, "s", "mdi:timer-alert-outline", "config"),  # FIX 47')
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix47_dashboard(cfg):
    """FIX 47: Karte 'Zaehler & zweiter Speicher' um Wartezeit und Zaehler-Info ergaenzen, Hilfetext."""
    if "sensor.marstek_meter_info" in json.dumps(cfg):
        return cfg
    for v in cfg.get("views", []):
        if v.get("path") != "konfiguration":
            continue
        for c in (v.get("sections") or [{}])[0].get("cards", []):
            for r in c.get("entities") or []:
                if isinstance(r, dict) and r.get("type") == "custom:fold-entity-row" \
                        and (r.get("head") or {}).get("name") == "Zähler & zweiter Speicher":
                    ents = r["entities"]
                    pos = next(k for k, e in enumerate(ents) if e.get("entity") == "switch.marstek_grid_sensor_invert") + 1
                    ents[pos:pos] = [
                        {"entity": "number.marstek_meter_max_age_s", "name": "STOP wenn Zählerwert älter als"},
                        {"type": "section", "label": "Zähler-Info (nur Anzeige)"},
                        {"entity": "sensor.marstek_meter_info", "name": "Alter des letzten Zählerwerts"},
                        {"type": "attribute", "entity": "sensor.marstek_meter_info", "attribute": "status", "name": "Zustand"},
                        {"type": "attribute", "entity": "sensor.marstek_meter_info", "attribute": "avg_interval_s",
                         "name": "Neue Werte im Schnitt alle", "suffix": " s"},
                        {"type": "attribute", "entity": "sensor.marstek_meter_info", "attribute": "max_gap_s",
                         "name": "Längste Pause", "suffix": " s"},
                        {"type": "attribute", "entity": "sensor.marstek_meter_info", "attribute": "stops_24h",
                         "name": "STOPs wegen altem Wert (24 h)"},
                        {"type": "attribute", "entity": "sensor.marstek_meter_info", "attribute": "suggested_s",
                         "name": "Vorschlag Wartezeit", "suffix": " s"},
                    ]
            if c.get("type") == "markdown" and "Zähler &amp; zweiter Speicher: Was bedeutet das?" in c.get("content", ""):
                a = "Änderungen wirken sofort."
                assert a in c["content"]
                c["content"] = c["content"].replace(a, METER_HELP_MD + a, 1)
        break
    assert "sensor.marstek_meter_info" in json.dumps(cfg), "Zaehler-Karte nicht gefunden"
    return cfg


LIMIT_HELP_MD = """

**Grenzwerte** (je Venus): **max. Ladeleistung** und **max. Entladeleistung** begrenzen, was die Steuerung dieser Batterie höchstens vorgibt (Venus E: bis 2500 W). **Min-SoC**: darunter wird nicht weiter entladen. **Max-SoC**: darüber wird nicht weiter geladen (die BMS-Pflege lädt trotzdem bis „Vollladung bis“). **Kapazität**: nutzbare Größe der Batterie in kWh – damit verteilt die Steuerung die Leistung passend auf mehrere Batterien. Änderungen wirken ab dem nächsten Regelzyklus, die Verbindung wird dafür **nicht** neu aufgebaut. Ist Min-SoC nicht kleiner als Max-SoC, bleiben die alten Werte."""

LIMIT_ROWS = (("max_charge_w", "max. Ladeleistung"), ("max_discharge_w", "max. Entladeleistung"),
              ("min_soc", "Min-SoC"), ("max_soc", "Max-SoC"), ("capacity_kwh", "Kapazität"))


def fix48_control(src):
    """FIX 48: Grenzwerte je Batterie (BATTERYn_MAX_CHARGE_W ...) aus dem Dashboard in BATTERY_CONFIG uebernehmen."""
    if "FIX 48" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 47:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 48: Grenzwerte je Batterie (max. Lade-/Entladeleistung, Min-/Max-SoC, Kapazitaet) im Dashboard "
                                "aenderbar (BATTERYn_MAX_CHARGE_W, _MAX_DISCHARGE_W, _MIN_SOC, _MAX_SOC, _CAPACITY_KWH); ohne Treiber-Neuaufbau.")
    i = find(L, "CONN_PARAMS = (")
    j = i
    while L[j].strip() != ")":
        j += 1
    L[j + 1:j + 1] = [
        "# FIX 48: Grenzwerte je Batterie aus dem Dashboard -> Schluessel in BATTERY_CONFIG",
        "LIMIT_PARAMS = (",
        '    ("_MAX_CHARGE_W", "max_charge_w", int, 0, 10000),',
        '    ("_MAX_DISCHARGE_W", "max_discharge_w", int, 0, 10000),',
        '    ("_MIN_SOC", "min_soc", int, 0, 50),',
        '    ("_MAX_SOC", "max_soc", int, 50, 100),',
        '    ("_CAPACITY_KWH", "capacity_kwh", float, 0.5, 50),',
        ")",
    ]
    i = find(L, "def _sync_conn_params(self, name, initial=False):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _sync_limit_params(self, name, initial=False):",
        b + "    \"\"\"FIX 48: Grenzwerte aus dem Dashboard (BATTERYn_MAX_CHARGE_W usw.) in die Batterie-Config uebernehmen.\"\"\"",
        b + "    cfg = self.batteries.get(name)",
        b + "    if cfg is None:",
        b + "        return False",
        b + "    new = {}",
        b + "    for suffix, ckey, typ, lo, hi in LIMIT_PARAMS:",
        b + "        gkey = name.upper() + suffix",
        b + "        if gkey not in globals():",
        b + "            continue",
        b + "        try:",
        b + "            val = typ(max(lo, min(hi, float(globals()[gkey]))))",
        b + "        except (ValueError, TypeError):",
        b + "            continue",
        b + "        if typ is float:",
        b + "            val = round(val, 2)",
        b + "        if cfg.get(ckey) is None or typ(cfg.get(ckey)) != val:",
        b + "            new[ckey] = val",
        b + "    if not new:",
        b + "        return False",
        b + "    mn = new.get('min_soc', cfg.get('min_soc', 0))",
        b + "    mx = new.get('max_soc', cfg.get('max_soc', 100))",
        b + "    if int(mn) >= int(mx):",
        b + "        if not initial and getattr(self, '_limit_warned', {}).get(name) != (mn, mx):",
        b + "            self.log(f'[{name}] GRENZEN: Min-SoC {mn} % ist nicht kleiner als Max-SoC {mx} % - SoC-Werte bleiben', level='WARNING')",
        b + "            self.__dict__.setdefault('_limit_warned', {})[name] = (mn, mx)",
        b + "        new.pop('min_soc', None)",
        b + "        new.pop('max_soc', None)",
        b + "    cfg.update(new)",
        b + "    if new and not initial:",
        b + "        self.log(f\"[{name}] GRENZEN geaendert: {', '.join(f'{k}={v}' for k, v in new.items())}\")",
        b + "    return bool(new)",
        "",
    ]
    i = find(L, "self._sync_conn_params(name, initial=True)  # FIX 36")
    L.insert(i + 1, ind(L[i]) + "self._sync_limit_params(name, initial=True)  # FIX 48")
    i = find(L, "self._sync_conn_params(_n)  # FIX 36")
    L.insert(i + 1, ind(L[i]) + "self._sync_limit_params(_n)  # FIX 48")
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix49_control(src):
    """FIX 49: Energiezaehler (kWh) nur plausibel an HA geben - kein Ruecksprung, kein Riesensprung."""
    if "FIX 49" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 48:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 49: Gesamt-Energiezaehler (kWh) werden nur plausibel an Home Assistant gegeben: kein Ruecksprung, "
                                "kein Sprung groesser als physikalisch moeglich (z. B. vertauschte Modbus-Antworten). Ein abweichender "
                                "Wert wird erst uebernommen, wenn er mind. 10 min stabil bleibt. Sonst zaehlte HA Hunderte kWh 'heute'.")
    i = find(L, "def _set_ha_sensor(self, entity_id, value, unit=\"\", attrs=None):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _kwh_guard(self, eid, v):",
        b + "    \"\"\"FIX 49: Rueckgabe des Werts, der an HA gehen darf, oder None (verwerfen).\"\"\"",
        b + "    now = time.time()",
        b + "    if v < 0:",
        b + "        return None",
        b + "    last = self.__dict__.setdefault('_kwh_last', {})",
        b + "    pend = self.__dict__.setdefault('_kwh_pend', {})",
        b + "    if eid not in last:",
        b + "        try:",
        b + "            cur = float(self.get_state(eid))",
        b + "        except Exception:",
        b + "            cur = None",
        b + "        if cur is None or cur <= 0 or cur != cur:",
        b + "            last[eid] = (v, now)",
        b + "            return v",
        b + "        last[eid] = (cur, now)",
        b + "    lv, lt = last[eid]",
        b + "    max_up = 0.05 + max(15.0, now - lt) * 12.0 / 3600.0  # hoechstens 12 kW seit dem letzten guten Wert",
        b + "    if lv - 0.2 <= v <= lv + max_up:  # API rundet auf 0,1 kWh, Modbus auf 0,01 kWh",
        b + "        pend.pop(eid, None)",
        b + "        v = max(v, lv)",
        b + "        last[eid] = (v, now)",
        b + "        return v",
        b + "    pv = pend.get(eid)",
        b + "    if pv and pv[0] - 0.05 <= v <= pv[0] + 0.05 + (now - pv[1]) * 12.0 / 3600.0:",
        b + "        if now - pv[1] >= 600 and pv[2] >= 4:",
        b + "            self.log(f'ENERGIE: {eid} neuer Zaehlerstand {v} kWh uebernommen (vorher {lv} kWh, 10 min stabil)', level='WARNING')",
        b + "            pend.pop(eid, None)",
        b + "            last[eid] = (v, now)",
        b + "            return v",
        b + "        pend[eid] = (pv[0], pv[1], pv[2] + 1)",
        b + "        return None",
        b + "    self.log(f'ENERGIE: {eid} unplausibel {v} kWh (letzter guter Wert {lv} kWh) - verworfen', level='WARNING')",
        b + "    pend[eid] = (v, now, 1)",
        b + "    return None",
        "",
    ]
    i = find(L, "def _set_ha_sensor(self, entity_id, value, unit=\"\", attrs=None):")
    j = find(L, "if value is None:", i)
    assert j - i < 5
    k = j + 1
    while L[k].strip() != "return":
        k += 1
    bb = ind(L[j])
    L[k + 1:k + 1] = [
        bb + "if str(unit).strip().lower() == 'kwh' and 'total' in str(entity_id):  # FIX 49 (nur Gesamtzaehler)",
        bb + "    try:",
        bb + "        value = self._kwh_guard(entity_id, float(value))",
        bb + "    except (TypeError, ValueError):",
        bb + "        return",
        bb + "    if value is None:",
        bb + "        return",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix48_mqtt(src):
    if "LIMIT_FIELDS" in src:
        return src
    L = src.split("\n")
    i = find(L, "CONN_FIELDS = [")
    j = i
    while L[j].strip() != "]":
        j += 1
    L[j + 1:j + 1] = [
        "# FIX 48: Grenzwerte je Batterie (Suffix, Typ, Name, min, max, step, Einheit, Icon, Schluessel in BATTERY_CONFIG)",
        "LIMIT_FIELDS = [",
        '    ("_MAX_CHARGE_W", "number", "max. Ladeleistung", 0, 10000, 50, "W", "mdi:battery-arrow-up", "max_charge_w"),',
        '    ("_MAX_DISCHARGE_W", "number", "max. Entladeleistung", 0, 10000, 50, "W", "mdi:battery-arrow-down", "max_discharge_w"),',
        '    ("_MIN_SOC", "number", "Min-SoC", 0, 50, 1, "%", "mdi:battery-10", "min_soc"),',
        '    ("_MAX_SOC", "number", "Max-SoC", 50, 100, 1, "%", "mdi:battery", "max_soc"),',
        '    ("_CAPACITY_KWH", "number", "Kapazitaet", 0.5, 50, 0.1, "kWh", "mdi:battery-high", "capacity_kwh"),',
        "]",
    ]
    i = find(L, 'params.insert(k, (pkey, ftyp, f"Venus {idx} {flabel}", flo, fhi, fstep, None, ficon, "config"))')
    b = ind(L[i])  # Ebene innerhalb der CONN_FIELDS-Schleife
    ob = b[:-4]
    L[i + 2:i + 2] = [
        ob + "# FIX 48: Grenzwerte je Batterie",
        ob + "for suffix, ftyp, flabel, flo, fhi, fstep, funit, ficon, fsrc in LIMIT_FIELDS:",
        ob + "    pkey = n.upper() + suffix",
        ob + "    if pkey not in self.g:",
        ob + "        v = ((self.g.get('BATTERY_CONFIG') or {}).get(n) or {}).get(fsrc)",
        ob + "        if v is None:",
        ob + "            continue",
        ob + "        self.g[pkey] = round(float(v), 2) if isinstance(fstep, float) else int(round(float(v)))",
        ob + "    params.insert(k, (pkey, ftyp, f'Venus {idx} {flabel}', flo, fhi, fstep, funit, ficon, 'config'))",
        ob + "    k += 1",
    ]
    assert L[i + 1].strip() == "k += 1", L[i + 1]
    i = find(L, "for _sfx, _t, *_r in CONN_FIELDS:  # FIX 36")
    L[i] = L[i].replace("in CONN_FIELDS:  # FIX 36", "in CONN_FIELDS + LIMIT_FIELDS:  # FIX 36/48")
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix48_dashboard(cfg):
    """FIX 48: je Venus-Faltbereich Abschnitt 'Grenzwerte' + Hilfetext."""
    if re.search(r"marstek_battery\d+_max_charge_w", json.dumps(cfg)):
        return cfg
    n_done = 0
    for v in cfg.get("views", []):
        if v.get("path") != "konfiguration":
            continue
        for c in (v.get("sections") or [{}])[0].get("cards", []):
            for r in c.get("entities") or []:
                lab = str(((r.get("head") or {}).get("label")) or "") if isinstance(r, dict) else ""
                if r.get("type") == "custom:fold-entity-row" if isinstance(r, dict) else False:
                    m = re.match(r"^Venus (\d+)$", lab)
                    if m and any("_api_port" in str(e.get("entity", "")) for e in r.get("entities", [])):
                        n = m.group(1)
                        r["entities"] += [{"type": "section", "label": "Grenzwerte"}] + [
                            {"entity": f"number.marstek_battery{n}_{k}", "name": nm} for k, nm in LIMIT_ROWS]
                        n_done += 1
            if c.get("type") == "markdown" and "Verbindung (IP-Adressen und Ports): Was bedeutet das?" in c.get("content", ""):
                a = "\n</details>"
                assert c["content"].rstrip().endswith("</details>")
                c["content"] = c["content"].rstrip()[:-len("</details>")].rstrip() + LIMIT_HELP_MD + "\n</details>"
        break
    assert n_done >= 1, "keine Venus-Faltbereiche gefunden"
    return cfg


CELL_REGS = (
    '    # FIX 50: hoechste / niedrigste Zellspannung (nur Modbus, Venus E alle Generationen; Register 37007/37008, 1 mV)',
    '    "cell_voltage_max":         {"enabled": True,  "addr": 37007, "cnt": 1, "type": "u16", "scale": 0.001,  "unit": "V",   "prec": 3, "name": "Cell Voltage Max"},',
    '    "cell_voltage_min":         {"enabled": True,  "addr": 37008, "cnt": 1, "type": "u16", "scale": 0.001,  "unit": "V",   "prec": 3, "name": "Cell Voltage Min"},',
    '',
)
CELL_CFG = """# FIX 50: BMS-Pflege vorziehen, wenn die Zellspannungen weit auseinanderliegen (nur bei Modbus-Anbindung,
# die WLAN-API liefert keine Zellspannungen). Unterschied hoechste - niedrigste Zelle in mV, 0 = aus.
# Vorgezogen wird nur mit PV-Ueberschuss (nie aus dem Netz) und fruehestens BMS_CARE_CELL_MIN_DAYS nach der
# letzten Vollladung; der Unterschied muss BMS_CARE_CELL_DELTA_MIN_S Sekunden am Stueck anliegen.
BMS_CARE_CELL_DELTA_MV = 100
BMS_CARE_CELL_DELTA_MIN_S = 1800
BMS_CARE_CELL_MIN_DAYS = 3
"""
CELL_HELP_MD = """
- **Vorziehen ab Zell-Unterschied** (nur bei **Modbus**): Liegen höchste und niedrigste Zellspannung einer Venus mindestens 30 min so weit auseinander, startet ihre Vollladung schon vor den 10 Tagen – damit das BMS die Zellen ausgleichen kann. Nur mit PV-Überschuss, nie aus dem Netz. 0 = aus, Standard 100 mV. Die WLAN-API liefert keine Zellspannungen, dort gibt es diese Funktion nicht.
- **Vorziehen frühestens nach:** so viele Tage nach der letzten Vollladung (Standard 3)."""


def fix50_config(src):
    if "BMS_CARE_CELL_DELTA_MV" in src:
        return src
    L = src.split("\n")
    i = find(L, '"temp_cell_min":', startswith=True)
    L[i + 1:i + 1] = [""] + list(CELL_REGS)[:-1]
    i = find(L, "BMS_CARE_PV_WAIT_H = 48")
    L[i + 1:i + 1] = CELL_CFG.rstrip("\n").split("\n")
    out = "\n".join(L)
    compile(out, "pv_config.py", "exec")
    return out


def fix50_control(src):
    """FIX 50: Zellspannungs-Unterschied je Batterie (nur Modbus) anzeigen und BMS-Pflege vorziehen."""
    if "FIX 50" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 49:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 50: hoechste/niedrigste Zellspannung (Modbus 37007/37008) und Unterschied in mV je Batterie "
                                "(sensor.marstekvenus_N_cell_voltage_delta, nur Modbus); liegt der Unterschied >= BMS_CARE_CELL_DELTA_MV "
                                "laenger an, wird die BMS-Pflege vorgezogen (nur mit PV-Ueberschuss, fruehestens nach BMS_CARE_CELL_MIN_DAYS).")
    i = find(L, 'for _k46 in ("EXT_STORAGE_DISCHARGE_SENSOR", "EXT_STORAGE_CHARGE_SENSOR", "EXT_PV_SENSOR"):')
    L[i + 3:i + 3] = [
        "",
        "# FIX 50: Zellspannungen (nur Modbus) - Standardwerte und Register, falls eine alte pv_config.py sie nicht kennt",
        'for _k50, _v50 in (("BMS_CARE_CELL_DELTA_MV", 100), ("BMS_CARE_CELL_DELTA_MIN_S", 1800), ("BMS_CARE_CELL_MIN_DAYS", 3)):',
        "    if _k50 not in globals():",
        "        globals()[_k50] = _v50",
        "try:",
        '    HA_REGMAP.setdefault("cell_voltage_max", {"enabled": True, "addr": 37007, "cnt": 1, "type": "u16", "scale": 0.001, "unit": "V", "prec": 3, "name": "Cell Voltage Max"})',
        '    HA_REGMAP.setdefault("cell_voltage_min", {"enabled": True, "addr": 37008, "cnt": 1, "type": "u16", "scale": 0.001, "unit": "V", "prec": 3, "name": "Cell Voltage Min"})',
        "except NameError:",
        "    pass",
    ]
    # Modbus-Export: Zell-Register, die das Geraet nicht kennt, 6 h ueberspringen statt die Verbindung als schlecht zu markieren
    i = find(L, 'keys = [k for k, v in HA_REGMAP.items() if v.get("enabled", True)]')
    b = ind(L[i])
    L.insert(i + 1, b + "_cu = getattr(self, '_cell_unsupported', {}).get(battery_name, 0)  # FIX 50")
    L.insert(i + 2, b + "keys = [k for k in keys if not (k.startswith('cell_voltage_') and time.time() < _cu)]")
    i = find(L, "raw = self._read_reg_value(client, cfg[\"unit\"], spec, battery_name)")
    j = find(L, "if raw is None:", i)
    bb = ind(L[j])
    L[j:j] = [
        bb + "if raw is None and key.startswith('cell_voltage_'):  # FIX 50: Register evtl. nicht vorhanden",
        bb + "    self.__dict__.setdefault('_cell_unsupported', {})[battery_name] = time.time() + 6 * 3600",
        bb + "    self.log(f'[{battery_name}] Zellspannung (Register {spec.get(\"addr\")}) nicht lesbar - 6 h pausiert', level='WARNING')",
        bb + "    continue",
    ]
    i = find(L, "self._export_computed_sensors(battery_name, key, val)")
    L.insert(i + 1, ind(L[i]) + "if key.startswith('cell_voltage_'):  # FIX 50")
    L.insert(i + 2, ind(L[i]) + "    self._cell_store(battery_name, key, val)")
    # Methoden
    i = find(L, "def _kwh_guard(self, eid, v):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _cell_store(self, name, key, val):",
        b + "    \"\"\"FIX 50: Zellspannung merken und Unterschied auswerten (nur plausible Werte 2,0-4,5 V).\"\"\"",
        b + "    try:",
        b + "        v = float(val)",
        b + "    except (TypeError, ValueError):",
        b + "        return",
        b + "    if not 2.0 <= v <= 4.5:",
        b + "        return",
        b + "    now = time.time()",
        b + "    c = self.__dict__.setdefault('_cell_v', {}).setdefault(name, {})",
        b + "    c['max' if key.endswith('_max') else 'min'] = (v, now)",
        b + "    if 'max' not in c or 'min' not in c or abs(c['max'][1] - c['min'][1]) > 600:",
        b + "        return",
        b + "    d = int(round((c['max'][0] - c['min'][0]) * 1000))",
        b + "    if d < 0:",
        b + "        return",
        b + "    c['delta'] = (d, now)",
        b + "    lim = int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0)",
        b + "    if lim > 0 and d >= lim:",
        b + "        c.setdefault('high_since', now)",
        b + "    elif lim <= 0 or d < lim * 0.8:",
        b + "        c.pop('high_since', None)",
        "",
        b + "def _cell_early(self, name):",
        b + "    \"\"\"FIX 50: True, wenn der Zell-Unterschied lange genug ueber der Grenze liegt (frische Modbus-Werte).\"\"\"",
        b + "    c = (getattr(self, '_cell_v', {}) or {}).get(name) or {}",
        b + "    hs, dl = c.get('high_since'), c.get('delta')",
        b + "    if not hs or not dl or time.time() - dl[1] > 900 or int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0) <= 0:",
        b + "        return False",
        b + "    return time.time() - hs >= float(globals().get('BMS_CARE_CELL_DELTA_MIN_S', 1800))",
        "",
        b + "def _cell_publish(self, name):",
        b + "    \"\"\"FIX 50: sensor.<prefix>_N_cell_voltage_delta fuers Dashboard (nur Anzeige).\"\"\"",
        b + "    try:",
        b + "        c = (getattr(self, '_cell_v', {}) or {}).get(name) or {}",
        b + "        dl = c.get('delta')",
        b + "        lim = int(globals().get('BMS_CARE_CELL_DELTA_MV', 100) or 0)",
        b + "        fresh = bool(dl) and time.time() - dl[1] <= 900",
        b + "        if fresh:",
        b + "            state = str(dl[0])",
        b + "            if self._cell_early(name):",
        b + "                status = 'hoch - BMS-Pflege wird vorgezogen (sobald PV-Ueberschuss da ist)'",
        b + "            elif c.get('high_since'):",
        b + "                status = f\"hoch seit {int((time.time() - c['high_since']) // 60)} min\"",
        b + "            else:",
        b + "                status = 'OK' if lim > 0 else 'OK (Vorziehen aus)'",
        b + "        else:",
        b + "            state = 'unknown'",
        b + "            status = ('nur mit Modbus-Anbindung verfuegbar' if isinstance(self._drivers.get(name), ApiDriver)",
        b + "                      else 'noch keine Werte')",
        b + "        idx = BATTERY_META.get(name, {}).get('idx', name)",
        b + "        self.set_state(self._ha_entity(name, 'cell_voltage_delta'), state=state, attributes={",
        b + "            'friendly_name': f'MarstekVenus {idx} Cell Voltage Delta', 'unit_of_measurement': 'mV',",
        b + "            'icon': 'mdi:battery-heart-variant', 'status': status, 'limit_mv': lim,",
        b + "            'max_v': (c.get('max') or (None,))[0] if fresh else None,",
        b + "            'min_v': (c.get('min') or (None,))[0] if fresh else None})",
        b + "    except Exception as e:",
        b + "        self.log(f'ZELLSPANNUNG {name}: {e}')",
        "",
    ]
    # Anzeige je Zyklus
    i = find(L, "# 5) Mode-Wechsel")
    b = ind(L[i])
    L[i:i] = [
        b + "# FIX 50: Zellspannungs-Unterschied je Batterie (nur Modbus)",
        b + "for n in self.batteries.keys():",
        b + "    self._cell_publish(n)",
        "",
    ]
    # BMS-Pflege vorziehen
    i = find(L, "if last > 0 and (now_ts - last) < (FULL_CHARGE_INTERVAL_DAYS * 86400):")
    b = ind(L[i])
    assert L[i + 1].strip() == "continue"
    L[i + 1:i + 2] = [
        b + "    # FIX 50: vorziehen bei grossem Zell-Unterschied - nur Modbus, nur mit PV-Ueberschuss, Mindestabstand",
        b + "    if not (self._cell_early(name) and bool(getattr(self, 'pv_surplus', False))",
        b + "            and (now_ts - last) >= float(globals().get('BMS_CARE_CELL_MIN_DAYS', 3)) * 86400):",
        b + "        continue",
        b + "    self.log(f\"BMS {name}: Zellspannungen {((getattr(self, '_cell_v', {}) or {}).get(name) or {}).get('delta', ('?',))[0]} mV \"",
        b + "             f\"auseinander -> BMS-Pflege vorgezogen (nur mit PV-Ueberschuss)\")",
        b + "    return name",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix50_mqtt(src):
    if '"BMS_CARE_CELL_DELTA_MV"' in src:
        return src
    L = src.split("\n")
    i = find(L, '("BMS_CARE_PV_WAIT_H",', startswith=True)
    L[i + 1:i + 1] = [
        ind(L[i]) + '("BMS_CARE_CELL_DELTA_MV",    "number", "BMS-Pflege vorziehen ab Zell-Unterschied", 0, 500, 5, "mV", "mdi:battery-heart-variant", None),  # FIX 50',
        ind(L[i]) + '("BMS_CARE_CELL_MIN_DAYS",    "number", "BMS-Pflege vorziehen fruehestens nach", 1, 30, 1, "d",  "mdi:calendar-clock",       None),  # FIX 50',
    ]
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix50_dashboard(cfg):
    """FIX 50: Batterien-Tab: Zell-Unterschied; Konfiguration/BMS-Pflege: zwei Einstellungen + Hilfe."""
    if "cell_voltage_delta" in json.dumps(cfg):
        return cfg
    hits = {"bat": 0, "cfg": 0, "help": 0}

    def walk(o):
        if isinstance(o, dict):
            if isinstance(o.get("entities"), list):
                new = []
                for r in o["entities"]:
                    new.append(r)
                    if not isinstance(r, dict):
                        continue
                    m = re.match(r"^sensor\.bms_battery(\d+)_phase$", r.get("entity") or "")
                    if m and r.get("type") == "attribute" and r.get("attribute") == "next_due":
                        e = f"sensor.marstekvenus_{m.group(1)}_cell_voltage_delta"
                        new += [
                            {"entity": e, "name": "Zellspannung: Unterschied", "icon": "mdi:battery-heart-variant"},
                            {"type": "attribute", "entity": e, "attribute": "status", "name": "Zellen", "icon": "mdi:information-outline"},
                            {"type": "attribute", "entity": e, "attribute": "max_v", "name": "Höchste Zelle", "suffix": " V", "icon": "mdi:arrow-up-bold"},
                            {"type": "attribute", "entity": e, "attribute": "min_v", "name": "Niedrigste Zelle", "suffix": " V", "icon": "mdi:arrow-down-bold"},
                        ]
                        hits["bat"] += 1
                    if r.get("entity") == "number.marstek_bms_care_pv_wait_h":
                        new += [{"entity": "number.marstek_bms_care_cell_delta_mv", "name": "Vorziehen ab Zell-Unterschied (nur Modbus)"},
                                {"entity": "number.marstek_bms_care_cell_min_days", "name": "Vorziehen frühestens nach"}]
                        hits["cfg"] += 1
                o["entities"] = new
            if o.get("type") == "markdown" and "BMS-Pflege: Tabelle Jetzt/Original" in str(o.get("content", "")):
                a = "(Standard 48 h)."
                if a in o["content"]:
                    o["content"] = o["content"].replace(a, a + CELL_HELP_MD, 1)
                    hits["help"] += 1
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(cfg)
    assert hits["bat"] >= 1 and hits["cfg"] == 1 and hits["help"] == 1, hits
    return cfg


BMS_HELP_MD = """<details><summary>ℹ️ <b>Was bedeutet das?</b></summary>

**Wozu?** Lithium-Akkus zeigen den Ladestand nur dann genau an und gleichen ihre Zellen nur dann aus, wenn sie ab und zu **ganz voll** werden. Den Ausgleich (Zell-Balancing) macht das BMS in der Venus selbst – die Steuerung sorgt nur dafür, dass jede Batterie regelmäßig dorthin kommt. Es wird höchstens **eine Batterie pro Tag** voll geladen, die anderen regeln ganz normal weiter.

**So läuft eine BMS-Pflege ab:**
1. **Fällig:** nach „Vollladung alle“ (Standard 10 Tage) – oder früher, wenn die Zellspannungen zu weit auseinanderliegen (nur Modbus, siehe unten).
2. **Laden:** Mit „Sonne bevorzugen“ nur mit PV-Überschuss; bei Wolken oder nachts macht sie Pause. Ist sie nach „Max. Warten auf Sonne“ immer noch nicht voll, lädt sie auch aus dem Netz.
3. **Voll:** Erreicht der Ladestand „Vollladung bis“, gilt die Pflege als erledigt. Die Batterie bekommt dann **45 min Ruhe** (kein Laden, kein Entladen) – in dieser Zeit gleicht das BMS die Zellen aus.
4. **Danach** wird sie noch **30 min nicht entladen**, dann regelt sie wieder normal mit.

**Die Einstellungen:**
- **BMS-Pflege (Vollladung):** ein/aus. Empfehlung: an.
- **Vollladung bis** (Standard 99 %): ab diesem Ladestand gilt die Batterie als voll. **Warum 99 und nicht 100 %?** Manche Akkus zeigen 100 % erst sehr spät oder gar nicht an, auch wenn sie praktisch voll sind. Mit 99 % kann die Pflege nicht „hängen bleiben“. 100 % ist möglich und gleicht etwas gründlicher aus – zeigt die Batterie aber nie 100 %, läuft die Pflege so lange weiter und blockiert die Pflege der anderen.
- **Vollladung alle:** wie oft, in Tagen (10 Tage ≈ 3× im Monat).
- **Sonne bevorzugen:** an = eine fällige Vollladung lädt nur mit PV-Überschuss.
- **Max. Warten auf Sonne:** kommt so lange keine Sonne, wird **aus dem Netz** voll geladen (Standard 48 h ab Fälligkeit).
- **Vorziehen ab Zell-Unterschied** (nur bei **Modbus**): Liegen höchste und niedrigste Zellspannung einer Venus mindestens 30 min so weit auseinander, startet ihre Pflege schon früher – nur mit PV-Überschuss, nie aus dem Netz. 0 = aus, Standard 100 mV. Über die WLAN-API gibt es keine Zellspannungen, dort entfällt das.
- **Vorziehen frühestens nach:** so viele Tage nach der letzten Vollladung (Standard 3).

Wann eine Batterie zuletzt dran war und wann sie wieder fällig ist, steht im Tab **Batterien** bei der jeweiligen Venus.
</details>"""


def fix51_dashboard(cfg):
    """FIX 51: Hilfetext BMS-Pflege klarer (Ablauf, Warum 99 %)."""
    hits = [0]

    def walk(o):
        if isinstance(o, dict):
            c = o.get("content")
            if o.get("type") == "markdown" and isinstance(c, str) and "BMS-Pflege: Tabelle Jetzt/Original" in c \
                    and "Warum 99 und nicht 100" not in c:
                a = c.find("<details><summary>ℹ️ <b>Was bedeutet das?</b></summary>")
                assert a > 0
                b = c.rfind("</details>")
                b = c.rfind("</details>", 0, b)
                assert b > a
                o["content"] = c[:a] + BMS_HELP_MD + c[b + len("</details>"):]
                hits[0] += 1
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(cfg)
    return cfg


UPD_METHODS = r"""
    # ------------------------------------------------ FIX 52: Software-Update
    def _ver_tuple(self, v):
        return tuple(int(x) for x in re.findall(r"\d+", str(v or "0"))[:4])

    def _cur_version(self):
        return str(self.g.get("PV_CONTROL_VERSION") or DEVICE.get("sw_version") or "0")

    def _publish_update_state(self):
        c = self.client
        if not c or not self.connected:
            return
        u = getattr(self, "_upd", None) or {}
        cur = self._cur_version()
        st = {"installed_version": cur, "latest_version": u.get("latest") or cur,
              "title": "Marstek Venus Steuerung",
              "release_url": u.get("url") or f"https://github.com/{UPDATE_REPO}/releases",
              "release_summary": (u.get("summary") or "")[:250], "in_progress": bool(u.get("busy"))}
        c.publish(f"{BASE}/software/state", json.dumps(st), qos=1, retain=True)

    def _update_loop(self):
        time.sleep(45)
        while getattr(self, "_upd_run", False):
            if self.g.get("UPDATE_CHECK", True):
                self._update_check()
            for _ in range(int(float(UPDATE_CHECK_H) * 120)):
                if not getattr(self, "_upd_run", False):
                    return
                time.sleep(30)

    def _update_check(self):
        import urllib.request
        try:
            r = urllib.request.Request(f"https://api.github.com/repos/{UPDATE_REPO}/releases/latest",
                                       headers={"User-Agent": "marstek-venus-steuerung",
                                                "Accept": "application/vnd.github+json"})
            d = json.loads(urllib.request.urlopen(r, timeout=20).read().decode("utf-8"))
            tag = str(d.get("tag_name") or "").lstrip("vV").strip()
            if not tag:
                return
            cur = self._cur_version()
            notes = self._update_notes(tag, cur)
            body = re.sub(r"[#*`>]", "", notes or str(d.get("body") or "")).strip()
            u = dict(getattr(self, "_upd", None) or {})
            u.update(latest=tag, url=d.get("html_url"), summary=body[:250], checked=time.time())
            self._upd = u
            try:
                self.app.set_state("sensor.marstek_update_notes", state=str(tag), attributes={
                    "friendly_name": "Marstek Update: Was ist neu", "icon": "mdi:text-box-outline",
                    "installed": cur, "latest": tag, "url": d.get("html_url") or "",
                    "notes": notes or "Keine Beschreibung gefunden - siehe Link."})
            except Exception as e:
                self.log(f"UPDATE: Hinweis nicht gesetzt ({e})")
            if self._ver_tuple(tag) > self._ver_tuple(cur) and getattr(self, "_upd_logged", None) != tag:
                self._upd_logged = tag
                self.log(f"UPDATE: neue Version {tag} verfuegbar (installiert {cur})")
            self._publish_update_state()
        except Exception as e:
            self.log(f"UPDATE: Pruefung fehlgeschlagen ({e})")

    def _update_notes(self, tag, cur):
        # Abschnitte aus CHANGELOG.md der neuen Version, die neuer sind als die installierte (max. ca. 3000 Zeichen)
        import urllib.request
        try:
            r = urllib.request.Request(f"https://raw.githubusercontent.com/{UPDATE_REPO}/v{tag}/CHANGELOG.md",
                                       headers={"User-Agent": "marstek-venus-steuerung"})
            text = urllib.request.urlopen(r, timeout=20).read().decode("utf-8")
        except Exception:
            return ""
        out = []
        for part in re.split(r"(?m)^## ", text)[1:]:
            head = part.split("\n", 1)[0].strip()
            m = re.match(r"([0-9][0-9.]*)", head)
            if not m or self._ver_tuple(m.group(1)) <= self._ver_tuple(cur):
                continue
            rest = part.split("\n", 1)[1] if "\n" in part else ""
            rest = re.sub(r"(?m)^### ", "", rest).strip()
            out.append(f"**Version {head}**\n{rest}")
        txt = "\n\n".join(out)
        return txt[:3000] + (" ..." if len(txt) > 3000 else "")

"""

UPD_HELP_MD = """<details><summary>ℹ️ <b>Software-Update: Wie geht das?</b></summary>

Die Steuerung schaut alle 6 Stunden auf GitHub nach, ob es eine neue Version gibt. Ist eine da, erscheint oben in der **Übersicht** ein Hinweis mit allen Neuerungen seit deiner Version, und Home Assistant zeigt sie zusätzlich unter **Einstellungen → Updates**.

**Einspielen:** im Terminal von Home Assistant den Installationsbefehl ausführen (steht im Hinweis). Alle Einstellungen, Batterien und Dashboard-Werte bleiben erhalten, vorher werden Sicherungen angelegt.

Abschalten der Prüfung: in `pv_config.py` die Zeile `UPDATE_CHECK = False` ergänzen.
</details>"""


def fix52_mqtt(src):
    """FIX 52: Update-Entitaet (MQTT update, nur Anzeige), GitHub-Pruefung alle 6 h, Neuerungen."""
    if "FIX 52" in src:
        return src
    L = src.split("\n")
    i = find(L, 'CONFIRM_S = 15')
    L[i + 1:i + 1] = [
        "# FIX 52: Software-Update von GitHub (nur Hinweis, Einspielen per Installationsbefehl)",
        'UPDATE_REPO = "NeueMedienBecker/homeassistant-marstek-venus"',
        "UPDATE_CHECK_H = 6",
    ]
    # Discovery: Update-Entitaet nach den Buttons
    i = find(L, "for key, name, icon, cat in BUTTONS:")
    j = i
    while "}), qos=1, retain=True)" not in L[j]:
        j += 1
    b = ind(L[i])
    L[j + 1:j + 1] = [
        b + "# FIX 52: Software-Update (HA zeigt es auch unter Einstellungen -> Updates)",
        b + 'client.publish(f"{DISCOVERY}/update/{BASE}/software/config", json.dumps({',
        b + '    "name": "Software", "unique_id": "marstek_ctrl_software", "default_entity_id": "update.marstek_software",',
        b + '    "state_topic": f"{BASE}/software/state", "availability": avail, "device": DEVICE,',
        b + '    "entity_picture": "https://brands.home-assistant.io/_/hassio/icon.png",',
        b + '}), qos=1, retain=True)',
    ]
    # nach dem Verbinden: Zustand senden, Pruef-Thread einmal starten
    i = find(L, 'client.publish(f"{BASE}/admin_status/state", self.status, qos=1, retain=True)')
    b = ind(L[i])
    L[i + 1:i + 1] = [
        b + "self._publish_update_state()  # FIX 52",
        b + "if not getattr(self, '_upd_thread', None):",
        b + "    self._upd_run = True",
        b + "    self._upd_thread = threading.Thread(target=self._update_loop, daemon=True)",
        b + "    self._upd_thread.start()",
    ]
    # Stop: Thread beenden
    i = find(L, "def stop(self):")
    L.insert(i + 1, ind(L[i]) + "    self._upd_run = False  # FIX 52")
    # Methoden vor apply_pending
    i = find(L, "def apply_pending(self):")
    L[i:i] = UPD_METHODS.strip("\n").split("\n") + [""]
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix52_dashboard(cfg):
    """FIX 52: Uebersicht: Hinweis mit Neuerungen, wenn ein Update da ist; Verwaltung: Zeile + Hilfe."""
    if "update.marstek_software" in json.dumps(cfg):
        return cfg
    banner = {"type": "conditional",
              "conditions": [{"condition": "state", "entity": "update.marstek_software", "state": "on"}],
              "card": {"type": "vertical-stack", "cards": [
                  {"type": "markdown", "content": (
                      "### 🔔 Update verfügbar\n"
                      "Version **{{ state_attr('update.marstek_software','latest_version') }}** ist da "
                      "(installiert: {{ state_attr('update.marstek_software','installed_version') }}). "
                      "[Release auf GitHub]({{ state_attr('update.marstek_software','release_url') }})\n\n"
                      "{{ state_attr('sensor.marstek_update_notes','notes') or '' }}\n\n"
                      "**Einspielen:** im Terminal von Home Assistant denselben Befehl wie bei der Installation "
                      "ausführen – alle Einstellungen bleiben erhalten, vorher werden Sicherungen angelegt:\n\n"
                      "`curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash`")},
              ]}}
    hits = 0
    for v in cfg.get("views", []):
        secs = v.get("sections") or []
        if v.get("path") == "uebersicht" and secs:
            cards = secs[0].setdefault("cards", [])
            pos = 1 if cards and cards[0].get("type") == "heading" else 0
            cards.insert(pos, banner)
            hits += 1
        if v.get("path") == "konfiguration" and len(secs) > 1:
            cards = secs[1].get("cards", [])
            for c in cards:
                if c.get("type") == "entities":
                    c.setdefault("entities", []).append({"entity": "update.marstek_software", "name": "Software-Update"})
                    hits += 1
                    break
            cards.append({"type": "markdown", "content": UPD_HELP_MD})
    assert hits == 2, hits
    return cfg


FULL100_BULLET = ("- **Vollladung bis** (Standard 100 %): ab diesem Ladestand gilt die Batterie als voll. Bei 100 % gleicht "
                  "das BMS am gründlichsten aus. Manche Akkus zeigen 100 % aber erst sehr spät oder gar nicht an – damit die "
                  "Pflege dann nicht „hängen bleibt“, gilt die Batterie auch als voll, wenn sie **mindestens 99 %** zeigt und "
                  "**30 min lang trotz Ladebefehl keine Leistung mehr aufnimmt**.")


def fix53_config(src):
    if "FIX 53" in src:
        return src
    L = src.split("\n")
    i = find(L, "FULL_CHARGE_SOC = 99")
    L[i] = "FULL_CHARGE_SOC = 100"
    L[i + 1:i + 1] = [
        "# FIX 53: Zeigt eine Batterie trotz Ladebefehl nie 100 %, gilt sie als voll, sobald sie mindestens 99 % zeigt",
        "# und BMS_FULL_STALL_S Sekunden lang weniger als BMS_FULL_STALL_W Watt aufnimmt (kein Haengenbleiben).",
        "BMS_FULL_STALL_S = 1800",
        "BMS_FULL_STALL_W = 60",
    ]
    out = "\n".join(L)
    compile(out, "pv_config.py", "exec")
    return out


def fix53_control(src):
    """FIX 53: BMS-Pflege: 'voll', wenn >= 99 % und trotz Ladebefehl lange keine Leistung mehr (100 % nie angezeigt)."""
    if "FIX 53" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 52:", startswith=True) if any(x.strip().startswith("- FIX 52:") for x in L) else find(L, "- FIX 50:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 53: Vollladung bis 100 % (Standard); zeigt eine Batterie nie 100 %, gilt sie als voll, wenn sie >= 99 % "
                                "zeigt und BMS_FULL_STALL_S lang trotz Ladebefehl < BMS_FULL_STALL_W aufnimmt.")
    i = find(L, "# FIX 18: Haltephase laeuft bis holding_until, auch wenn der SoC kurz auf 98 % faellt")
    b = ind(L[i])
    L[i:i] = [
        b + "# FIX 53: 100 % nie angezeigt? -> voll, wenn >= 99 % und die Batterie trotz Ladebefehl nichts mehr aufnimmt",
        b + "_st53 = states.get(name, {}) or {}",
        b + "if bms.get('phase') == 'charging' and min(int(FULL_CHARGE_SOC), 99) <= soc < int(FULL_CHARGE_SOC):",
        b + "    _cmd53 = (getattr(self, '_bms_last_cmd', None) or {}).get(name)",
        b + "    try:",
        b + "        _pw53 = max(abs(float(_st53.get('ac') or 0)), abs(float(_st53.get('bp') or 0)))",
        b + "    except (TypeError, ValueError):",
        b + "        _pw53 = 9999.0",
        b + "    if _cmd53 and str(_cmd53[0]).startswith('charge') and _pw53 < float(globals().get('BMS_FULL_STALL_W', 60)):",
        b + "        bms.setdefault('stall_since', now_ts)",
        b + "        if now_ts - float(bms['stall_since']) >= float(globals().get('BMS_FULL_STALL_S', 1800)):",
        b + "            write_logs.append(f\"{name}: BMS {soc}% und seit {int((now_ts - float(bms['stall_since'])) // 60)} min \"",
        b + "                              f\"keine Ladeleistung mehr -> gilt als voll\")",
        b + "            soc = int(FULL_CHARGE_SOC)",
        b + "    else:",
        b + "        bms.pop('stall_since', None)",
        b + "else:",
        b + "    bms.pop('stall_since', None)",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix53_dashboard(cfg):
    """FIX 53: Hilfetext 'Vollladung bis' auf 100 % mit Absicherung."""
    def walk(o):
        if isinstance(o, dict):
            c = o.get("content")
            if isinstance(c, str) and "- **Vollladung bis** (Standard 99 %)" in c:
                a = c.index("- **Vollladung bis** (Standard 99 %)")
                e = c.index("\n", a)
                o["content"] = c[:a] + FULL100_BULLET + c[e:]
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)
    walk(cfg)
    return cfg


def fix55_control(src):
    """FIX 55: BMS-Zustand fuer das Dashboard (pausiert/wartet auf Sonne, ab wann aus dem Netz); set_state mit replace=True."""
    if "FIX 55" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 53:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 55: BMS-Pflege zeigt im Dashboard, ob sie mit PV-Ueberschuss laedt, pausiert und auf Sonne wartet "
                                "(mit Uhrzeit, ab wann aus dem Netz geladen wird) oder in der Haltephase ist.")
    i = find(L, "self._bms_cmd(name, driver, 'pause', 0, 0, 0)")
    L.insert(i, ind(L[i]) + "bms['pv_wait'], bms['from_grid'] = True, False  # FIX 55")
    i = find(L, "ok = self._bms_cmd(name, driver, 'charge%d' % _w, 1, _w, 0)")
    L.insert(i, ind(L[i]) + "bms['pv_wait'], bms['from_grid'] = False, _budget is None  # FIX 55")
    i = find(L, "'status': ({'idle': 'Bereitschaft', 'charging': 'Vollladung laeuft (nur mit PV-Ueberschuss)', 'hold': 'Haltephase (voll)'}", startswith=True)
    assert L[i + 1].strip() == ".get(bms.get('phase', 'idle'), bms.get('phase', 'idle'))),"
    b = ind(L[i])
    L[i:i + 2] = [b + "'status': self._bms_status_text(n, bms),  # FIX 55",
                  b + "'grid_from': self._fmt_ts(self._bms_grid_from(bms)) if self._bms_grid_from(bms) else '',"]
    i = find(L, "def _bms_next_due_ts(self, name):")
    b = ind(L[i])
    L[i:i] = [
        b + "def _bms_grid_from(self, bms):",
        b + "    \"\"\"FIX 55: ab wann eine faellige Vollladung ohne Sonne aus dem Netz laedt (0 = sofort/nie relevant).\"\"\"",
        b + "    if not globals().get('BMS_CARE_REQUIRE_PV', True):",
        b + "        return 0",
        b + "    last = float(bms.get('last_full_ts') or 0)",
        b + "    if last <= 0:",
        b + "        return 0",
        b + "    return last + FULL_CHARGE_INTERVAL_DAYS * 86400 + float(globals().get('BMS_CARE_PV_WAIT_H', 48)) * 3600",
        "",
        b + "def _bms_status_text(self, name, bms):",
        b + "    \"\"\"FIX 55: Zustand der BMS-Pflege in Worten fuer das Dashboard.\"\"\"",
        b + "    ph = bms.get('phase', 'idle')",
        b + "    if ph == 'hold':",
        b + "        return f\"Voll – Haltephase (Zellausgleich) bis {time.strftime('%H:%M', time.localtime(float(bms.get('holding_until') or 0)))} Uhr\"",
        b + "    if ph != 'charging':",
        b + "        return 'Bereitschaft'",
        b + "    gf = self._bms_grid_from(bms)",
        b + "    if bms.get('pv_wait'):",
        b + "        t = 'Pausiert – wartet auf Sonne (PV-Überschuss).'",
        b + "        if gf and gf > time.time():",
        b + "            t += f\" Spätestens ab {time.strftime('%d.%m. %H:%M', time.localtime(gf))} Uhr lädt sie aus dem Netz.\"",
        b + "        return t",
        b + "    if bms.get('from_grid'):",
        b + "        return 'Vollladung läuft aus dem Netz (Wartezeit auf Sonne abgelaufen)'",
        b + "    return 'Vollladung läuft mit PV-Überschuss'",
        "",
    ]
    # Attribute ersetzen statt mischen (sonst landen interne Felder wie last_changed/context in den Attributen)
    for a, bnew in (
        ('self.set_state(f"sensor.bms_{n}_phase", state=bms.get("phase", "idle"), attributes={',
         'self.set_state(f"sensor.bms_{n}_phase", state=bms.get("phase", "idle"), replace=True, attributes={'),
        ("self.set_state(entity_id, state=state, attributes=a)", "self.set_state(entity_id, state=state, replace=True, attributes=a)"),
        ("self.set_state('sensor.marstek_meter_info', state=", "self.set_state('sensor.marstek_meter_info', replace=True, state="),
        ("self.set_state(self._ha_entity(name, 'cell_voltage_delta'), state=state, attributes={",
         "self.set_state(self._ha_entity(name, 'cell_voltage_delta'), state=state, replace=True, attributes={"),
    ):
        k = [j for j, x in enumerate(L) if a in x]
        assert len(k) == 1, a
        L[k[0]] = L[k[0]].replace(a, bnew)
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


BMS_BANNER_MD = ("{% for n in range(1, 9) %}{% set e = 'sensor.bms_battery' ~ n ~ '_phase' %}"
                 "{% if states(e) in ['charging', 'hold'] %}**🔋 Venus {{ n }} – BMS-Pflege:** {{ state_attr(e, 'status') }}"
                 "{% if state_attr(e, 'grid_from') and states(e) == 'charging' %}{% endif %}\n\n{% endif %}{% endfor %}")


def fix55_dashboard(cfg):
    """FIX 55: Batterien-Tab: Zeile 'BMS-Pflege Zustand'; Uebersicht: Hinweis, solange eine BMS-Pflege laeuft."""
    if "BMS-Pflege Zustand" in json.dumps(cfg, ensure_ascii=False):
        return cfg
    hits = {"row": 0, "banner": 0}

    def walk(o):
        if isinstance(o, dict):
            if isinstance(o.get("entities"), list):
                new = []
                for r in o["entities"]:
                    new.append(r)
                    if isinstance(r, dict) and r.get("type") == "attribute" and r.get("attribute") == "next_due" \
                            and re.match(r"^sensor\.bms_battery\d+_phase$", r.get("entity") or ""):
                        new.append({"type": "attribute", "entity": r["entity"], "attribute": "status",
                                    "name": "BMS-Pflege Zustand", "icon": "mdi:information-outline"})
                        hits["row"] += 1
                o["entities"] = new
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)
    walk(cfg)
    banner = {"type": "conditional",
              "conditions": [{"condition": "or", "conditions": [
                  {"condition": "state", "entity": f"sensor.bms_battery{n}_phase", "state": ["charging", "hold"]}
                  for n in range(1, 7)]}],
              "card": {"type": "markdown", "content": BMS_BANNER_MD}}
    for v in cfg.get("views", []):
        secs = v.get("sections") or []
        if v.get("path") == "uebersicht" and secs:
            cards = secs[0].setdefault("cards", [])
            pos = 1 if cards and cards[0].get("type") == "heading" else 0
            cards.insert(pos, banner)
            hits["banner"] += 1
    assert hits["row"] >= 1 and hits["banner"] == 1, hits
    return cfg


def fix56_control(src):
    """FIX 56: Haupt-/Batterieschalter aus -> Steuerung an Marstek zurueckgeben (Modbus 42000=21947, API Modus Auto)."""
    if "FIX 56" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 55:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 56: Hauptschalter oder 'Venus N vom Script gesteuert' aus -> die Batterie wird gestoppt und an Marstek "
                                "zurueckgegeben (Modbus: RS485-Steuerung aus, API: Modus Auto) und nicht wieder uebernommen, "
                                "bis der Schalter wieder an ist. Wartezeit der Notladung im Dashboard einstellbar.")
    # Basisklasse: release_control + released
    i = find(L, "class BatteryDriver:")
    j = find(L, "def init_control(self):", i)
    assert L[j + 2].strip() == "return 21930, False"
    b = ind(L[j])
    L[j + 3:j + 3] = [
        "",
        b + "released = False  # FIX 56: True = Steuerung an Marstek zurueckgegeben",
        "",
        b + "def release_control(self):",
        b + "    \"\"\"FIX 56: Steuerung an Marstek zurueckgeben. True = erledigt.\"\"\"",
        b + "    self.released = True",
        b + "    return True",
    ]
    # Modbus: Steuerung nicht wieder einschalten, solange freigegeben
    i = find(L, "def _ensure_ctrl(self, client, current_ctrl):")
    b = ind(L[i + 1])
    L[i + 1:i + 1] = [b + "if getattr(self, 'released', False):  # FIX 56: an Marstek zurueckgegeben -> nicht wieder uebernehmen",
                      b + "    return current_ctrl, False"]
    i = find(L, 'if result["ctrl"] not in self.control_enabled_values:')
    L[i] = L[i].replace('if result["ctrl"] not in self.control_enabled_values:',
                        'if result["ctrl"] not in self.control_enabled_values and not getattr(self, "released", False):  # FIX 56')
    i = find(L, "class ModbusDriver(BatteryDriver):")
    j = find(L, "def init_control(self):", i)
    b = ind(L[j])
    L[j:j] = [
        b + "def release_control(self):",
        b + "    \"\"\"FIX 56: Sollwerte 0, Modus STOP, dann RS485-Steuerung aus (42000 = 21947) -> Marstek steuert selbst.\"\"\"",
        b + "    if globals().get(\"READ_ONLY\", False):",
        b + "        self.released = True",
        b + "        return True",
        b + "    client = self._mk_client()",
        b + "    if not client.connect():",
        b + "        self._drop_client()",
        b + "        return False",
        b + "    try:",
        b + "        self._write(client, self.registers[\"charge_set\"], 0)",
        b + "        time.sleep(0.03)",
        b + "        self._write(client, self.registers[\"discharge_set\"], 0)",
        b + "        time.sleep(0.03)",
        b + "        self._write(client, self.registers[\"mode\"], 0)",
        b + "        time.sleep(0.05)",
        b + "        ok = self._write(client, self.registers[\"control\"], 21947)",
        b + "        time.sleep(0.05)",
        b + "        regs = self._read(client, self.registers[\"control\"], 1)",
        b + "        if ok and regs and regs[0] not in self.control_enabled_values:",
        b + "            self.released = True",
        b + "            self.log(f\"[{self.name}] Modbus: Steuerung an Marstek zurueckgegeben ({regs[0]})\")",
        b + "            return True",
        b + "        return False",
        b + "    except Exception as e:",
        b + "        self.error(f\"[{self.name}] ModbusDriver.release_control: {e}\")",
        b + "        return False",
        b + "    finally:",
        b + "        try:",
        b + "            self._done(client)",
        b + "        except Exception:",
        b + "            pass",
        "",
    ]
    i = find(L, "class ApiDriver(BatteryDriver):")
    j = find(L, "def init_control(self):", i)
    b = ind(L[j])
    L[j:j] = [
        b + "def release_control(self):",
        b + "    \"\"\"FIX 56: 0 W, dann Modus Auto -> Marstek steuert wieder selbst (wie in der App eingestellt).\"\"\"",
        b + "    if globals().get(\"READ_ONLY\", False):",
        b + "        self.released = True",
        b + "        return True",
        b + "    self._send_power(0, \"(Rueckgabe an Marstek)\")",
        b + "    res = self._call(\"ES.SetMode\", {\"id\": 0, \"config\": {\"mode\": \"Auto\", \"auto_cfg\": {\"enable\": 1}}})",
        b + "    ok = isinstance(res, dict)",
        b + "    if ok:",
        b + "        self.released = True",
        b + "        self._last_power = 0",
        b + "        self._pending_power = None",
        b + "        self.invalidate_cache()",
        b + "        self.log(f\"[{self.name}] API: Steuerung an Marstek zurueckgegeben (Modus Auto)\")",
        b + "    else:",
        b + "        self._warn_throttled(\"release\", f\"[{self.name}] API: Modus Auto setzen fehlgeschlagen\")",
        b + "    return ok",
        "",
    ]
    # Treiberwechsel waehrend abgeschaltet -> neu freigeben
    i = find(L, "def _replace_driver(self, name, new_drv, init=True):")
    j = find(L, "self._drivers[name] = new_drv", i)
    L.insert(j + 1, ind(L[j]) + "getattr(self, '_dis_stopped', set()).discard(name)  # FIX 56: ggf. erneut an Marstek zurueckgeben")
    # Hauptschleife: freigegebene Batterie ist kein Fehler
    i = find(L, "if ctrl not in CONTROL_ENABLED_VALUES:")
    L[i] = L[i].replace("if ctrl not in CONTROL_ENABLED_VALUES:",
                        "if ctrl not in CONTROL_ENABLED_VALUES and not getattr(driver, 'released', False):  # FIX 56")
    # Schreiben: abgeschaltet -> zurueckgeben statt nur STOP; wieder an -> uebernehmen
    i = find(L, "if name in _disabled:  # FIX 29: einmal STOP, danach nicht mehr ansteuern")
    j = find(L, "_pw_add(name, _drv_d, int(self.last_distribution.get(name, 0)), 0, 0, 0)", i)
    assert L[j - 1].strip() == "if _drv_d is not None:"
    assert L[j + 1].strip() == 'write_logs.append(f"{name}: STOP (per Schalter abgeschaltet)")'
    assert L[j + 2].strip() == "self.last_distribution[name] = 0"
    assert L[j + 3].strip() == "_ds.add(name)"
    b = ind(L[j])
    L[j:j + 4] = [
        b + "if globals().get('RELEASE_ON_DISABLE', True):  # FIX 56: an Marstek zurueckgeben",
        b + "    if _drv_d.release_control():",
        b + "        write_logs.append(f\"{name}: per Schalter abgeschaltet -> STOP und an Marstek zurueckgegeben\")",
        b + "        self.last_distribution[name] = 0",
        b + "        _ds.add(name)",
        b + "    else:",
        b + "        write_logs.append(f\"{name}: Rueckgabe an Marstek fehlgeschlagen - neuer Versuch im naechsten Zyklus\")",
        b + "else:",
        b + "    _pw_add(name, _drv_d, int(self.last_distribution.get(name, 0)), 0, 0, 0)",
        b + "    write_logs.append(f\"{name}: STOP (per Schalter abgeschaltet)\")",
        b + "    self.last_distribution[name] = 0",
        b + "    _ds.add(name)",
    ]
    i = find(L, 'getattr(self, "_dis_stopped", set()).discard(name)', i)
    b = ind(L[i])
    L[i:i + 1] = [
        b + "if name in getattr(self, \"_dis_stopped\", set()):  # FIX 56: Schalter wieder an -> Steuerung uebernehmen",
        b + "    self._dis_stopped.discard(name)",
        b + "    _drv_r = self._drivers.get(name)",
        b + "    if _drv_r is not None and getattr(_drv_r, 'released', False):",
        b + "        _drv_r.released = False",
        b + "        try:",
        b + "            _cv56, _ch56 = _drv_r.init_control()",
        b + "            write_logs.append(f\"{name}: Schalter wieder an -> Steuerung uebernommen ({_cv56})\")",
        b + "        except Exception as _ex56:",
        b + "            self.error(f\"[{name}] Uebernahme nach Schalter an fehlgeschlagen: {_ex56}\")",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix56_config(src):
    if "FIX 56" in src:
        return src
    L = src.split("\n")
    i = find(L, "SOC_SAFETY_AFTER_HOURS = 48")
    L[i] = "SOC_SAFETY_AFTER_HOURS = 24  # FIX 56: Standard 24 h (vorher 48 h), im Dashboard einstellbar, 0 = sofort"
    i = find(L, "CONTROL_DISABLED_VALUES = {21947, 0}")
    L[i + 1:i + 1] = [
        "",
        "# FIX 56: Hauptschalter oder 'Venus N vom Script gesteuert' aus -> Batterie an Marstek zurueckgeben",
        "# (Modbus: Register 42000 = 21947, API: Modus Auto). Dann laesst sie sich wieder in der Marstek-App steuern.",
        "# False = altes Verhalten (nur einmal STOP, Steuerung bleibt beim Script).",
        "RELEASE_ON_DISABLE = True",
    ]
    out = "\n".join(L)
    compile(out, "pv_config.py", "exec")
    return out


def fix56_mqtt(src):
    if '"SOC_SAFETY_AFTER_HOURS"' in src:
        return src
    L = src.split("\n")
    i = find(L, '("SOC_SAFETY_CHARGE_W",', startswith=True)
    L.insert(i + 1, ind(L[i]) + '("SOC_SAFETY_AFTER_HOURS",    "number", "Notladung: Wartezeit unter Min-SoC", 0, 96, 1, "h",  "mdi:timer-outline",      None),  # FIX 56')
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


def fix56_dashboard(cfg):
    """FIX 56: Wartezeit der Notladung einstellbar; Hilfetexte: Ausschalten gibt an Marstek zurueck."""
    if "number.marstek_soc_safety_after_hours" in json.dumps(cfg, ensure_ascii=False):
        return cfg
    hits = {"row": 0, "tab": 0, "help": 0, "main": 0, "del": 0, "fixed": 0}
    MAIN_OLD = "Alle Batterien bekommen **einmal STOP** und bleiben dann im Leerlauf. Auch Notladung und BMS-Pflege pausieren. Wieder an = alles läuft normal weiter."
    MAIN_NEW = ("Alle Batterien werden gestoppt und **an Marstek zurückgegeben** (Modbus: RS485-Steuerung aus, WLAN/API: Modus „Automatik“). "
                "Sie arbeiten dann wieder so, wie es in der **Marstek-App** eingestellt ist – dort kann man z. B. auch von Hand laden. "
                "Notladung und BMS-Pflege pausieren. Wieder an = das Script übernimmt die Batterien wieder.\n"
                "- **Wichtig:** Pro Batterie nur **ein** Programm, das per Modbus steuert. Läuft zusätzlich eine andere Modbus-Integration "
                "(z. B. mit einem Schalter „RS485-Steuerung“), bitte diese deaktivieren – sonst greifen beide gleichzeitig zu.")
    BAT_OLD = "Sie bekommt einmal STOP, danach schickt das Script ihr nichts mehr."
    BAT_NEW = "Sie wird gestoppt und an Marstek zurückgegeben (wie beim Hauptschalter) und lässt sich dann in der Marstek-App steuern."
    DEL_OLD = "Die Batterie bekommt einmal STOP und wird dann in Ruhe gelassen."
    DEL_NEW = "Die Batterie wird gestoppt, an Marstek zurückgegeben (in der Marstek-App steuerbar) und dann in Ruhe gelassen."
    HELP_REPL = (
        ("Bleibt eine Batterie **48 Stunden** auf oder unter dem Min-SoC (z. B. im Winter), wird sie ein Stück nachgeladen.",
         "Bleibt eine Batterie die eingestellte **Wartezeit** (Standard 24 Stunden) auf oder unter dem Min-SoC (z. B. im Winter), wird sie ein Stück nachgeladen."),
        ("die 48-Stunden-Uhr startet.", "die Wartezeit läuft los."),
        ("- **Ziel-SoC:**", "- **Wartezeit unter Min-SoC:** So lange muss die Batterie auf oder unter dem Min-SoC sein, bevor nachgeladen wird. **0 = sofort.**\n- **Ziel-SoC:**"),
        ("nach den 48 h wird zuerst", "nach der Wartezeit wird zuerst"),
        ("(Standard 12 h, also spätestens nach 60 h insgesamt)", "(Standard 12 h, also spätestens Wartezeit + 12 h)"),
        ("('SOC_SAFETY_CHARGE_W','number','Notladeleistung','W'),",
         "('SOC_SAFETY_CHARGE_W','number','Notladeleistung','W'),('SOC_SAFETY_AFTER_HOURS','number','Wartezeit unter Min-SoC','h'),"),
    )

    def walk(o):
        if isinstance(o, dict):
            if isinstance(o.get("entities"), list):
                new = []
                for r in o["entities"]:
                    new.append(r)
                    if isinstance(r, dict) and r.get("entity") == "number.marstek_soc_safety_charge_w":
                        new.append({"entity": "number.marstek_soc_safety_after_hours", "name": "Wartezeit unter Min-SoC"})
                        hits["row"] += 1
                o["entities"] = new
            c = o.get("content")
            if isinstance(c, str):
                if "SoC-Schutz / Notladung: Tabelle Jetzt/Original" in c:
                    for a, bnew in HELP_REPL:
                        assert c.count(a) == 1, a
                        c = c.replace(a, bnew, 1)
                    hits["help"] += 1
                if MAIN_OLD in c:
                    c = c.replace(MAIN_OLD, MAIN_NEW, 1)
                    hits["main"] += 1
                if BAT_OLD in c:
                    c = c.replace(BAT_OLD, BAT_NEW, 1)
                if DEL_OLD in c:
                    c = c.replace(DEL_OLD, DEL_NEW, 1)
                    hits["del"] += 1
                if "| SoC-Schutz nach | `SOC_SAFETY_AFTER_HOURS` |" in c:
                    c = _re.sub(r"\| SoC-Schutz nach \| `SOC_SAFETY_AFTER_HOURS` \| \*\*[^|]*\*\* \|\n", "", c)
                    hits["fixed"] += 1
                o["content"] = c
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)
    walk(cfg)
    assert hits["row"] == 1 and hits["help"] == 1 and hits["main"] == 1, hits
    return cfg


UPD_NOTES_OLD = "{{ state_attr('sensor.marstek_update_notes','notes') or '' }}\n\n"
UPD_NOTES_NEW = ("{% set _n = state_attr('sensor.marstek_update_notes','notes') %}{% if _n %}"
                 "<details><summary>📋 <b>Was ist neu?</b> (zum Aufklappen antippen)</summary>\n\n"
                 "{{ _n }}\n\n</details>\n\n{% endif %}")


def fix57_dashboard(cfg):
    """FIX 57: Update-Hinweis in der Uebersicht: Neuerungen eingeklappt (details), Rest bleibt sichtbar."""
    if "<b>Was ist neu?</b>" in json.dumps(cfg, ensure_ascii=False):
        return cfg
    hits = {"n": 0}

    def walk(o):
        if isinstance(o, dict):
            c = o.get("content")
            if isinstance(c, str) and "### 🔔 Update verfügbar" in c and UPD_NOTES_OLD in c:
                o["content"] = c.replace(UPD_NOTES_OLD, UPD_NOTES_NEW, 1)
                hits["n"] += 1
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)
    walk(cfg)
    assert hits["n"] == 1, hits
    return cfg


def fix42_control(src):
    """FIX 42: Sensoren geloeschter Batterien beim Start aus Home Assistant entfernen."""
    if "FIX 42" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 40:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 42: Beim Start werden HA-Sensoren von Batterien, die es nicht mehr gibt "
                                "(Dashboard: Batterie loeschen), entfernt.")
    i = find(L, "self.last_distribution = {name: 0 for name in self.batteries}")
    b = ind(L[i])
    L[i:i] = [
        b + "# FIX 42: Sensoren geloeschter Batterien entfernen (sonst bleiben sie bis zum HA-Neustart sichtbar)",
        b + "try:",
        b + "    _keep = {str(int(BATTERY_META.get(_n, {}).get('idx', 0) or 0)) for _n in self.batteries}",
        b + "    _pat = re.compile(rf'^sensor\\.(?:{re.escape(HA_EXPORT_PREFIX)}_(\\d+)_|pv_battery(\\d+)_comm_mode$|bms_battery(\\d+)_phase$)')",
        b + "    for _eid in list((self.get_state() or {}).keys()):",
        b + "        _m = _pat.match(_eid)",
        b + "        if _m and (_m.group(1) or _m.group(2) or _m.group(3)) not in _keep:",
        b + "            try:",
        b + "                self.remove_entity(_eid)",
        b + "            except Exception:",
        b + "                pass",
        b + "except Exception as _e:",
        b + "    self.log(f'FIX 42: alte Sensoren nicht entfernt ({_e})', level='WARNING')",
        "",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix43_control(src):
    """FIX 43: Ladeleistung fuer BMS-Pflege / Notladung zaehlt nicht als Hausverbrauch - sonst entladen die
    anderen Batterien (oder ein fremder Speicher), um die ladende Batterie zu fuettern."""
    if "FIX 43" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 42:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 43: Ladeleistung von BMS-Pflege/Notladung wird aus dem Netzfluss herausgerechnet "
                                "(andere Batterien entladen nicht, um die ladende Batterie zu fuettern).")
    i = find(L, "ac_ctrl_now = 0.0")
    b = ind(L[i])
    L[i:i] = [
        b + "# FIX 43: Laden fuer BMS-Pflege / Notladung ist kein Hausverbrauch -> aus dem Netzfluss herausrechnen",
        b + "_care_charge = 0.0",
        b + "for _n in (bms_active | soc_safety_active):",
        b + "    _st = states.get(_n) or {}",
        b + "    if not _st.get('disabled') and not _st.get('err'):",
        b + "        _care_charge += min(0.0, float(_st.get('ac', 0) or 0))  # ac < 0 = Laden",
        b + "if _care_charge < 0:",
        b + "    grid_flow += _care_charge",
        "",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix44_control(src):
    """FIX 44: Unbekannte Batterie (last_full_ts == 0, z.B. nach Neuinstallation) startet die BMS-Vollladung
    nicht sofort aus dem Netz, sondern wartet wie jede faellige Batterie erst auf PV-Ueberschuss."""
    if "FIX 44" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 43:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 44: Nach einer Neuinstallation wartet die BMS-Pflege erst auf PV-Ueberschuss "
                                "(BMS_CARE_PV_WAIT_H), statt sofort aus dem Netz voll zu laden.")
    i = find(L, 'last = float(bms.get("last_full_ts") or 0)')
    # nur die Stelle in _bms_select_candidate (die erste nach "def _bms_select_candidate")
    j = find(L, "def _bms_select_candidate(self, states):", startswith=True)
    i = find(L, 'last = float(bms.get("last_full_ts") or 0)', start=j)
    b = ind(L[i])
    L[i + 1:i + 1] = [
        b + "if last <= 0 and name in self.bms_state:",
        b + "    # FIX 44: unbekannter Stand -> gilt als faellig, aber mit PV-Wartezeit (kein Sofort-Laden aus dem Netz)",
        b + "    last = now_ts - FULL_CHARGE_INTERVAL_DAYS * 86400",
        b + "    self.bms_state[name]['last_full_ts'] = last",
        b + "    self._save_bms_state()",
        b + "    self.log(f'BMS {name}: kein Vollladedatum bekannt -> faellig, wartet auf PV-Ueberschuss')",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def fix45_control(src):
    """FIX 45: BMS-Vollladung nur aus PV-Ueberschuss (Budget je Zyklus, sonst Pause); die anderen Batterien
    nehmen der BMS-Batterie den Ueberschuss nicht weg und entladen nicht fuer sie (A_set einseitig korrigiert)."""
    if "FIX 45" in src:
        return src
    L = src.split("\n")
    i = find(L, "- FIX 44:", startswith=True)
    L.insert(i + 1, ind(L[i]) + "- FIX 45: BMS-Pflege laedt nur mit dem PV-Ueberschuss, der gerade da ist (pausiert bei Wolken/nachts, "
                                "nach BMS_CARE_PV_WAIT_H ohne Abschluss mit voller Leistung); geregelte Batterien nehmen "
                                "der BMS-Batterie den Ueberschuss nicht weg und entladen nicht fuer sie.")
    # 1) echter Ueberschuss + einseitige A_set-Korrektur
    i = find(L, "grid_flow += _care_charge")
    assert L[i - 1].strip() == "if _care_charge < 0:"
    b = ind(L[i - 1])
    L[i] = L[i] + "  # FIX 45: nur noch fuer die PV-Erkennung"
    L[i - 1:i - 1] = [
        b + "# FIX 45: echter PV-Ueberschuss = -Netzfluss + alles, was gerade in Batterien geht",
        b + "_ctrl_charge = 0.0",
        b + "for _n, _st in states.items():",
        b + "    if _n in bms_active or _n in soc_safety_active or _st.get('disabled') or _st.get('err'):",
        b + "        continue",
        b + "    _ctrl_charge += min(0.0, float(_st.get('ac', 0) or 0))",
        b + "self._surplus_w = -float(actual_flow) - _ctrl_charge - _care_charge",
    ]
    i = find(L, "A_set = ac_ctrl_now + grid_flow")
    b = ind(L[i])
    L[i:i + 1] = [
        b + "A_set = ac_ctrl_now + actual_flow",
        b + "# FIX 45: andere Batterien entladen nicht fuer BMS-/Notladung - nehmen ihr aber auch den Ueberschuss nicht weg",
        b + "if _care_charge < 0 and A_set > 0:",
        b + "    A_set = max(A_set + _care_charge, 0.0)",
    ]
    # 2) _bms_apply: Budget / Pause
    i = find(L, 'ok = self._bms_cmd(name, driver, "charge%d" % int(BMS_FORCE_CHARGE_W), 1, BMS_FORCE_CHARGE_W, 0)')
    assert L[i - 1].strip() == "if driver:" and L[i + 1].strip() == "if ok:" and "BMS CHARGE set=" in L[i + 2]
    b = ind(L[i - 1])
    L[i - 1:i + 3] = [
        b + "# FIX 45: nur mit PV-Ueberschuss laden, sonst pausieren; nach BMS_CARE_PV_WAIT_H ohne Abschluss volle Leistung",
        b + "_w = int(BMS_FORCE_CHARGE_W)",
        b + "_budget = None",
        b + "if globals().get('BMS_CARE_REQUIRE_PV', True):",
        b + "    if not bms.get('charging_since'):",
        b + "        bms['charging_since'] = now_ts",
        b + "        self._save_bms_state()",
        b + "    _wait_s = float(globals().get('BMS_CARE_PV_WAIT_H', 48)) * 3600",
        b + "    if (now_ts - float(bms.get('charging_since') or now_ts)) < _wait_s:",
        b + "        _budget = float(getattr(self, '_surplus_w', 0.0) or 0.0)",
        b + "        _w = int(min(float(BMS_FORCE_CHARGE_W), max(0.0, _budget)) // 50 * 50)",
        b + "if driver:",
        b + "    if _w < int(globals().get('BMS_CARE_MIN_W', 150)):",
        b + "        self._bms_cmd(name, driver, 'pause', 0, 0, 0)",
        b + "        verify_logs.append(f\"{name}: BMS CHARGE pausiert - kein PV-Ueberschuss ({(_budget or 0):.0f}W), wartet\")",
        b + "    else:",
        b + "        ok = self._bms_cmd(name, driver, 'charge%d' % _w, 1, _w, 0)",
        b + "        if ok:",
        b + "            verify_logs.append(f\"{name}: BMS CHARGE set={_w}W\" + (' (PV-Ueberschuss)' if _budget is not None else '') + f\" bis SoC>={FULL_CHARGE_SOC}%\")",
    ]
    i = find(L, 'bms["last_full_ts"]   = now_ts')
    L.insert(i + 1, ind(L[i]) + "bms['charging_since'] = 0  # FIX 45")
    i = find(L, '"min_soc_since", "safety_done", "safety_active"):')
    L[i] = L[i].replace('"safety_active"):', '"safety_active", "charging_since"):')
    # 3) HA-Sensor: letzte und naechste BMS-Pflege als Attribute
    i = find(L, '"cooldown_until": self._fmt_ts(bms.get("cooldown_until")),')
    b = ind(L[i])
    L[i + 1:i + 1] = [
        b + "# FIX 45: fuer das Dashboard - wann zuletzt, wann wieder faellig",
        b + "'next_due': (self._fmt_ts(float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400)",
        b + "             if float(bms.get('last_full_ts') or 0) + FULL_CHARGE_INTERVAL_DAYS * 86400 > time.time()",
        b + "             else 'faellig - wartet auf PV-Ueberschuss'),",
        b + "'status': ({'idle': 'Bereitschaft', 'charging': 'Vollladung laeuft (nur mit PV-Ueberschuss)', 'hold': 'Haltephase (voll)'}",
        b + "           .get(bms.get('phase', 'idle'), bms.get('phase', 'idle'))),",
    ]
    out = "\n".join(L)
    compile(out, "pv_control.py", "exec")
    return out


def build_mqtt(src, version):
    L = src.split("\n")
    if any('"sw_version"' in x for x in L):
        return src
    i = find(L, '"model": "pv_control.py",')
    L.insert(i + 1, ind(L[i]) + f'"sw_version": "{version}",')
    out = "\n".join(L)
    compile(out, "pv_mqtt.py", "exec")
    return out


EXAMPLE_BATTERY = {
    "battery1": {
        "enabled": True,
        "comm_mode": "api",
        "modbus_host": "192.168.1.50",
        "api_host": "192.168.1.51",
        "port": 502,
        "unit": 1,
        "api_port": 30000,
        "api_cd_time": 300,
        "capacity_kwh": 5.12,
        "max_charge_w": 2500,
        "max_discharge_w": 2500,
        "min_soc": 11,
        "max_soc": 99,
    },
}


def build_config(src):
    s = H.config_set(src, {
        "BATTERY_CONFIG": EXAMPLE_BATTERY,
        "POWER_SENSOR": {"enabled": True, "entity": "sensor.netz_leistung", "invert": False},
        "HA_ENTITY_ID_MAP": {},
        "API_LOG_DIAG": False,
        "COMM_MODE_FALLBACK_ENABLE": False,
    })
    s = re.sub(r"(?m)^BATTERY[2-9]_ACTIVE = .*\n", "", s)
    s = s.replace("# pv_control_config.py\n", "# pv_config.py\n", 1)
    s = s.replace("in diesem Fall AC_SIGN_INVERT = True setzen).",
                  'in diesem Fall in POWER_SENSOR "invert": True setzen).')
    s = s.replace("POWER_SENSOR = {",
                  "# enabled: True | entity: Netz-Zaehler-Sensor (W oder kW) | invert: True = Vorzeichen drehen\n"
                  "POWER_SENSOR = {", 1)
    s = s.replace("HA_ENTITY_ID_MAP = {}",
                  "# Normalerweise leer lassen (nur fuer alte Installationen mit Entitaeten wie ..._2).\n"
                  "HA_ENTITY_ID_MAP = {}", 1)
    s = s.replace("COMM_MODE_FALLBACK_ENABLE = False  # FIX 30: aus, solange die Open API nicht antwortet",
                  "COMM_MODE_FALLBACK_ENABLE = False  # Ausweichweg wird je Batterie im Dashboard gewaehlt (Anbindung)")
    compile(s, "pv_config.py", "exec")
    return s


# Personen-/Anlagenbezogene Teile des eigenen Dashboards
GRID_SENSORS = ("sensor.netz_gefiltert", "sensor.stromzaehler_sml_aktuelle_wirkleistung")
DROP_ENTITIES = ("sensor.batterien_gesamt", "sensor.solar_produktion_insgesamt")
BATS_JINJA = ("{%- set bats = states.sensor | selectattr('entity_id', 'match', "
              "'sensor.marstekvenus_[0-9]+_battery_soc$') | map(attribute='entity_id') "
              "| map('regex_replace', '[^0-9]', '') | map('int') | sort | list -%}\n")


def build_dashboard(cfg):
    s = json.dumps(cfg, ensure_ascii=False)
    s = re.sub(r"(sensor\.marstekvenus_\d+_total_(?:dis)?charging_energy)_2(?![0-9a-z_])", r"\1", s)
    for g in GRID_SENSORS:
        s = s.replace(g, "__GRID_SENSOR__")
    s = s.replace("Werksreset (Grundeinstellung 26.09.)", "Werksreset (Grundeinstellung)")
    s = s.replace("geprüfte Grundeinstellung vom 26.09.2026", "Grundeinstellung bei der Installation")
    s = s.replace(" Hinweis: Bis 29.09. wurden die Zähler bei API-Anbindung in andere Sensoren geschrieben, "
                  "deshalb können für Tage davor Werte fehlen.", "")
    s = s.replace("{%- set tol = 40 -%}", "{%- set tol = states('number.marstek_tolerance_w') | float(40) -%}")
    s = s.replace("{%- set min_soc = 11 -%}", "{%- set min_soc = states('number.marstek_soc_safety_min_soc') | int(11) -%}")
    s = s.replace("{%- set full_days = 10 -%}",
                  "{%- set full_days = states('number.marstek_full_charge_interval_days') | int(10) -%}")
    cfg = json.loads(s)

    def fix_md(c):
        if c.get("type") == "markdown" and "[1,2,3,4]" in c.get("content", ""):
            c["content"] = BATS_JINJA + c["content"].replace("[1,2,3,4]", "bats")

    for v in cfg["views"]:
        keep = []
        for sec in v.get("sections", []):
            if any((c.get("heading") or "").startswith("Solar") for c in sec.get("cards", [])):
                continue
            sec["cards"] = [c for c in sec.get("cards", []) if c.get("entity") not in DROP_ENTITIES]
            for c in sec["cards"]:
                fix_md(c)
            keep.append(sec)
        v["sections"] = keep
    out = json.dumps(cfg, ensure_ascii=False, indent=1)
    rest = [x for x in ("_2\"", "netz_gefiltert", "stromzaehler", "solar", "pv_1_dach", "26.09", "29.09",
                        "[1,2,3,4]", "batterien_gesamt") if x in out]
    return cfg, rest


def apply_fix40_repo():
    """Nur FIX 40 auf die Dateien im Repo anwenden (ohne Neuaufbau aus der Live-Installation)."""
    dst = os.path.join(REPO, "apps", "marstek_venus")
    for fn, f in (("pv_control.py", lambda t: fix56_control(fix55_control(fix53_control(fix50_control(fix49_control(fix48_control(fix47_control(fix46_control(fix45_control(fix44_control(fix43_control(fix42_control(fix40_control(t)))))))))))))), ("pv_mqtt.py", lambda t: fix56_mqtt(fix52_mqtt(fix50_mqtt(fix48_mqtt(fix47_mqtt(fix46_mqtt(fix40_mqtt(t)))))))),
                  ("pv_config.py", lambda t: fix56_config(fix53_config(fix50_config(fix40_config(t)))))):
        p = os.path.join(dst, fn)
        wr(p, f(rd(p)))
    p = os.path.join(REPO, "dashboard", "marstek_dashboard.json")
    wr(p, json.dumps(fix57_dashboard(fix56_dashboard(fix55_dashboard(fix53_dashboard(fix52_dashboard(fix51_dashboard(fix50_dashboard(fix48_dashboard(fix47_dashboard(fix46_dashboard(fix45_dashboard(fix41_dashboard(fix40_dashboard(json.loads(rd(p))))))))))))))), ensure_ascii=False, indent=1) + "\n")
    print("FIX 40 OK")


def main():
    if "--fix40-only" in sys.argv:
        return apply_fix40_repo()
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", required=True)
    ap.add_argument("--dashboard", required=True)
    ap.add_argument("--version", required=True)
    a = ap.parse_args()
    dst = os.path.join(REPO, "apps", "marstek_venus")
    wr(os.path.join(dst, "pv_control.py"),
       fix56_control(fix55_control(fix53_control(fix50_control(fix49_control(fix48_control(fix47_control(fix46_control(fix45_control(fix44_control(fix43_control(fix42_control(fix40_control(build_control(rd(os.path.join(a.live, "pv_control.py")), a.version)))))))))))))))
    wr(os.path.join(dst, "pv_mqtt.py"), fix56_mqtt(fix52_mqtt(fix50_mqtt(fix48_mqtt(fix47_mqtt(fix46_mqtt(fix40_mqtt(build_mqtt(rd(os.path.join(a.live, "pv_mqtt.py")), a.version)))))))))
    wr(os.path.join(dst, "pv_config.py"), fix56_config(fix53_config(fix50_config(fix40_config(build_config(rd(os.path.join(a.live, "pv_config.py"))))))))
    raw = json.loads(rd(a.dashboard))
    cfg, rest = build_dashboard(raw["data"]["config"] if "data" in raw else raw)
    cfg = fix57_dashboard(fix56_dashboard(fix55_dashboard(fix53_dashboard(fix52_dashboard(fix51_dashboard(fix50_dashboard(fix48_dashboard(fix47_dashboard(fix46_dashboard(fix45_dashboard(fix41_dashboard(fix40_dashboard(cfg)))))))))))))
    wr(os.path.join(REPO, "dashboard", "marstek_dashboard.json"), json.dumps(cfg, ensure_ascii=False, indent=1) + "\n")
    wr(os.path.join(REPO, "VERSION"), a.version + "\n")
    print("OK - Rest-Hinweise im Dashboard:", rest or "keine")


if __name__ == "__main__":
    main()
