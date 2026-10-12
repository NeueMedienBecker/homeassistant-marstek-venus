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
    for fn, f in (("pv_control.py", lambda t: fix44_control(fix43_control(fix42_control(fix40_control(t))))), ("pv_mqtt.py", fix40_mqtt),
                  ("pv_config.py", fix40_config)):
        p = os.path.join(dst, fn)
        wr(p, f(rd(p)))
    p = os.path.join(REPO, "dashboard", "marstek_dashboard.json")
    wr(p, json.dumps(fix41_dashboard(fix40_dashboard(json.loads(rd(p)))), ensure_ascii=False, indent=1) + "\n")
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
       fix44_control(fix43_control(fix42_control(fix40_control(build_control(rd(os.path.join(a.live, "pv_control.py")), a.version))))))
    wr(os.path.join(dst, "pv_mqtt.py"), fix40_mqtt(build_mqtt(rd(os.path.join(a.live, "pv_mqtt.py")), a.version)))
    wr(os.path.join(dst, "pv_config.py"), fix40_config(build_config(rd(os.path.join(a.live, "pv_config.py")))))
    raw = json.loads(rd(a.dashboard))
    cfg, rest = build_dashboard(raw["data"]["config"] if "data" in raw else raw)
    cfg = fix41_dashboard(fix40_dashboard(cfg))
    wr(os.path.join(REPO, "dashboard", "marstek_dashboard.json"), json.dumps(cfg, ensure_ascii=False, indent=1) + "\n")
    wr(os.path.join(REPO, "VERSION"), a.version + "\n")
    print("OK - Rest-Hinweise im Dashboard:", rest or "keine")


if __name__ == "__main__":
    main()
