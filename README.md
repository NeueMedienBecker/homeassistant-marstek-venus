# Marstek Venus – Home Assistant / AppDaemon Skript

Python- / AppDaemon-Skript zur intelligenten Steuerung von **Marstek-Venus-Batteriesystemen**
in **Home Assistant** über **Modbus**.

Das Skript ermöglicht eine dynamische und batterieschonende Lade- und Entladesteuerung,
berücksichtigt SoC-Grenzen, unterstützt einen Notfall-Lademodus und bietet ausführliches
Logging zur Analyse und Fehlersuche.

---

## Funktionen

- Dynamische Lade- und Entladesteuerung
- SoC-basierte Regelung mit Mindest- und Maximalgrenzen
- Notfall-Lademodus bei kritischem Batteriestand
- Begrenzung der Lade- und Entladeleistung
- Debug- und Detail-Logging
- Modbus-Anbindung (TCP oder RTU)
- Optional: MQTT-Anbindung für Status und Steuerung

---

## Voraussetzungen

Pflichtvoraussetzungen:

- Home Assistant (Core / OS / Supervised)
- AppDaemon Add-on (installiert **und laufend**)
- Marstek-Venus-Batteriesystem
- Modbus-Zugriff (TCP oder RTU)
- Netzwerkverbindung zur Batterie
- Schreibrechte im AppDaemon-App-Verzeichnis

**Wichtig:**  
Das Skript wird **nur geladen und ausgeführt**, wenn **AppDaemon aktiv läuft**.

---

## Verzeichnisstruktur (WICHTIG)

Die Dateien müssen **exakt** so im AppDaemon-Verzeichnis liegen:

/config/appdaemon/apps/

marstek_venus/
marstek_venus.py
init.py

apps.yaml


Hinweise:

- Keine Leerzeichen in Datei- oder Ordnernamen
- Dateiname **exakt**: `marstek_venus.py`
- Die Datei `__init__.py` ist **zwingend erforderlich**

---

## Installation

### 1. AppDaemon installieren und starten

1. Home Assistant → Einstellungen
2. Add-ons → Add-on-Store
3. AppDaemon installieren
4. AppDaemon starten

Prüfen:
- Weboberfläche erreichbar (Standard-Port: 5050)
- Status: „Wird ausgeführt“

---

### 2. Benötigte Pakete in AppDaemon installieren

Öffne die AppDaemon-Add-on-Konfiguration.

**System packages**  
→ leer lassen

**Python packages**  
→ exakt diese Pakete eintragen:

pymodbus & paho-mqtt


Erklärung:
- `pymodbus` → Modbus-Kommunikation mit Marstek-Venus
- `paho-mqtt` → MQTT-Integration (Status / Steuerung)

**Init commands**  
→ leer lassen

Danach:
- Speichern
- AppDaemon neu starten
- Warten, bis AppDaemon vollständig hochgefahren ist

---

## apps.yaml konfigurieren

Datei: /config/appdaemon/apps/apps.yaml


Minimaler Eintrag:

```yaml
marstek_venus:
  module: marstek_venus
  class: MarstekVenus
```

Parameter	Bedeutung
modbus_host	IP-Adresse der Marstek-Venus
modbus_port	Modbus-Port (Standard: 502)
modbus_unit_id	Modbus-Slave-ID
min_soc	Untere SoC-Grenze
max_soc	Obere SoC-Grenze
emergency_soc	Schwelle für Notfall-Lademodus
max_charge_power	Maximale Ladeleistung (W)
max_discharge_power	Maximale Entladeleistung (W)
mqtt_enabled	MQTT aktivieren (true / false)
mqtt_host	MQTT-Broker IP
mqtt_port	MQTT-Broker Port
mqtt_topic_prefix	MQTT-Topic-Präfix
debug	Detail-Logging aktivieren



Funktionsbeschreibung der Lade-Logik
Normalbetrieb

SoC < min_soc
→ Entladung gesperrt

SoC > max_soc
→ Ladung gesperrt

SoC zwischen min_soc und max_soc
→ Dynamische Lade- und Entladesteuerung

Notfall-Lademodus

Aktiv, wenn:

SoC ≤ emergency_soc


Verhalten:

Erzwingt Ladung

Priorität auf Batterieschutz

Überschreibt normale Entladebeschränkungen

Funktionsprüfung

AppDaemon läuft

Keine Import-Fehler im Log

Skript startet beim AppDaemon-Start

Modbus-Werte werden gelesen

Steuerung reagiert korrekt

Typische Fehler
Fehler	Ursache
ModuleNotFoundError: pymodbus	Python-Paket fehlt
Keine Logausgabe	AppDaemon läuft nicht
Skript startet nicht	Falscher Klassenname
YAML-Fehler	Falsche Einrückung
Netzwerk

AppDaemon Webserver: Port 5050/tcp

Keine weiteren Ports notwendig

Modbus und MQTT erfolgen intern über Home Assistant



Lizenz

MIT License – freie Nutzung, Änderung und Weitergabe erlaubt.
