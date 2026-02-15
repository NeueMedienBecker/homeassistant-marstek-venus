# homeassistant-marstek-venus
Home-Assistant- / AppDaemon-Python-Skript zur Steuerung von Marstek-Venus-Batterien über Modbus.


# Marstek Venus – Home Assistant Skript

Python- / AppDaemon-Skript zur Steuerung von Marstek-Venus-Batteriesystemen
in Home Assistant über Modbus.

## Funktionen
- Dynamische Lade- und Entladesteuerung
- SoC-Grenzen
- Notfall-Lademodus
- Debug- und Detail-Logging

## Voraussetzungen
- Home Assistant
- AppDaemon
- Modbus (TCP oder RTU)

🧩 Installationsanleitung
Marstek-Venus-Skript für Home Assistant (AppDaemon)

Diese Anleitung beschreibt die Installation und den Betrieb des Marstek-Venus-Python-Skripts in Home Assistant mit AppDaemon.

🔧 Voraussetzungen (PFLICHT)

✅ Home Assistant (Core / OS / Supervised)

✅ AppDaemon Add-on installiert und laufend

✅ Zugriff auf Modbus (TCP oder RTU)

✅ Netzwerkverbindung zur Marstek-Venus-Batterie

✅ Schreibrechte im AppDaemon-App-Verzeichnis

⚠️ Wichtig:
Das Skript funktioniert nur, wenn AppDaemon aktiv läuft.
Ohne laufenden AppDaemon wird das Skript nicht geladen.

🧱 Schritt 1: AppDaemon installieren & starten

Home Assistant → Einstellungen

Add-ons → Add-on-Store

AppDaemon installieren

AppDaemon starten

Prüfen:

Weboberfläche erreichbar (Standard: Port 5050)

Status: „Wird ausgeführt“

📦 Schritt 2: Benötigte Pakete in AppDaemon installieren

Öffne die AppDaemon-Add-on-Konfiguration
(so wie in deinem Screenshot)

🔹 System packages

➡️ Nichts eintragen
(die benötigten Bibliotheken kommen als Python-Pakete)

🔹 Python packages

➡️ GENAU diese Pakete eintragen:

pymodbus
paho-mqtt


💡 Erklärung:

pymodbus → Kommunikation mit Marstek-Venus (Modbus)

paho-mqtt → MQTT-Integration (Status / Steuerung)

🔹 Init commands

➡️ Leer lassen

🔹 Speichern & Neustart

Speichern

AppDaemon neu starten

Warten, bis AppDaemon vollständig hochgefahren ist

📁 Schritt 3: Skript in AppDaemon ablegen
Verzeichnisstruktur

Das Skript muss im AppDaemon-App-Ordner liegen:

/config/appdaemon/apps/
│
├── marstek_venus/
│   ├── marstek_venus.py
│   └── __init__.py
│
└── apps.yaml


👉 Wichtig:

Keine Leerzeichen

Dateiname exakt: marstek_venus.py

⚙️ Schritt 4: apps.yaml konfigurieren

In der Datei /config/appdaemon/apps/apps.yaml:

marstek_venus:
  module: marstek_venus
  class: MarstekVenus


Falls dein Skript Konfigurationsparameter nutzt (z. B. Modbus-IP, Limits), kommen diese hier darunter.

▶️ Schritt 5: AppDaemon neu starten

AppDaemon neu starten

Log prüfen:

Home Assistant → Einstellungen → Add-ons → AppDaemon → Protokoll

Erwartete Logmeldung:
Marstek Venus Controller gestartet


❌ Falls Fehler auftreten:

Python-Pakete prüfen

Dateinamen prüfen

Einrückung in apps.yaml prüfen

🌐 Netzwerk / Ports

AppDaemon Webserver:

Port: 5050/tcp

Es sind keine weiteren Ports notwendig

Modbus / MQTT erfolgt intern über Home Assistant

🧪 Funktionsprüfung (Kurzcheck)

✅ AppDaemon läuft

✅ Keine Import-Fehler im Log

✅ Skript startet beim AppDaemon-Start

✅ Modbus-Werte werden gelesen

✅ Steuerung reagiert

⚠️ Typische Fehler & Lösungen
Fehler	Ursache
ModuleNotFoundError: pymodbus	Python-Paket nicht installiert
Skript startet nicht	AppDaemon läuft nicht
Keine Logausgabe	Falscher Klassenname
YAML-Fehler	Falsche Einrückung
✅ Zusammenfassung

✔ AppDaemon muss laufen
✔ pymodbus & paho-mqtt sind Pflicht
✔ Skript liegt unter /apps/
✔ Eintrag in apps.yaml erforderlich
