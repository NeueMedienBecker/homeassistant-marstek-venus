# Marstek Venus Steuerung für Home Assistant

**Deutsch** · [English](README.en.md)

Steuert eine oder mehrere **Marstek-Venus-Batterien** (Venus E, Venus E 3.0) aus Home Assistant heraus so,
dass am Stromzähler möglichst **nichts bezogen und nichts eingespeist** wird: Überschuss vom Balkonkraftwerk
oder der PV-Anlage landet in den Batterien, abends und nachts decken die Batterien den Verbrauch im Haus.

Die Batterien werden wahlweise über **WLAN (Marstek Open API, ohne Zusatzgerät)** oder über
**RS485-LAN-Adapter (Modbus TCP)** angesprochen – auch beides mit automatischem Ausweichweg.

Alles Weitere stellst du bequem im mitgelieferten **Dashboard „Marstek"** ein.

---

## Was kann die Steuerung?

- **Nulleinspeisung / Netz-Ausgleich** – Laden bei Überschuss, Entladen bei Bezug, mit Totband und Schutz gegen ständiges Hin- und Herschalten
- **Mehrere Batterien** – Leistung wird nach Ladestand verteilt; bei wenig PV-Leistung wird gezielt nur eine Batterie geladen (effizienter), die Reihenfolge wechselt
- **Zwei Anbindungen je Batterie** – Modbus *oder* WLAN-API *oder* „Modbus, sonst API" bzw. „API, sonst Modbus". Antwortet eine Batterie auf keinem Weg, bekommt sie keine Leistung zugeteilt
- **SoC-Schutz / Notladung** – hängt eine Batterie zu lange am Minimum, wird sie sanft nachgeladen (bevorzugt mit Sonne)
- **BMS-Pflege** – regelmäßige Vollladung zum Zellausgleich, eine Batterie nach der anderen, nur mit PV-Überschuss (pausiert bei Wolken und nachts)
- **Sicherheit** – unplausible oder veraltete Zählerwerte stoppen die Batterien; Hauptschalter und Schalter je Batterie
- **Dashboard** – Übersicht mit Ampel, Ladestand, Leistung, Energie *heute / gestern / diese Woche*, alle Einstellungen live änderbar (ohne Neustart), Batterien anlegen und löschen, Einstellungen sichern
- **Firmware-Erkennung** – unterschiedliche Einheiten der Open API je Firmware (z. B. Temperatur, Energiezähler) werden automatisch umgerechnet

---

## Was brauchst du?

| | |
|---|---|
| **Home Assistant** | Home Assistant OS oder Supervised (also mit Add-ons) |
| **Batterie** | Marstek Venus E / Venus E 3.0 (andere Venus-Modelle ungetestet) |
| **Anbindung** | **WLAN:** in der Marstek-App die *Open API* einschalten (siehe unten) – **oder** – **Modbus:** ein RS485-LAN-Adapter je Batterie |
| **Netz-Zähler** | ein Sensor in Home Assistant, der die aktuelle Leistung am Hausanschluss zeigt (z. B. Lesekopf am Stromzähler, Shelly 3EM). W oder kW, **positiv bei Bezug, negativ bei Einspeisung** (andersherum geht auch: Installer mit --meter starten und die Vorzeichen-Frage mit „nein“ beantworten) |

Mosquitto (MQTT) und AppDaemon brauchst du **nicht** selbst zu installieren – das erledigt der Installer.

---

## Installation in 3 Schritten

**1. Terminal installieren**

Einstellungen → **Apps** → App installieren → **„Terminal & SSH"** installieren, starten und
„In Seitenleiste anzeigen" einschalten. (Bei älteren Home-Assistant-Versionen heißt es
Einstellungen → Add-ons → Add-on-Store. Wer schon „Advanced SSH & Web Terminal" hat, kann das nehmen.)

**2. Terminal öffnen und diesen einen Befehl einfügen**

    curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash

**3. Warten – der Rest läuft von allein**

Der Installer richtet alles selbst ein, ohne Fragen:

- Er installiert **Mosquitto (MQTT)** und **AppDaemon** mit den nötigen Python-Paketen und richtet die MQTT-Integration ein
- Er **sucht deine Batterien im WLAN** (Open API muss in der Marstek-App an sein) und übernimmt sie
- Er **erkennt deinen Netz-Zähler** (Sensor für die Leistung am Hausanschluss)
- Er legt das **Dashboard „Marstek"** in der Seitenleiste an und prüft zum Schluss, ob die Batterien Werte liefern
- Bei einer Neuinstallation startet Home Assistant am Ende einmal neu (etwa 1 Minute)

Nur wenn er etwas nicht eindeutig erkennt (keine Batterie gefunden, mehrere mögliche Zähler), fragt er nach –
**Enter** übernimmt dann den Vorschlag. Wer lieber alles selbst angibt: Option --ask.

**Beobachtungsmodus:** Regelt gerade eine andere Steuerung die Batterien (z. B. ein zweites Home Assistant)
oder fehlt der Netz-Zähler noch, startet die Steuerung im Beobachtungsmodus – sie zeigt alle Werte, sendet
aber keine Befehle. Ausschalten im Dashboard: Konfiguration → Hauptschalter & Batterien → „Nur beobachten".

> **Tipp:** Gib jeder Batterie (und jedem RS485-Adapter) im Router eine **feste IP-Adresse**.

---

## Open API in der Marstek-App einschalten (für WLAN)

1. Marstek-App öffnen und die Batterie auswählen
2. Einstellungen → **Open API** (bei manchen Versionen unter „Lokale API" / „Netzwerk") → **einschalten**
3. **UDP-Port 30000** eintragen (Standard)
4. Die angezeigte **WLAN-IP** der Batterie notieren

Antwortet die Batterie beim Test im Installer gar nicht, ist die Open API meist noch ausgeschaltet oder der Port stimmt nicht.

---

## Update

Einfach **denselben Befehl erneut** im Terminal ausführen. Der Installer fragt dann, ob die
Einstellungen bleiben sollen (Enter) oder ob alles neu eingerichtet werden soll. Deine Einstellungen (Batterien, IPs, Zähler,
alle im Dashboard geänderten Werte) bleiben erhalten; von allen ersetzten Dateien wird vorher eine
Sicherung im Ordner *backup* angelegt. Das Dashboard wird nur ersetzt, wenn du es bestätigst.

**Weitere Optionen** – hinter den Befehl *bash -s --* und die Option hängen, z. B.:

    curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash -s -- --reconfigure

| Option | Wirkung |
|---|---|
| --fresh | alles neu einrichten: Batterien, Zähler und Dashboard-Werte löschen (mit Sicherung) |
| --meter | nur den Netz-Zähler neu wählen (z. B. nachdem der Lesekopf eingerichtet wurde) |
| --reconfigure | Batterien und Netz-Zähler neu abfragen (z. B. RS485-Adapter, anderer Zähler) |
| --ask | Neuinstallation mit allen Fragen statt automatisch |
| --dashboard | nur das Dashboard neu erzeugen (z. B. nach dem Anlegen einer weiteren Batterie) |
| --dry-run | Testlauf: zeigt nur, was passieren würde |
| --yes | Update ohne Rückfragen |
| --lang en | Installer auf Englisch |
| --uninstall | Steuerung entfernen (mit Sicherung) |
| --diag | Fehlersuche: erstellt einen Bericht zum Weitergeben (ohne Passwörter) und bietet an, gefundene Probleme gleich zu beheben. Vorhandene MQTT-Benutzer und Passwörter werden dabei nie geändert |
| --fix | wie --diag, behebt gefundene Probleme aber ohne Rückfrage |

---

## Häufige Fragen

**Das Dashboard zeigt „Beobachtungsmodus“ – ist das ein Fehler?**
Nein. Die Steuerung liest nur und sendet keine Befehle. Das passiert automatisch, wenn der Netz-Zähler noch
fehlt oder eine andere Steuerung die Batterien gerade regelt (z. B. die Marstek-App im Modus „Passive“ oder ein
zweites Home Assistant). Ausschalten: Konfiguration → Hauptschalter & Batterien → „Nur beobachten“ aus – aber
erst, wenn keine andere Steuerung mehr läuft. **Es darf immer nur eine Steuerung die Batterien regeln.**

**Ich habe den Zähler erst nach der Installation eingerichtet.**
Im Terminal den Installer mit *--meter* starten. Er erkennt den Zähler, trägt ihn ein und schaltet den
Beobachtungsmodus aus, wenn nichts mehr dagegenspricht.

**Ich habe eine weitere Batterie bekommen.**
Open API in der Marstek-App einschalten und einfach den normalen Installer-Befehl erneut ausführen – neue
WLAN-Batterien werden gefunden und ergänzt, das Dashboard wird neu erzeugt. Batterien mit RS485-Adapter:
Installer mit *--reconfigure*.

**Die Batterien laden, wenn sie entladen sollten.**
Das Vorzeichen des Zählers ist vertauscht: Installer mit *--meter* starten und die Vorzeichen-Frage mit „nein“ beantworten.

**Ich habe die alte Ein-Datei-Version (1.0) im Einsatz.**
Einfach den Installer ausführen. Die alte Datei bleibt liegen, bitte den alten Eintrag in *apps.yaml* danach entfernen.

---

## Das Dashboard „Marstek"

- **Übersicht** – Ampel je Batterie (Kontakt, Ladestand, Temperatur, Vollladung, Ausweichweg), Leistung und Ladestand, Verläufe
- **Batterien** – je Batterie Zustand, Leistung, Temperaturen und **Energie geladen/entladen heute, gestern, diese Woche**
- **Konfiguration** – aufklappbar: Hauptschalter, je Batterie Anbindung und IP-Adressen, Zähler & zweiter Speicher (z. B. Hybrid-Wechselrichter), SoC-Schutz, Regelung, BMS-Pflege, Erweitert; dazu Verwaltung (Einstellungen fest übernehmen / verwerfen / Werksreset, Batterie anlegen / löschen). Jede Gruppe hat eine einfache Erklärung.

Änderungen im Dashboard wirken nach ca. 15 Sekunden, ohne Neustart.

---

## Fehlerbehebung

| Problem | Lösung |
|---|---|
| Batterie liefert keine Werte (WLAN) | Open API in der App an? Port 30000? WLAN-IP richtig? Im Dashboard unter Konfiguration → Venus N prüfen |
| Batterie liefert keine Werte (Modbus) | Adapter-IP und Port 502 prüfen, Slave-ID meist 1; nur **eine** Verbindung je Adapter erlaubt (keine zweite Modbus-Integration parallel) |
| „Netz-Zähler nicht verfügbar" | Sensor in Home Assistant prüfen; bei veralteten Werten (über 60 s) stoppt die Steuerung zur Sicherheit |
| Batterien laden, wenn sie entladen sollten | Vorzeichen des Zählers falsch → Installer mit --reconfigure starten und die Vorzeichen-Frage mit „nein" beantworten |
| Konfigurationsseite zeigt rote Kästen | die Erweiterung *fold-entity-row* fehlt → Installer mit --dashboard erneut ausführen oder über HACS installieren |
| Dashboard zeigt „Beobachtungsmodus" | gewollt, solange eine andere Steuerung läuft oder der Zähler fehlt – danach unter Konfiguration → Hauptschalter & Batterien „Nur beobachten" ausschalten |
| Viele Felder zeigen „Entität nicht gefunden“ | Schalter und Einstellungen kommen über MQTT – meist fehlt die MQTT-Integration oder das MQTT-Passwort passt nicht. Den normalen Installationsbefehl erneut ausführen; hilft das nicht, mit --diag einen Bericht erstellen |
| Sonstiges | Installer mit **--diag** starten und den Bericht schicken, oder Einstellungen → Apps (Add-ons) → AppDaemon → **Protokoll** |

---

## Wie ist das aufgebaut?

| Datei | Zweck |
|---|---|
| apps/marstek_venus/pv_control.py | die Regelung (AppDaemon-App, Takt 15 s) |
| apps/marstek_venus/pv_mqtt.py | Brücke zum Dashboard (MQTT-Entitäten „Marstek Steuerung") |
| apps/marstek_venus/pv_config.py | alle Einstellungen mit Erklärung |
| dashboard/marstek_dashboard.json | Vorlage für das Dashboard |
| installer/ha_setup.py, install.sh | Installer / Updater |

Installiert wird nach */addon_configs/a0d7b954_appdaemon/apps/marstek_venus/* (bei Home Assistant ab 2026.7: */app_configs/…*). Im Dashboard geänderte Werte
stehen in *pv_mqtt_overrides.json*, angelegte Batterien in *pv_batteries.json*, die MQTT-Zugangsdaten in
*pv_mqtt_login.json* – diese Dateien bleiben bei Updates unangetastet.

**Eigener MQTT-Broker:** Benutzt Home Assistant nicht Mosquitto, sondern einen anderen MQTT-Broker, übernimmt die Steuerung bei Installation, Update und `--fix` automatisch diesen Broker (Zugangsdaten aus der MQTT-Integration, vorher Verbindungstest). Am Broker und an Passwörtern wird nichts geändert, Mosquitto wird dann nicht installiert. Schlägt der Test fehl, auf dem Broker einen Benutzer anlegen und in *pv_mqtt_login.json* eintragen – er bleibt bei jedem Update erhalten.

**Zähler sendet selten:** Kommt vom Netz-Zähler länger als 60 s kein neuer Wert, gehen die Batterien zur Sicherheit auf STOP. Im Dashboard unter *Konfiguration → Zähler & zweiter Speicher* lässt sich diese Wartezeit einstellen; die **Zähler-Info** daneben zeigt, wie oft der Zähler wirklich neue Werte liefert, und macht einen Vorschlag.

**Grenzwerte je Batterie:** max. Lade- und Entladeleistung, Min-/Max-SoC und Kapazität jeder Venus lassen sich im Dashboard unter *Konfiguration → Hauptschalter & Batterien → Venus X → Grenzwerte* ändern.

**Zellspannungen (nur Modbus):** Bei Anbindung per Modbus zeigt der Tab *Batterien* je Venus die höchste und niedrigste Zellspannung und den Unterschied in mV. Liegt der Unterschied länger über der Grenze (Standard 100 mV), wird die BMS-Pflege vorgezogen – nur mit PV-Überschuss. Die WLAN-API liefert keine Zellspannungen.

**Updates:** Die Steuerung prüft alle 6 Stunden, ob es auf GitHub eine neue Version gibt. Dann erscheint in der *Übersicht* ein Hinweis mit allen Neuerungen (zusätzlich unter *Einstellungen → Updates*). Eingespielt wird mit dem Installationsbefehl – alle Einstellungen bleiben erhalten.

**Ohne Installer** (für Erfahrene): AppDaemon-Add-on mit den Python-Paketen *pymodbus==3.9.2* und *paho-mqtt*,
den Ordner *apps/marstek_venus* in den AppDaemon-apps-Ordner kopieren, in *apps.yaml* eintragen:

    marstek_venus:
      module: pv_control
      class: PVControlApp

dann *pv_config.py* (BATTERY_CONFIG, POWER_SENSOR) anpassen und *pv_mqtt_login.json* mit
host / port / user / password des MQTT-Brokers anlegen.

Die alte Ein-Datei-Version (bis 03/2026) liegt im Ordner [legacy](legacy/).

---

## Haftung, Lizenz, Kontakt

Die Steuerung greift aktiv in das Laden und Entladen deiner Batterien ein. Nutzung auf eigene Verantwortung –
bitte den [Haftungsausschluss](DISCLAIMER.md) lesen. Dieses Projekt steht in keiner Verbindung zu Marstek.

Lizenz: [MIT](LICENSE) · Entwickelt und gepflegt von **Neue Medien Becker** · info@neuemedienbecker.de

Wenn dir das Projekt hilft, freue ich mich über einen Stern ⭐ auf GitHub oder eine freiwillige Spende.
