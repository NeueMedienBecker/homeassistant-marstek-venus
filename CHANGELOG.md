# Changelog

## 2.0.24 (2026-10-16)

### Neu
- **Ausschalten gibt die Batterie an Marstek zurueck**: Hauptschalter 'Regelung aktiv' oder 'Venus N vom Script gesteuert' aus -> die Batterie wird gestoppt und die Steuerung an Marstek zurueckgegeben (Modbus: RS485-Steuerung aus, Register 42000 = 21947; WLAN/API: Modus Auto). Sie laesst sich dann wieder in der Marstek-App bedienen (z. B. von Hand laden). Schalter wieder an -> das Script uebernimmt wieder. Abschaltbar mit RELEASE_ON_DISABLE = False.
- **Notladung: Wartezeit einstellbar**: 'Wartezeit unter Min-SoC' (SOC_SAFETY_AFTER_HOURS) im Dashboard unter SoC-Schutz / Notladung, 0 = sofort. Standard fuer neue Installationen 24 h (vorher fest 48 h).

### Behoben
- Bisher schaltete das Script die RS485-Steuerung bei jedem Auslesen wieder ein, auch wenn die Batterie im Dashboard abgeschaltet war - die Marstek-App kam nicht an die Batterie.

## 2.0.23 (2026-10-15)

### Neu
- **BMS-Pflege: Zustand im Dashboard**: Im Tab Batterien steht je Venus 'BMS-Pflege Zustand', in der Uebersicht erscheint ein Hinweis, solange eine BMS-Pflege laeuft - z. B. 'Pausiert - wartet auf Sonne (PV-Ueberschuss). Spaetestens ab 16.10. 15:59 Uhr laedt sie aus dem Netz.', 'Vollladung laeuft mit PV-Ueberschuss', 'Vollladung laeuft aus dem Netz' oder 'Voll - Haltephase bis ...'.

### Behoben
- In den Attributen der Batterie-, BMS- und Info-Sensoren standen interne Zusatzdaten (last_changed, context). Die Attribute werden jetzt ersetzt statt gemischt.

## 2.0.22 (2026-10-14)

### Neu
- **Update-Hinweis**: Die Steuerung prueft alle 6 Stunden GitHub. Gibt es eine neue Version, zeigt die Uebersicht einen Hinweis mit allen Neuerungen seit der installierten Version und dem Befehl zum Einspielen; Home Assistant zeigt das Update auch unter Einstellungen -> Updates. Abschalten: UPDATE_CHECK = False in pv_config.py.
- **Vollladung bis 100 %** (vorher 99 %): bei 100 % gleicht das BMS am gruendlichsten aus. Zeigt eine Batterie nie 100 %, gilt sie als voll, wenn sie mind. 99 % zeigt und 30 min trotz Ladebefehl keine Leistung mehr aufnimmt - die BMS-Pflege bleibt nicht haengen.

## 2.0.21 (2026-10-14)

### Behoben
- Zaehler-Info und Zell-Unterschied: Bei einem Wert von genau 0 (z. B. Zaehlerwert gerade erst gekommen) lehnte Home Assistant die Aktualisierung ab ('400 Bad Request' im AppDaemon-Log). Der Wert wird jetzt als Text uebergeben.

## 2.0.20 (2026-10-14)

### Behoben
- Energiezaehler-Schutz (2.0.17) meldete 'unplausibel', wenn eine Batterie zwischen WLAN-API (0,1 kWh genau) und Modbus (0,01 kWh genau) wechselte. Kleine Ruecklaeufe bis 0,2 kWh werden jetzt still ignoriert (der Zaehler bleibt auf dem hoeheren Wert).

## 2.0.19 (2026-10-14)

### Behoben
- **Geloeschte Batterie kam beim Update wieder**: Der Installer hat eine im Dashboard geloeschte Batterie beim naechsten Update wieder aktiviert, sobald er sie im Netz fand. Geloeschte Batterien bleiben jetzt geloescht (wieder anlegen nur bewusst im Dashboard).
- **Doppelte Batterie nach IP-Wechsel**: Bekam eine Venus per DHCP eine neue WLAN-IP, legte der Installer sie als zusaetzliche Batterie an (zwei Eintraege fuer dasselbe Geraet). Jetzt vergleicht er die Energiezaehler mit den vorhandenen Batterien und traegt bei Gleichheit nur die neue IP nach - auch in den Dashboard-Werten.

## 2.0.18 (2026-10-14)

### Neu
- **Hilfetext BMS-Pflege** verstaendlicher: Ablauf in 4 Schritten, alle Einstellungen erklaert, warum 'Vollladung bis' 99 % statt 100 %.
- **Zellspannungen (nur Modbus)**: hoechste und niedrigste Zellspannung (Register 37007/37008) und der Unterschied in mV je Venus im Batterien-Tab (sensor.marstekvenus_N_cell_voltage_delta). Bei Anbindung per WLAN-API steht dort 'nur mit Modbus-Anbindung verfuegbar' - die API liefert keine Zellspannungen.
- **BMS-Pflege vorziehen**: Liegt der Unterschied mind. 30 min ueber 'Vorziehen ab Zell-Unterschied' (Standard 100 mV, 0 = aus), startet die Vollladung schon vor den 10 Tagen - nur mit PV-Ueberschuss, fruehestens 3 Tage nach der letzten (einstellbar). Kennt eine Venus die Register nicht, werden sie 6 h uebersprungen.

## 2.0.17 (2026-10-14)

### Neu
- **Grenzwerte je Batterie im Dashboard**: max. Ladeleistung, max. Entladeleistung, Min-SoC, Max-SoC und Kapazitaet jeder Venus sind unter Konfiguration -> Hauptschalter & Batterien -> Venus X -> Grenzwerte einstellbar (vorher nur in pv_config.py). Aenderungen wirken ab dem naechsten Regelzyklus, die Verbindung wird nicht neu aufgebaut.

### Behoben
- **Energie 'heute' mit Hunderten kWh**: Kam vom Akku kurz ein falscher Gesamtzaehler (z. B. der Lade- statt der Entladezaehler, etwa nach einer vertauschten Modbus-Antwort), zaehlte Home Assistant die Differenz und den Ruecksprung als Verbrauch. Gesamtzaehler werden jetzt nur noch plausibel weitergegeben: kein Ruecksprung, kein Sprung groesser als physikalisch moeglich; ein abweichender Wert wird erst nach 10 min Stabilitaet uebernommen.

## 2.0.16 (2026-10-14)

### Neu
- **Zaehler-Wartezeit im Dashboard**: 'STOP wenn Zaehlerwert aelter als' (METER_MAX_AGE_S, Standard 60 s) ist unter Konfiguration -> Zaehler & zweiter Speicher einstellbar.
- **Zaehler-Info im Dashboard** (sensor.marstek_meter_info): Alter des letzten Zaehlerwerts, wie oft im Schnitt neue Werte kommen, laengste Pause, STOPs der letzten 24 h und ein Vorschlag fuer die Wartezeit. Bei Pausen ueber 5 min Hinweis 'Zaehler bzw. WLAN pruefen'.
- --diag wertet 'Zaehlerwert zu alt -> STOP' im Log aus und nennt die passende Wartezeit.

## 2.0.15 (2026-10-14)

### Behoben
- **Anderer MQTT-Broker**: Zeigt die MQTT-Integration von Home Assistant auf einen anderen Broker als Mosquitto, uebernimmt die Steuerung diesen Broker jetzt schon bei Installation und Update (vorher nur per --fix) - mit den Zugangsdaten der Integration und nur nach erfolgreichem Verbindungstest. Am Broker und an Passwoertern wird nichts geaendert; Mosquitto wird in diesem Fall weder installiert noch angefasst. Vorher landeten die Schalter auf Mosquitto und Home Assistant sah sie nie ("Entitaet nicht gefunden").
- --diag: zeigt das Ergebnis des Verbindungstests, prueft das Discovery-Praefix und meldet fehlendes Mosquitto nicht mehr als Fehler, wenn es gar nicht gebraucht wird.

## 2.0.14 (2026-10-14)

### Neu
- **--diag behebt auf Wunsch gleich**: Nach dem Bericht listet der Installer, was er automatisch reparieren kann (Mosquitto/AppDaemon starten, paho-mqtt, MQTT-Zugang, apps.yaml, MQTT-Integration einrichten oder aktivieren, Discovery einschalten, deaktivierte Entitaeten aktivieren, Steuerung auf den Broker von Home Assistant umstellen) und fragt nach. `--fix` repariert ohne Rueckfrage.

### Geaendert
- Vorhandene MQTT-Benutzer und Passwoerter werden nie mehr geaendert (auch nicht beim normalen Update). Weicht das Passwort ab, uebernimmt die Steuerung das Passwort aus Mosquitto. Fehlt der Benutzer, wird nur der eigene Benutzer 'marstek' ergaenzt. Benutzt die Steuerung einen fremden Broker, bleibt Mosquitto ganz unberuehrt.

## 2.0.13 (2026-10-14)

### Neu
- Installer-Option **--diag** (Fehlersuche): prueft Mosquitto, AppDaemon, MQTT-Zugang, MQTT-Integration, Steuer-Entitaeten und die Protokolle, nennt gefundene Probleme in einfachen Worten und speichert einen Bericht ohne Passwoerter im Home-Assistant-Ordner. Veraendert nichts.

## 2.0.12 (2026-10-13)

### Neu
- Dashboard, Tab Konfiguration, neue Gruppe **Zaehler & zweiter Speicher**: Der Netz-Zaehler (Sensor, Vorzeichen) ist jetzt im Dashboard sichtbar und aenderbar. Zusaetzlich kann ein zweiter Speicher (z. B. Hybrid-Wechselrichter wie SMA) mit Entlade-, Lade- und PV-Leistung eingetragen werden - die Regelung rechnet ihn dann heraus: abends uebernimmt die Venus den Hausverbrauch zuerst, tagsueber laedt erst der andere Speicher voll, die Venus nur mit dem Rest.

### Behoben
- BMS-Pflege: "Max. Warten auf Sonne" zaehlt ab dem Faelligkeitstag. Ist die Vollladung bis dahin nicht fertig (zu wenig Sonne), laedt sie danach mit voller Leistung ohne PV durch.
- Installer: --meter / --reconfigure halten den neuen Dashboard-Wert "Netz-Zaehler" synchron.

## 2.0.11 (2026-10-13)

### Behoben
- BMS-Pflege: Die Vollladung laeuft jetzt nur noch mit dem PV-Ueberschuss, der gerade da ist. Bei Wolken oder nachts pausiert sie (vorher lief sie stur mit voller Leistung weiter - aus dem Netz oder aus einem zweiten Speicher, z. B. einem SMA-Hybridspeicher). Erst wenn sie BMS_CARE_PV_WAIT_H Stunden nicht fertig wurde, laedt sie mit voller Leistung.
- Die geregelten Batterien nehmen der BMS-Batterie den PV-Ueberschuss nicht mehr weg (vorher lud die geregelte Batterie den ganzen Ueberschuss und die BMS-Batterie zog aus dem Netz).

### Neu
- Batterien-Tab: Je Batterie steht jetzt, wann die BMS-Pflege zuletzt lief und wann sie wieder faellig ist (Dashboard einmal mit `--dashboard` neu erzeugen lassen).

## 2.0.10 (2026-10-12)

### Behoben
- Laedt eine Batterie fuer die BMS-Pflege oder eine Notladung, wurde diese Ladeleistung als Hausverbrauch gewertet: die anderen Batterien (oder ein fremder Speicher) haben entladen, um die ladende Batterie zu fuettern. Diese Ladeleistung wird jetzt aus dem Netzfluss herausgerechnet.
- Nach einer Neuinstallation startete die BMS-Pflege sofort mit einer Vollladung aus dem Netz. Jetzt wartet sie wie gewohnt erst auf PV-Ueberschuss (BMS_CARE_PV_WAIT_H).

## 2.0.9 (2026-10-12)

### Behoben
- Eine im Dashboard geloeschte Batterie wird beim naechsten Update wieder sauber angelegt (vorher blieb sie als geloescht markiert).

## 2.0.8 (2026-10-12)

### Behoben
- Batterie loeschen im Dashboard: Die Sensoren der geloeschten Batterie verschwinden jetzt sofort (vorher blieben sie bis zum Neustart von Home Assistant sichtbar, so dass das Loeschen wirkungslos aussah).

## 2.0.7 (2026-10-12)

### Behoben
- --meter: nach einem Zaehlerwechsel wird das Dashboard ohne Rueckfrage neu erzeugt.

## 2.0.6 (2026-10-12)

### Neu
- Beim Update fragt der Installer: Einstellungen behalten oder alles neu einrichten (Batterien, Zaehler, Dashboard-Werte loeschen, mit Sicherung). Ohne Frage: Option --fresh.

### Behoben
- Im Dashboard geaenderte IP-Adressen wurden bei der Suche nach neuen Batterien nicht beruecksichtigt (doppelte Batterien).

## 2.0.5 (2026-10-11)

### Neu
- Option --meter: nur den Netz-Zaehler neu waehlen (automatisch, wenn eindeutig). Der Beobachtungsmodus wird danach nur ausgeschaltet, wenn keine andere Steuerung die Batterien regelt.

## 2.0.4 (2026-10-11)

### Neu
- Update: Neue WLAN-Batterien (Open API) werden beim Update automatisch gefunden und ergaenzt, das Dashboard wird dann ohne Rueckfrage neu erzeugt. Gilt nur, wenn alle vorhandenen Batterien ueber WLAN angebunden sind; sonst Installer mit --reconfigure.

## 2.0.3 (2026-10-11)

### Behoben
- Dashboard: Fehlt der Netz-Zaehler noch, steht jetzt ein klarer Hinweis mit Anleitung statt Entitaet nicht gefunden.

## 2.0.2 (2026-10-11)

### Behoben
- Dashboard: Der Schalter „Nur beobachten“ stand an der falschen Stelle (Kachel „Konfigurationsfehler“ in der Übersicht) und fehlte unter Konfiguration → Hauptschalter & Batterien. Jetzt dort als Zeile, in der Übersicht als Kachel, solange er an ist.
- Dashboard: Werte, die es nur per Modbus gibt (Wechselrichter-Status, Spannung, MOS-Temperaturen), werden bei WLAN-Batterien ausgeblendet statt „Entität nicht gefunden“ zu zeigen.
- Nach dem Update das Dashboard neu erzeugen: Installer mit --dashboard ausführen.

## 2.0.1 (2026-10-11)

### Behoben
- Installer: funktioniert jetzt auch mit dem Standard-Terminal „Terminal & SSH“. Dort fehlte der Zugriff auf Home Assistant, deshalb wurden MQTT-Integration und Dashboard nicht angelegt und der Netz-Zähler nicht gefunden. Eine kleine Hilfs-App in AppDaemon übernimmt das während der Einrichtung und wird danach automatisch wieder entfernt.
- Wird der Ordner www neu angelegt, startet der Installer Home Assistant am Ende einmal neu, damit das Dashboard alle Teile laden kann.
- Eine von Mosquitto gemeldete MQTT-Einrichtung wird zuverlässig gefunden und abgeschlossen.

## 2.0.0 (2026-10-01)

### Neu
- **Installer / Updater** für Home Assistant mit einem Befehl (install.sh, installer/ha_setup.py): richtet Mosquitto, AppDaemon (inkl. Python-Pakete), MQTT-Integration und das Dashboard ein, findet Marstek-Batterien im WLAN und den Netz-Zähler **automatisch, ohne Fragen** (gefragt wird nur, was sich nicht erkennen lässt) und prüft am Ende, ob Werte kommen. Updates übernehmen alle eigenen Einstellungen. Deinstallation mit Sicherung. Deutsch und Englisch.
- **Beobachtungsmodus** („Nur beobachten", READ_ONLY): liest und zeigt alle Werte, sendet aber keine Befehle an die Batterien. Schaltet sich bei der Einrichtung automatisch ein, wenn schon eine andere Steuerung die Batterien regelt oder der Netz-Zähler fehlt.
- Unterstützt die neuen Home-Assistant-Pfade ab 2026.7 („Apps" statt „Add-ons").
- Aufteilung in **pv_control.py** (Regelung), **pv_mqtt.py** (Brücke zum Dashboard) und **pv_config.py** (alle Einstellungen mit Erklärung).
- **Marstek Open API (WLAN)** als vollwertige Alternative zu Modbus: ein gemeinsamer UDP-Socket für alle Batterien, kurze Timeouts mit Wiederholung, letzte Werte bei kurzen Aussetzern, Sollwert-Auffrischung vor Ablauf des Countdowns, Erkennung von Modell und Firmware mit automatischer Einheiten-Korrektur (Venus E Hardware 2.0 ab Firmware 154: Temperatur und Energiezähler).
- **Anbindung je Batterie wählbar**: Modbus / API / „Modbus, sonst API" / „API, sonst Modbus". IP-Adressen und Ports im Dashboard änderbar.
- **Dashboard „Marstek"**: Übersicht mit Ampel je Batterie, Batterien-Seite mit Energie geladen/entladen heute, gestern, diese Woche, aufklappbare Konfiguration, Verwaltung (Einstellungen fest übernehmen, verwerfen, Werksreset, Batterie anlegen/löschen). Alle Einstellungen wirken live ohne Neustart.
- Hauptschalter und Schalter je Batterie, PV-Vorrang für Notladung und BMS-Pflege, Plausibilitätsprüfung des Netz-Zählers (Ausreißer, veraltete Werte → Stopp).
- Netz-Zähler: Vorzeichen umkehrbar (POWER_SENSOR „invert"), Werte in kW werden automatisch umgerechnet.

### Geändert
- Eine Batterie, die auf keinem Weg antwortet, bekommt keinen Leistungsanteil mehr.
- Sollwerte über die API werden nur bei Änderung gesendet; kleine Änderungen (unter 50 W) werden spätestens nach 30 s nachgeholt.
- Dauerhafte Modbus-Verbindungen, paralleles Lesen und Schreiben aller Batterien, späte Antworten werden erkannt.
- Status-Tabelle im Log nur noch jede Minute (bei Ereignissen sofort).

### Behoben
- Nach einem Verbindungsabbruch konnte die Ausweichlogik dauerhaft auf einem nicht erreichbaren Weg hängen bleiben; die Batterie bekam trotzdem Leistung zugeteilt, die dann ins Netz ging.
- Temperatur und Energiezähler über die Open API bei Venus E (Hardware 2.0) waren falsch skaliert.

## 1.0 (2026-03-01)

### Added
- Zentrale Registerdefinition `HA_REGMAP` (type: u16/s16/u32/s32 inkl. `scale`, `offset`, `precision` wie in HA-YAML)
- PV-Surplus Zeitfilter `PV_SURPLUS_MIN_TIME` (Anti-Wolken-PingPong, erst nach stabiler Dauer aktiv)
- SOC-SAFETY (Anti-Totlaufen am Min-SoC) mit Persistenz:
  - `min_soc_since`, `safety_active`, `safety_done`
- Option `SOC_SAFETY_REQUIRE_PV` (Schutzladung nur bei PV-Ueberschuss)
- HA Timeline Sensoren fuer BMS-Status:
  - `sensor.bms_<battery>_phase` inkl. `last_full`, `hold_until`, `cooldown_until`
- Setpoint Execution Verification (optional):
  - `VERIFY_SETPOINT_EXEC`, `VERIFY_MIN_SET_W`, `VERIFY_TOL_W`

### Changed
- Mode-Entscheidung (Grid-Follow):
  - `stop` nur wenn wirklich Grid ~ 0 UND Batterie ~ 0 UND kein SOC-SAFETY aktiv
  - `discharge` sobald Last vorhanden oder Batterie bereits laeuft
  - `charge` nur bei echter Einspeisung
- BMS-Care als **weicher Modus**:
  - eine Batterie im BMS (charging/hold), andere Batterien regeln weiter normal
- Waterfill-Verteilung:
  - detailliertere Teilnahme-/Ausschlussgruende (SoC, BMS hold/cooldown, pv_surplus)
  - zusaetzliche Runden-/Residual-Logs
- Schreiblogik:
  - weniger Writes durch `TOLERANCE_W` und "need_write" (insb. bei `limit=0`)
  - Stop schreibt nur wenn wirklich etwas zu stoppen ist (mode/bp/last != 0)
- Logging/Finally:
  - unbound-sicher (finally loggt immer stabil)
  - PVCONTROL Tabelle erweitert (BMS/SOC-SAFETY, pv_surplus, SOC11-Seit)

### Fixed
- Robustere Fehlerbehandlung bei Modbus Read/Write (None-Responses / fehlende Keys verursachen keinen Crash)
- Reduziertes Ping-Pong Verhalten bei PV-Schwankungen durch Surplus-Stabilisierung
- Schutz vor doppelten/mehrfachen Status-Logs (konsolidierter Log-Block)

---

## Ältere Versionen

- Basis Grid-Follow Regelung (laden/entladen/stopp)
- BMS-Care vorhanden, aber weniger "weich" integriert (mehr Block-/Sonderfaelle)
- Keine zentrale Registermap (`HA_REGMAP`) fuer einheitliche Skalierung/Typisierung
- Kein PV-Surplus Zeitfilter (hoeheres Risiko fuer Wolken-PingPong)
- SOC-SAFETY entweder nicht vorhanden oder ohne vollstaendige Persistenz/Automatik
- Logging Tabelle vorhanden, aber weniger Zustands-/Timeline-Infos
