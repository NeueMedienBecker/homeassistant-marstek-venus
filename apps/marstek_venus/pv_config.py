# =============================================================================
# pv_config.py
#
# Externe Konfiguration fuer pv_control.py (Marstek Venus / AppDaemon)
#
# ZWECK:
#   Alle steuerbaren Parameter sind hier zentralisiert – pv_control.py selbst
#   enthaelt keine fest codierten Werte. Aenderungen hier werden beim naechsten
#   AppDaemon-Neustart automatisch uebernommen (kein Deployment noetig).
#
# LADEN:
#   pv_control.py laedt diese Datei per importlib.util.spec_from_file_location
#   anhand des absoluten Pfads relativ zu __file__. Damit ist kein sys.path-
#   Eintrag noetig und __pycache__-Probleme werden vermieden.
#
# NAMENSKONVENTION:
#   Die Datei heisst auf dem Server "pv_config.py" (nicht pv_control_config.py).
#   Der Name in diesem Repo ist nur fuer die Entwicklung.
#
# ABSCHNITTE:
#   1.  API / Kommunikation           – UDP-Port, Timeout, Retry
#   2.  HA-Export                     – Prefix, Taktung
#   3.  Modbus Register-Map           – welche Register nach HA exportiert werden
#   3b. API Export-Map                – welche API-Keys nach HA exportiert werden
#   4.  Entity-ID-Mapping             – feste HA-Entity-IDs
#   5.  Logging-Flags                 – was geloggt wird
#   6.  Basis-Parameter               – Zykluszeit, Toleranz, Stabilitaet
#   7.  Modbus-Client-Parameter       – Timeout, Retries
#   8.  Setpoint-Verifikation         – Schreibpruefung
#   9.  SOC-Safety                    – Notfall-Ladung bei lang anhaltendem MinSoC
#  10.  Log-Retention                 – automatische Log-Bereinigung
#  11.  PV-Surplus-Zeitfilter         – Entprellzeit fuer PV-Ueberschuss
#  12.  Grid-Follow                   – Totband und Stop-Timer
#  13.  PV-Ladeverteilung             – Bucket- und Rotationslogik
#  14.  BMS / Battery Care            – Voll-Lade-Zyklen fuer Zellbalancierung
#  15.  Control-Register-Werte        – Modbus 42000 Steuerwerte
#  16.  Smartmeter                    – Netz-Zaehler Entity
#  17.  Modbus-Register-Adressen      – Adressen der Steuer-Register
#  18.  Batterie-Konfiguration        – IP, Port, Protokoll, Grenzen je Batterie
#  19.  Tabellen-Layout               – Spalten der Log-Ausgabe
# =============================================================================


# =============================================================================
# 1. API / Kommunikation
#
#    Einstellungen fuer das Marstek Open API Protokoll (UDP/JSON-RPC Rev 1.0).
#    Wird nur verwendet wenn comm_mode="api" in BATTERY_CONFIG gesetzt ist.
# =============================================================================

# Globaler Fallback-Kommunikationsmodus fuer Batterien ohne expliziten comm_mode.
# "modbus" = Modbus TCP (benoetigt LAN/RS485-Adapter)
# "api"    = Marstek Open API UDP (direkt per WLAN an die Batterie)
COMM_MODE_DEFAULT = "api"

# Standard-UDP-Port gemaess Marstek Open API Dokumentation Rev 1.0.
# Aeltere Firmware-Versionen (<= V154) verwenden 30000.
# Ab V155 hat Marstek (per App konfigurierbar).
# Achtung: Dieser Wert wird von api_port in BATTERY_CONFIG ueberschrieben,
# falls dort ein abweichender Port eingetragen ist.
API_UDP_PORT = 30000

# FIX 33: Wartezeit auf eine UDP-Antwort der Batterie (Sekunden). Die Batterie antwortet
# normalerweise in unter 0,3 s; ein verlorenes Paket wird nach API_RETRY_DELAY_S wiederholt.
API_TIMEOUT_S = 1.5
# Versuche pro Aufruf und Pause zwischen den Versuchen (Sekunden).
API_MAX_ATTEMPTS = 3
API_RETRY_DELAY_S = 0.3
# Aufwaermzeit nach dem Oeffnen des UDP-Sockets (Sekunden). Die Batterie ignoriert die ersten
# Pakete eines frisch geoeffneten Sockets; der Socket bleibt danach dauerhaft offen.
API_WARMUP_S = 1.5
# Lesetakt der API in Sekunden (Antworten werden so lange zwischengespeichert). 15 = jeder Zyklus.
API_READ_S = 15
# Bleibt eine Antwort aus, werden die letzten Werte so lange weiterverwendet (Sekunden).
# Danach gilt die Batterie als nicht erreichbar und bekommt keinen Anteil mehr.
API_STALE_MAX_S = 120
# Sollwert nur senden, wenn er sich um mindestens so viele Watt aendert.
# (Ein laufender Sollwert wird unabhaengig davon vor Ablauf von API_CD_TIME_S aufgefrischt.)
API_SEND_DELTA_W = 50

# Laenge des Passive-Mode-Countdowns in Sekunden.
# Im Passive-Modus schaltet die Batterie automatisch auf STOP um,
# wenn innerhalb dieser Zeit kein neues ES.SetMode-Kommando eingeht.
# Muss groesser sein als INTERVAL (Zykluszeit), sonst stoppt die Batterie
# zwischen zwei Steuerzyklen. Empfehlung: INTERVAL * 3 = 60s oder 90s.
API_CD_TIME_S = 300   # Marstek API Beispiel zeigt 300s – muss > Zykluszeit sein

# Lokaler UDP-Quellport fuer API-Anfragen.
# FIX 34: ALLE API-Batterien teilen sich EINEN Socket auf diesem Port; die Antworten werden
# nach Absender-IP verteilt. Grund: neuere Firmware (z.B. Venus E 1573) antwortet nur an
# Quell-Port 30000, aeltere (z.B. Venus E 3.0 v148) an jeden Port. api_local_port je Batterie
# wird nicht mehr benoetigt (wird ignoriert).
API_LOCAL_PORT = 30000

# Erweitertes Diagnose-Logging fuer die UDP-Kommunikation.
# True = loggt lokalen Port, Ziel-IP:Port, Byte-Anzahl und Timing pro Versuch.
# Genuegt um Port/IP/Timeout-Probleme zu identifizieren ohne JSON-Dumps.
# Empfehlung: True bei Inbetriebnahme oder nach Timeout-Fehlern.
API_LOG_DIAG = False

# Vollstaendiges Payload-Logging (JSON-Dumps jeder Anfrage und Antwort).
# Nur fuer tiefgehende Protokoll-Diagnose – erzeugt sehr viele Eintraege.
API_LOG_TXRX = False
# FIX 37: Einheiten der Open API sind je Firmware verschieden.
# Venus E (Hardware 2.0, Firmware ab 154, z.B. 1573) liefert bat_temp in 10-Grad-Schritten
# und die Netz-Energiezaehler in 0,1 kWh; Venus E 3.0 (Firmware 148) liefert Grad und Wh.
# Beobachtung 29.09.: Batterie 1 API 12,223 kWh gegen Modbus 1222,25 kWh, Temperatur 2 statt 30.
# Gleiche Regel wie jaapp/ha-marstek-local-api. Das Script erkennt Modell und Firmware selbst
# (Marstek.GetDevice) und rechnet um; Modbus-Werte sind nicht betroffen.
API_FW_SCALE = {
    "hw2_fw154": {
        ("Bat.GetStatus", "bat_temp"): 10.0,
        ("ES.GetStatus", "total_grid_input_energy"): 100.0,
        ("ES.GetStatus", "total_grid_output_energy"): 100.0,
    },
}

# Mindest-Pause zwischen zwei aufeinanderfolgenden UDP-Aufrufen an dieselbe
# Batterie in Sekunden. Die Marstek-Firmware kann Anfragen die nur wenige
# Millisekunden nach der vorherigen Antwort eintreffen nicht verarbeiten
# und antwortet dann nicht (Timeout).
# 0.3s (300 ms) reicht erfahrungsgemaess aus.
# 0.0 = keine Verzoegerung (original, fuehrt zu sporadischen Timeouts).
API_INTER_CALL_DELAY_S = 1.0  # FIX 33: 0.3 s war zu kurz - Anfragen <1 s nach der letzten Antwort gehen verloren

# Nach dem letzten fehlgeschlagenen Versuch noch X Sekunden weiter lauschen.
# Ergebnis im Log:
#   API SPAETE-ANTWORT  -> Geraet antwortet, aber nach >API_TIMEOUT Sekunden
#                          Loesung: API_TIMEOUT in diesem File erhoehen
#   API KEINE-ANTWORT   -> Geraet sendet wirklich gar nichts zurueck
#                          Haeufigste Ursache: Open API nicht aktiviert (siehe unten)
# 0 = Diagnose deaktiviert.
API_LATE_RESPONSE_WINDOW_S = 3.0

# Wartezeit (Sekunden) nach einem Lese-/Verbindungsfehler bevor die
# naechste Abfrage an diese Batterie gesendet wird.
# Niedrigerer Wert = schnellere Recovery nach kurzzeitigen Aussetzern (z.B. 403).
# Zu niedrig = Fluten bei echtem Ausfall. Empfehlung: 10
API_ERROR_BACKOFF_S = 15

# Wartezeit nach einem einzelnen fehlgeschlagenen Register-Lesevorgang (Modbus).
# Kuerzer als API_ERROR_BACKOFF_S weil nur ein Register betroffen ist.
MODBUS_REG_ERROR_BACKOFF_S = 5

# =============================================================================
# WICHTIG: Marstek Open API aktivieren (laut API Rev 2.0, Abschnitt 2.2.1)
#
# Wenn das Geraet mit Fehler data=403 antwortet oder gar nicht antwortet:
#   1. Marstek APP oeffnen
#   2. Geraet auswaehlen
#   3. Einstellungen -> Open API -> Aktivieren
#   4. UDP-Port auf 30000 setzen (oder den hier konfigurierten Wert)
#
# Fehlercode-Referenz (Marstek Open API Rev 2.0):
#   code=-32700, data=402  -> Broadcast-Paket abgelehnt (Discovery-Anfrage)
#   code=-32700, data=403  -> API nicht aktiviert / Zugriff gesperrt
#   code=-32600            -> Kein gueltiges JSON-RPC Request-Objekt
#   code=-32601            -> Methode unbekannt (Modell unterstuetzt sie nicht)
#   code=-32602            -> Ungueltige Parameter
#   code=-32603            -> Interner Geratefehler
# =============================================================================


# =============================================================================
# 1b. Treiber-Fallback (Automatischer Wechsel bei Verbindungsausfall)
#
#    Wenn ein Treiber (API oder Modbus) wiederholt nicht antwortet,
#    wechselt pv_control.py automatisch zum anderen Protokoll.
#    Nach COMM_MODE_FALLBACK_RETRY_MIN Minuten wird versucht zum
#    primaeren Treiber zurueckzukehren.
#
#    Beispiel comm_mode="api": API nicht erreichbar -> Wechsel auf Modbus.
#    Nach RETRY_MIN Minuten wird erneut versucht die API zu erreichen.
#    Gelingt das -> Wechsel zurueck auf API.
#
#    Voraussetzung: Beide Kommunikationswege muessen in BATTERY_CONFIG
#    korrekt konfiguriert sein (api_host UND modbus_host / port).
# =============================================================================

# Fallback-Automatik aktivieren.
# False = kein automatischer Treiberwechsel (wie bisher).
COMM_MODE_FALLBACK_ENABLE = False  # Ausweichweg wird je Batterie im Dashboard gewaehlt (Anbindung)

# Anzahl aufeinanderfolgender Lesefehler bevor der Treiber gewechselt wird.
# 3 = nach 3 fehlgeschlagenen Zyklen (= 3 * INTERVAL = 60s bei INTERVAL=20s).
# Nicht zu niedrig setzen um kurze WLAN-Aussetzer nicht als Ausfall zu werten.
COMM_MODE_FALLBACK_FAILURES = 3

# Minuten die gewartet wird bevor ein Rueckwechsel zum Primaer-Treiber versucht wird.
# 10 Minuten = nach 10 min wird einmalig geprueft ob der Primaer-Treiber wieder antwortet.
# Gelingt der Rueckwechsel nicht, bleibt der Fallback-Treiber aktiv und
# ein neuer Versuch erfolgt nach weiteren RETRY_MIN Minuten.
COMM_MODE_FALLBACK_RETRY_MIN = 10


# =============================================================================
# 2. HA-Export
#
#    Steuert wie Batterie-Messwerte als Sensoren in Home Assistant
#    veroeffentlicht werden (ueber AppDaemon set_state).
# =============================================================================

# Export aktivieren/deaktivieren.
# False = keine Sensoren in HA erstellen (nur Steuerung, kein Monitoring).
HA_EXPORT_ENABLE = True

# Praefix fuer alle HA-Entity-IDs.
# Ergibt z.B.: sensor.marstekvenus_1_battery_soc
# Aendern wenn mehrere Instanzen im gleichen HA laufen sollen.
HA_EXPORT_PREFIX = "marstekvenus"

# Anzahl der HA-Sensoren die pro Steuerzyklus aktualisiert werden.
# Niedrigerer Wert = gleichmaessigere Verteilung der set_state-Aufrufe,
# reduziert kurze Spitzen in der AppDaemon-API-Last.
# Alle Sensoren werden ueber mehrere Zyklen hinweg reihum aktualisiert.
HA_EXPORT_KEYS_PER_CYCLE = 4

# Optionale manuelle Metadaten je Batterie fuer den HA-Export.
# idx: wird als Suffix in der Entity-ID und im friendly_name verwendet
#      (z.B. sensor.marstekvenus_1_... / "MarstekVenus 1 ...")
#
# Wird hier NICHTS eingetragen, wird idx automatisch aus dem Suffix des
# Batterienamens abgeleitet:  battery1 -> idx=1,  battery4 -> idx=4, usw.
#
# Nur Batterien eintragen die vom Automatismus ABWEICHEN sollen:
#   z.B. "battery3": {"idx": 99}  -> sensor.marstekvenus_99_...
_BATTERY_META_OVERRIDE = {
    # "battery3": {"idx": 99},   # Beispiel: eigene Nummer vergeben
}


# =============================================================================
# 3. Modbus Register-Map (HA-Sensoren, nur bei comm_mode="modbus")
#
#    Legt fest welche Modbus-Register nach HA exportiert werden.
#    Wird nur ausgewertet wenn die Batterie comm_mode="modbus" hat.
#
#    Felder je Eintrag:
#      enabled : True  = wird gelesen und nach HA exportiert
#                False = wird uebersprungen (kein Modbus-Zugriff)
#      addr    : Modbus-Registeradresse (Holding Register, 0-basiert)
#      cnt     : Anzahl der zu lesenden Register (1 = 16 Bit, 2 = 32 Bit)
#      type    : Datentyp des Registers
#                  u16 = unsigned 16-Bit (0..65535)
#                  s16 = signed 16-Bit (-32768..32767)
#                  u32 = unsigned 32-Bit (2 Register, Big-Endian)
#                  s32 = signed 32-Bit (2 Register, Big-Endian)
#      scale   : Multiplikator (Rohwert * scale = physikalischer Wert)
#      unit    : Einheit fuer HA (z.B. "V", "W", "kWh", "%")
#      prec    : Nachkommastellen des exportierten Werts
#      name    : Anzeigename in HA (friendly_name)
# =============================================================================

HA_REGMAP = {
    # Batteriespannung in Volt (Rohwert in 10 mV, scale=0.01 ergibt V)
    "battery_voltage":          {"enabled": True,  "addr": 32100, "cnt": 1, "type": "u16", "scale": 0.01,   "unit": "V",   "prec": 2, "name": "Battery Voltage"},

    # State of Charge in Prozent (0-100)
    "battery_soc":              {"enabled": True,  "addr": 32104, "cnt": 1, "type": "u16", "scale": 1.0,    "unit": "%",   "prec": 0, "name": "Battery SoC"},

    # Verbleibende Energie in kWh (Rohwert in 100 Wh, scale=0.0001 ergibt kWh)
    "battery_energy":           {"enabled": True,  "addr": 32105, "cnt": 1, "type": "u16", "scale": 0.0001, "unit": "kWh", "prec": 4, "name": "Battery Energy"},

    # AC-Leistung am Netzanschluss in Watt (positiv = Einspeisung, negativ = Bezug)
    # 2 Register = 32-Bit-Wert notwendig wegen +/-2500 W Wertebereich
    "ac_power":                 {"enabled": True,  "addr": 32202, "cnt": 2, "type": "s32", "scale": 1.0,    "unit": "W",   "prec": 0, "name": "AC Power"},

    # Gesamte geladene Energie seit Inbetriebnahme (Rohwert in 10 Wh, scale=0.01 ergibt kWh)
    "total_charging_energy":    {"enabled": True,  "addr": 33000, "cnt": 2, "type": "u32", "scale": 0.01,   "unit": "kWh", "prec": 4, "name": "Total Charging Energy"},

    # Gesamte entladene Energie seit Inbetriebnahme
    "total_discharging_energy": {"enabled": True,  "addr": 33002, "cnt": 2, "type": "u32", "scale": 0.01,   "unit": "kWh", "prec": 4, "name": "Total Discharging Energy"},

    # Interne Gehaeusetemperatur (Rohwert in 0.1 C, scale=0.1 ergibt C)
    "temp_internal":            {"enabled": True,  "addr": 35000, "cnt": 1, "type": "s16", "scale": 0.1,    "unit": "C",   "prec": 1, "name": "Temp Internal"},

    # Temperatur MOSFET-Treiber 1 (Leistungsschalter Ladeseitig)
    "temp_mos1":                {"enabled": True,  "addr": 35001, "cnt": 1, "type": "s16", "scale": 0.1,    "unit": "C",   "prec": 1, "name": "Temp MOS1"},

    # Temperatur MOSFET-Treiber 2 (Leistungsschalter Entladeseitig)
    "temp_mos2":                {"enabled": True,  "addr": 35002, "cnt": 1, "type": "s16", "scale": 0.1,    "unit": "C",   "prec": 1, "name": "Temp MOS2"},

    # Maximaltemperatur der heissesten Zelle (deaktiviert: erzeugt viele Sensoren)
    "temp_cell_max":            {"enabled": False, "addr": 35010, "cnt": 1, "type": "s16", "scale": 0.1,    "unit": "C",   "prec": 1, "name": "Temp Cell Max"},

    # Minimaltemperatur der kaeltesten Zelle
    "temp_cell_min":            {"enabled": False, "addr": 35011, "cnt": 1, "type": "s16", "scale": 0.1,    "unit": "C",   "prec": 1, "name": "Temp Cell Min"},

    # Wechselrichter-Zustandscode (herstellerspezifisch, fuer Diagnose)
    "inverter_state":           {"enabled": True,  "addr": 35100, "cnt": 1, "type": "u16", "scale": 1.0,    "unit": "",    "prec": 0, "name": "Inverter State"},

    # Maximale Ladeschlussspannung (selten benoetigt)
    "chg_v_limit":              {"enabled": False, "addr": 35110, "cnt": 1, "type": "u16", "scale": 0.1,    "unit": "V",   "prec": 1, "name": "Charge V Limit"},

    # RS485-Steuerregister (42000): Wert 21930/21931 = Steuerung freigegeben
    # Nur zur Diagnose – wird von pv_control.py aktiv geschrieben
    "rs485_control":            {"enabled": False, "addr": 42000, "cnt": 1, "type": "u16", "scale": 1.0,    "unit": "",    "prec": 0, "name": "RS485 Control"},

    # Ziel-SoC fuer Ladekommando (Register 42011)
    "charge_to_soc":            {"enabled": False, "addr": 42011, "cnt": 1, "type": "u16", "scale": 1.0,    "unit": "%",   "prec": 0, "name": "Charge To SoC"},

    # Arbeitsmodus-Register (43000): 0=Auto, 1=Manuell, ...
    "user_work_mode":           {"enabled": False, "addr": 43000, "cnt": 1, "type": "u16", "scale": 1.0,    "unit": "",    "prec": 0, "name": "User Work Mode"},
}


# =============================================================================
# 3a. Berechnete HA-Sensoren (abgeleitet aus Modbus/API-Werten)
#
#    Diese Sensoren werden nicht direkt vom Geraet gelesen, sondern aus einem
#    vorhandenen Quell-Sensor berechnet und automatisch nach HA exportiert.
#
#    Typischer Anwendungsfall: HA Energy-Dashboard -> "Zwei Sensoren" Modus
#    benoetigt separate Lade- und Entladeleistung statt eines vorzeichenbehafteten
#    AC-Power-Werts.
#
#    Felder je Eintrag:
#      enabled   : True/False
#      source    : Quell-Sensor-Key (muss in HA_REGMAP oder API_EXPORT_MAP existieren
#                  und dort ebenfalls enabled sein)
#      transform : Umrechnungsregel:
#                    "pos"      -> max(0, wert)         Nur positive Werte
#                    "neg_abs"  -> abs(min(0, wert))    Betrag negativer Werte
#                    "abs"      -> abs(wert)            Absolutwert
#                    "negate"   -> -wert                Vorzeichen umkehren
#      unit      : Einheit fuer HA
#      prec      : Nachkommastellen
#      name      : Anzeigename in HA (wird zu "MarstekVenus {idx} {name}")
#
#    Hinweis Vorzeichen ac_power (Register 32202):
#      positiv  = Einspeisung ins Netz  (Batterie entlaedt)
#      negativ  = Bezug aus dem Netz    (Batterie laedt)
# =============================================================================

HA_COMPUTED_SENSORS = {
    # Entladeleistung fuer HA Energy-Dashboard (nur positive ac_power-Werte)
    "discharge_power":  {"enabled": True,  "source": "ac_power", "transform": "pos",     "unit": "W", "prec": 0, "name": "Discharge Power"},

    # Ladeleistung fuer HA Energy-Dashboard (Betrag negativer ac_power-Werte)
    "charge_power":     {"enabled": True,  "source": "ac_power", "transform": "neg_abs", "unit": "W", "prec": 0, "name": "Charge Power"},
}


# =============================================================================
# 3b. API Export-Map (HA-Sensoren, nur bei comm_mode="api")
#
#    Legt fest welche API-Schluessel nach HA exportiert werden.
#    Wird nur ausgewertet wenn die Batterie comm_mode="api" hat.
#
#    Felder je Eintrag:
#      enabled : True  = wird abgerufen und nach HA exportiert
#                False = wird uebersprungen (kein API-Call fuer diesen Key)
#      method  : JSON-RPC-Methode (z.B. "ES.GetStatus")
#                Mehrere Keys aus derselben Methode erzeugen nur einen API-Call
#                (Caching innerhalb des Zyklus).
#      key     : Schluessel im result-Objekt der JSON-Antwort
#      unit    : Einheit fuer HA
#      scale   : Multiplikator (Rohwert * scale = physikalischer Wert)
#      prec    : Nachkommastellen
#      name    : Anzeigename in HA
#
#    API-Methoden Uebersicht (Marstek Open API Rev 1.0):
#      ES.GetStatus   Energie-System Grundwerte (SoC, Leistungen, Energie-Zaehler)
#      Bat.GetStatus  Batterie-Details (Temp, Flags, Kapazitaet)
#      PV.GetStatus   PV-Eingang (nur Venus D mit integrierten Wechselrichter-Inputs)
#      EM.GetStatus   Energiezaehler / CT-Sensor Messwerte
#      ES.GetMode     Aktuell eingestellter Betriebsmodus + Parameter
#      Wifi.GetStatus WLAN-Verbindungsdaten (IP, SSID, RSSI)
# =============================================================================

API_EXPORT_MAP = {

    # -------------------------------------------------------------------------
    # ES.GetStatus – Energie-System Grundwerte
    # Immer verfuegbar, unabhaengig von Modell und Firmware-Version.
    #
    # canonical_key: HA-Entity-Key der verwendet wird (= identisch mit Modbus-Key
    # wo ein Aequivalent existiert). Dadurch sind Modbus- und API-Sensoren in HA
    # nicht unterscheidbar – der Treiberwechsel ist fuer HA unsichtbar.
    # -------------------------------------------------------------------------

    # SoC – entspricht Modbus "battery_soc"
    "es_bat_soc":                   {"enabled": True,  "method": "ES.GetStatus",   "key": "bat_soc",                  "scale": 1.0,   "unit": "%",   "prec": 0, "name": "Battery SoC",               "canonical_key": "battery_soc"},

    # Verbleibende Kapazitaet in Wh (API-only, kein Modbus-Aequivalent als Wh)
    "es_bat_cap":                   {"enabled": True,  "method": "ES.GetStatus",   "key": "bat_cap",                  "scale": 1.0,   "unit": "Wh",  "prec": 0, "name": "Battery Capacity Wh",       "canonical_key": "battery_capacity_wh"},

    # PV-Eingangsleistung (API-only bei Venus E)
    "es_pv_power":                  {"enabled": True,  "method": "ES.GetStatus",   "key": "pv_power",                 "scale": 1.0,   "unit": "W",   "prec": 0, "name": "PV Power",                  "canonical_key": "pv_power"},

    # Netzleistung – entspricht Modbus "ac_power"
    "es_ongrid_power":              {"enabled": True,  "method": "ES.GetStatus",   "key": "ongrid_power",             "scale": 1.0,   "unit": "W",   "prec": 0, "name": "AC Power",                  "canonical_key": "ac_power"},

    # Offgrid/EPS-Ausgangsleistung (API-only)
    "es_offgrid_power":             {"enabled": False, "method": "ES.GetStatus",   "key": "offgrid_power",            "scale": 1.0,   "unit": "W",   "prec": 0, "name": "Offgrid Power",             "canonical_key": "offgrid_power"},

    # Batterieleistung (API-only, Modbus-Register vorhanden aber nicht aktiv exportiert)
    # Vorzeichen API: positiv=Laden, negativ=Entladen (INVERS zu Modbus!)
    "es_bat_power":                 {"enabled": True,  "method": "ES.GetStatus",   "key": "bat_power",                "scale": 1.0,   "unit": "W",   "prec": 0, "name": "Battery Power",             "canonical_key": "battery_power"},

    # Kumulierte PV-Energie (API-only)
    "es_total_pv_energy":           {"enabled": True,  "method": "ES.GetStatus",   "key": "total_pv_energy",          "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Total PV Energy",           "canonical_key": "total_pv_energy"},

# FIX 37: Netzeinspeisung der Batterie = Entladezaehler -> gleiche Entitaet wie Modbus-Register 33002
    "es_total_grid_output_energy":  {"enabled": True,  "method": "ES.GetStatus",   "key": "total_grid_output_energy", "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Total Discharging Energy",  "canonical_key": "total_discharging_energy"},

# FIX 37: Netzbezug der Batterie = Ladezaehler -> gleiche Entitaet wie Modbus-Register 33000
    "es_total_grid_input_energy":   {"enabled": True,  "method": "ES.GetStatus",   "key": "total_grid_input_energy",  "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Total Charging Energy",   "canonical_key": "total_charging_energy"},

    # Kumulierter Haushaltsverbrauch (nur mit CT-Sensor)
    "es_total_load_energy":         {"enabled": False, "method": "ES.GetStatus",   "key": "total_load_energy",        "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Total Load Energy",         "canonical_key": "total_load_energy"},

    # -------------------------------------------------------------------------
    # Bat.GetStatus – Batterie-Details
    # -------------------------------------------------------------------------

    # SoC doppelt – deaktiviert, bereits ueber es_bat_soc abgedeckt
    "bat_soc":                      {"enabled": False, "method": "Bat.GetStatus",  "key": "soc",                      "scale": 1.0,   "unit": "%",   "prec": 0, "name": "Battery SoC",               "canonical_key": "battery_soc"},

    # Ladefreigabe (API-only, kein Modbus-Sensor)
    "bat_charg_flag":               {"enabled": True,  "method": "Bat.GetStatus",  "key": "charg_flag",               "scale": 1.0,   "unit": "",    "prec": 0, "name": "Charge Allowed",            "canonical_key": "charge_allowed"},

    # Entladefreigabe (API-only, kein Modbus-Sensor)
    "bat_dischrg_flag":             {"enabled": True,  "method": "Bat.GetStatus",  "key": "dischrg_flag",             "scale": 1.0,   "unit": "",    "prec": 0, "name": "Discharge Allowed",         "canonical_key": "discharge_allowed"},

    # Batterietemperatur – entspricht Modbus "temp_internal"
    "bat_temp":                     {"enabled": True,  "method": "Bat.GetStatus",  "key": "bat_temp",                 "scale": 1.0,   "unit": "C",   "prec": 1, "name": "Temp Internal",             "canonical_key": "temp_internal"},

    # Verbleibende Kapazitaet in kWh – entspricht Modbus "battery_energy"
    "bat_capacity":                 {"enabled": True,  "method": "Bat.GetStatus",  "key": "bat_capacity",             "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Battery Energy",            "canonical_key": "battery_energy"},

    # Nennkapazitaet laut Typenschild (API-only)
    "bat_rated_capacity":           {"enabled": False, "method": "Bat.GetStatus",  "key": "rated_capacity",           "scale": 0.001, "unit": "kWh", "prec": 3, "name": "Battery Rated Capacity",    "canonical_key": "battery_rated_capacity"},

    # -------------------------------------------------------------------------
    # PV.GetStatus – PV-Eingang (nur Venus D)
    # -------------------------------------------------------------------------

    "pv_power":                     {"enabled": False, "method": "PV.GetStatus",   "key": "pv_power",                 "scale": 1.0,   "unit": "W",   "prec": 0, "name": "PV Power Direct",           "canonical_key": "pv_power_direct"},
    "pv_voltage":                   {"enabled": False, "method": "PV.GetStatus",   "key": "pv_voltage",               "scale": 1.0,   "unit": "V",   "prec": 1, "name": "PV Voltage",                "canonical_key": "pv_voltage"},
    "pv_current":                   {"enabled": False, "method": "PV.GetStatus",   "key": "pv_current",               "scale": 1.0,   "unit": "A",   "prec": 2, "name": "PV Current",                "canonical_key": "pv_current"},

    # -------------------------------------------------------------------------
    # EM.GetStatus – CT-Sensor
    # -------------------------------------------------------------------------

    "em_ct_state":                  {"enabled": False, "method": "EM.GetStatus",   "key": "ct_state",                 "scale": 1.0,   "unit": "",    "prec": 0, "name": "CT State",                  "canonical_key": "em_ct_state"},
    "em_total_power":               {"enabled": False, "method": "EM.GetStatus",   "key": "total_power",              "scale": 1.0,   "unit": "W",   "prec": 0, "name": "EM Total Power",            "canonical_key": "em_total_power"},
    "em_a_power":                   {"enabled": False, "method": "EM.GetStatus",   "key": "a_power",                  "scale": 1.0,   "unit": "W",   "prec": 0, "name": "EM Phase A Power",          "canonical_key": "em_a_power"},
    "em_b_power":                   {"enabled": False, "method": "EM.GetStatus",   "key": "b_power",                  "scale": 1.0,   "unit": "W",   "prec": 0, "name": "EM Phase B Power",          "canonical_key": "em_b_power"},
    "em_c_power":                   {"enabled": False, "method": "EM.GetStatus",   "key": "c_power",                  "scale": 1.0,   "unit": "W",   "prec": 0, "name": "EM Phase C Power",          "canonical_key": "em_c_power"},

    # -------------------------------------------------------------------------
    # ES.GetMode – Betriebsmodus
    # -------------------------------------------------------------------------

    "es_mode":                      {"enabled": True, "method": "ES.GetMode",     "key": "mode",                     "scale": 1.0,   "unit": "",    "prec": 0, "name": "ES Mode",                   "canonical_key": "es_mode"},  # FIX 33
    "es_mode_ongrid_power":         {"enabled": True, "method": "ES.GetMode",     "key": "ongrid_power",             "scale": 1.0,   "unit": "W",   "prec": 0, "name": "ES Mode Grid Power",        "canonical_key": "es_mode_ongrid_power"},  # FIX 33
    "es_mode_bat_soc":              {"enabled": False, "method": "ES.GetMode",     "key": "bat_soc",                  "scale": 1.0,   "unit": "%",   "prec": 0, "name": "ES Mode Battery SoC",       "canonical_key": "es_mode_bat_soc"},

    # -------------------------------------------------------------------------
    # Wifi.GetStatus – WLAN
    # -------------------------------------------------------------------------

    "wifi_rssi":                    {"enabled": False, "method": "Wifi.GetStatus", "key": "rssi",                     "scale": 1.0,   "unit": "dBm", "prec": 0, "name": "WiFi RSSI",                 "canonical_key": "wifi_rssi"},
    "wifi_ssid":                    {"enabled": False, "method": "Wifi.GetStatus", "key": "ssid",                     "scale": 1.0,   "unit": "",    "prec": 0, "name": "WiFi SSID",                 "canonical_key": "wifi_ssid"},
    "wifi_ip":                      {"enabled": False, "method": "Wifi.GetStatus", "key": "sta_ip",                   "scale": 1.0,   "unit": "",    "prec": 0, "name": "WiFi IP",                   "canonical_key": "wifi_ip"},
}


# =============================================================================
# 4. Entity-ID-Mapping
#
#    Erzwingt feste HA-Entity-IDs fuer bestimmte (Batterie-Index, Key)-Paare.
#    Hintergrund: HA vergibt bei Duplikaten automatisch Suffixe (_2, _3, ...).
#    Da alle drei Batterien gleich heissen, bekommen Batterie 2+3 das Suffix.
#    Damit Energie-Statistiken in HA erhalten bleiben (z.B. fuer Dashboards),
#    werden die IDs hier fest verankert.
#
#    Format: {(batterie_idx, schluessel): "sensor.entity_id"}
#    Paare die hier nicht eingetragen sind, erhalten automatisch generierte IDs.
# =============================================================================

# Normalerweise leer lassen (nur fuer alte Installationen mit Entitaeten wie ..._2).
HA_ENTITY_ID_MAP = {}


# =============================================================================
# 5. Logging-Flags
#
#    Jedes Flag steuert eine bestimmte Gruppe von Log-Ausgaben.
#    Fuer Produktivbetrieb empfohlen: nur CALC_LOG, LOG_DISCHARGE, DEBUG_STATE.
# =============================================================================

# Vorzeichen-Interpretation der AC-Leistung invertieren.
# False = normal (positiv = Einspeisung, negativ = Bezug)
# True  = invertiert (z.B. bei manchen Modbus-Adaptern die das Vorzeichen drehen)
AC_SIGN_INVERT = False

# Diagnose-Log: Vorzeichen von AC_power und bat_power je Batterie pro Zyklus.
# Hilfreich beim ersten Einrichten um sicherzustellen dass +/- korrekt ist.
LOG_DIAG_SIGN = False

# Diagnose-Log: Grid-Follow Basiswerte (Netzleistung, Sollwert, Modus).
# Zeigt was der Grid-Follow-Algorithmus berechnet und warum er umschaltet.
LOG_DIAG_GRID = False

# Modbus: Erfolgreiche Lese- und Schreibvorgaenge loggen.
# Erzeugt sehr viele Eintraege (jeder Zyklus = mehrere Reads + 1 Write).
MODBUS_LOG_OK = False

# Modbus: Rohe TX/RX-Frames loggen (vollstaendige Modbus-PDU als Hex).
# Nur fuer tiefgehende Protokoll-Diagnose.
MODBUS_LOG_TXRX = False

# Modbus: Fehlermeldungen loggen.
# Empfohlen: False, da Fehler ohnehin im normalen Fehlerlog erscheinen.
MODBUS_LOG_ERROR = False

# Interne pymodbus Debug-Ausgaben aktivieren.
# Erzeugt sehr viele Eintraege, nur fuer Bibliotheks-Diagnose.
MODBUS_DEBUG = False

# Berechnungslogging: Grid-Follow-Ergebnis, Verteilungsrechnung, Modus-Wechsel.
# Zeigt warum pv_control.py Laden/Entladen/Stopp entschieden hat.
CALC_LOG = True

# Entlade-Ereignisse explizit loggen (Start, Ende, kWh).
LOG_DISCHARGE = True

# Rohzustand jeder Batterie zu Beginn jedes Zyklus (SoC, AC, Ctrl, Mode, ...).
# Wichtig fuer die Fehlersuche – empfohlen: True.
DEBUG_STATE = False

# Gewichtsberechnung bei der Lastverteilung zwischen mehreren Batterien.
# Zeigt wie die Ladeleistung aufgeteilt wird (Bucket, Rotation, SoC-Ausgleich).
DEBUG_WEIGHTS = False

# Gesetzte Sollwerte nach jedem Steuerzyklus (charge_set, discharge_set, mode).
DEBUG_SETPOINTS = False


# =============================================================================
# 6. Basis-Parameter
# =============================================================================

# Totband fuer AC/BP-Messwerte in Watt.
# Werte innerhalb +/- TOLERANCE_W gelten als "Null" (kein Laden/Entladen).
# Verhindert Flattern bei kleinen Schwankungen am Zaehler.
TOLERANCE_W = 40

# Zykluszeit in Sekunden (AppDaemon run_every Intervall).
# Jede INTERVAL Sekunden liest pv_control.py den Zustand und setzt neue Sollwerte.
# Kleineres Intervall = schnellere Reaktion, aber mehr Systemlast.
# Empfehlung: 20s (Marstek benoetigt nach einem Schreibkommando ca. 5s bis der
# Istwert sich stabilisiert hat).
INTERVAL = 15

# Mindestdauer in Sekunden, die ein Modus stabil anliegen muss,
# bevor er zu einem anderen Modus wechseln darf.
# Verhindert schnelles Hin- und Herschalten (Hysterese).
STABLE_TIMER_S = 20


# =============================================================================
# 7. Modbus-Client-Parameter
# =============================================================================

# TCP-Verbindungs-Timeout in Sekunden.
# Bei Unterschreitung (Adapter nicht erreichbar) wird die Verbindung als Fehler gewertet.
MB_TIMEOUT_S = 2  # FIX 30: vorher 1 s; spaete Antworten (>1 s) erzeugten transaction_id-Fehler

# Anzahl der Wiederholversuche bei einem fehlgeschlagenen Modbus-Request.
# 1 = ein Retry (insgesamt 2 Versuche). Hoehere Werte verlangsamen die Fehlerbehandlung.
MB_RETRIES = 2


# =============================================================================
# 8. Setpoint-Verifikation
#
#    Nach dem Schreiben eines Sollwerts per Modbus wird der AC-Istwert
#    nochmals gelesen und mit dem Sollwert verglichen.
#    Dient zur Fruehwarnung wenn das Geraet Kommandos ignoriert.
# =============================================================================

# Verifikation aktivieren (False = kein Ruecklesezyklus nach dem Schreiben).
# Deaktivieren wenn der Ruecklesezyklus zu Timing-Problemen fuehrt.
VERIFY_SETPOINT_EXEC = False

# Minimaler Sollwert ab dem eine Verifikation sinnvoll ist (W).
# Sehr kleine Werte (< 50 W) erzeugen durch Messrauschen oft falsche Alarme.
VERIFY_MIN_SET_W = 50

# Toleranz bei der Pruefung (W).
# Der Istwert darf um +/- VERIFY_TOL_W vom Sollwert abweichen ohne Alarm.
VERIFY_TOL_W = 30


# =============================================================================
# 9. SOC-Safety
#
#    Schutzfunktion: Wenn eine Batterie sehr lange (SOC_SAFETY_AFTER_HOURS)
#    bei oder unter SOC_SAFETY_MIN_SOC verbleibt, wird eine Notfall-Ladung
#    eingeleitet. Verhindert Tiefentladungsschaeden bei anhaltender Bewolkung
#    oder im Winter bei geringer PV-Leistung.
# =============================================================================

# Gesamtfunktion aktivieren/deaktivieren.
SOC_SAFETY_ENABLE = True

# SoC-Schwellwert in Prozent. Unterschreitet die Batterie diesen Wert,
# startet der SOC-Safety-Timer.
SOC_SAFETY_MIN_SOC = 11

# Ziel-SoC der Notfall-Ladung in Prozent.
# Wird dieser Wert erreicht, beendet pv_control.py die Notfall-Ladung.
# Muss groesser als SOC_SAFETY_MIN_SOC sein.
SOC_SAFETY_TARGET_SOC = 20

# Wartezeit in Stunden nach Unterschreitung von SOC_SAFETY_MIN_SOC
# bevor die Notfall-Ladung eingeleitet wird.
# Verhindert uebereifriges Laden bei kurzzeitigen Tiefentladungen.
SOC_SAFETY_AFTER_HOURS = 48

# Ladeleistung waehrend der Notfall-Ladung in Watt.
# Niedrig halten um das Netz wenig zu belasten.
SOC_SAFETY_CHARGE_W = 400

# SOC-Safety-Ereignisse im Log dokumentieren.
SOC_SAFETY_LOG_ENABLE = True

# Jeden Zyklus den SOC-Safety-Zustand loggen (nicht nur bei Ereignissen).
# True = sehr viele Eintraege, nur fuer Debugging.
SOC_SAFETY_LOG_EVERY_CYCLE = False

# True = Notfall-Ladung nur starten wenn gleichzeitig PV-Ueberschuss vorhanden.
# False = Notfall-Ladung auch aus dem Netz (empfohlen um echte Tiefentladung zu verhindern).
SOC_SAFETY_REQUIRE_PV = True


# =============================================================================
# 10. Log-Retention
#
#    Automatische Bereinigung alter Logdateien um Speicherplatz zu sparen.
#    Wird einmalig beim Start und danach alle LOG_CLEANUP_RUN_EVERY_H Stunden ausgefuehrt.
# =============================================================================

# Logdateien aelter als N Tage werden geloescht (365 = 1 Jahr aufbewahren).
LOG_RETENTION_DAYS = 365

# Pfad zum Verzeichnis mit den AppDaemon-Logdateien.
LOG_CLEANUP_PATH = "/config/appdaemon/logs"

# Dateiendung der zu pruefenden Logdateien (z.B. ".log" erfasst pv_control.log, etc.)
LOG_CLEANUP_GLOB = ".log"

# Intervall fuer den Cleanup-Job in Stunden.
# 3650h = ca. einmal im Jahr (praktisch: nur beim Start ausgefuehrt).
LOG_CLEANUP_RUN_EVERY_H = 365


# =============================================================================
# 11. PV-Surplus-Zeitfilter
#
#    PV-Ueberschuss wird erst dann als "stabil" anerkannt, wenn er mindestens
#    PV_SURPLUS_MIN_TIME Sekunden ununterbrochen anliegt.
#    Verhindert Ladeimpulse durch kurze Wolkenpausen.
# =============================================================================

# Mindestdauer in Sekunden fuer stabilen PV-Ueberschuss.
# Bei INTERVAL=20s und PV_SURPLUS_MIN_TIME=20s = mindestens 1 voller Zyklus.
PV_SURPLUS_MIN_TIME = 20


# =============================================================================
# 12. Grid-Follow
#
#    Der Grid-Follow-Algorithmus haelt die Netzleistung moeglichst nahe an Null,
#    indem er Laden/Entladen der Batterie steuert.
#    Innerhalb des Totbands passiert nichts (Hysterese).
# =============================================================================

# Totband um Null in Watt.
# Netzleistung innerhalb +/- GRID_DEADBAND_W gilt als "ausgeglichen".
# Kein Modus-Wechsel innerhalb des Totbands.
GRID_DEADBAND_W = 60

# Zeit in Sekunden, die die Netzleistung innerhalb des Totbands liegen muss,
# bevor der Modus auf "stopp" gesetzt wird.
# Verhindert sofortiges Stoppen bei kurzzeitigem Einpendeln.
GRID_STOP_AFTER_S = 90


# =============================================================================
# 13. PV-Ladeverteilung (Bucket / Rotation)
#
#    Bei mehreren Batterien verteilt pv_control.py die verfuegbare PV-Leistung
#    nach einem Bucket-Modell: Erst wenn ein Schwellwert ueberschritten wird,
#    wird eine weitere Batterie zugeschaltet.
#    Rotation stellt sicher dass alle Batterien gleichmaessig geladen werden.
# =============================================================================

# Bucket-Logik aktivieren.
# False = alle Batterien immer gleichmaessig laden.
PV_BUCKET_ENABLE = True

# PV-Gesamtleistung bis zu der nur 1 Batterie geladen wird (W).
# Unterhalb dieses Werts wird die Leistung auf eine einzige Batterie konzentriert
# (effizienter als 3x wenig Leistung aufzuteilen).
PV_BUCKET_1_TOTAL_W = 300

# PV-Gesamtleistung bis zu der nur 2 Batterien geladen werden (W).
# Ueber diesem Wert werden alle 3 Batterien zugeschaltet.
PV_BUCKET_2_TOTAL_W = 900

# Mindest-Ladeleistung pro Batterie in Watt.
# Eine Batterie wird nur dann aktiviert wenn sie mindestens diesen Wert erhaelt.
# Verhindert ineffizientes Laden mit sehr kleinen Leistungen.
PV_MIN_PER_BAT_W = 100

# Rotation aktivieren.
# True = die priorisierte "erste" Batterie wechselt zyklisch.
PV_ROTATE_ENABLE = True

# Rotationsmodus:
# "cycle" = nach jedem vollstaendigen Ladezyklus wird die Reihenfolge gewechselt
# "daily" = taeglich wird zufaellig eine neue Reihenfolge ausgewuerfelt
PV_ROTATE_MODE = "cycle"


# =============================================================================
# 14. BMS / Battery Care (Voll-Lade-Zyklen)
#
#    Lithium-Akkus profitieren von gelegentlichen Voll-Ladezyklen:
#    Die BMS-Zellbalancierung laeuft nur bei hohem SoC (> 90-95 %).
#    ENABLE_BMS_CARE erzwingt regelmaessig einen Ladevorgang bis FULL_CHARGE_SOC,
#    haelt ihn kurz (FULL_CHARGE_MIN_HOLD_S) und entlaesst dann wieder.
# =============================================================================

# BMS-Pflegefunktion aktivieren.
ENABLE_BMS_CARE = True

# Schwellwert Netzleistung fuer "PV-Ueberschuss vorhanden" (W).
# Negative Werte = Einspeisung. -100 W = mindestens 100 W Einspeisung ins Netz.
PV_CHARGE_ALLOW_W = -100

# SoC-Grenzwert fuer "voll geladen" in Prozent.
# Wird dieser Wert erreicht, gilt die Batterie als voll und der Halte-Timer startet.
FULL_CHARGE_SOC = 99

# Mindest-Abstand zwischen zwei BMS-Volllade-Zyklen in Tagen.
# 10 Tage = ca. 3-4 Vollladevorgaenge pro Monat.
FULL_CHARGE_INTERVAL_DAYS = 10

# Dauer der Haltephase nach Erreichen von FULL_CHARGE_SOC in Sekunden.
# Die Batterie bleibt in der Ladephase damit das BMS Zeit zur Balancierung hat.
# 45 Minuten = 45 * 60 = 2700 Sekunden.
FULL_CHARGE_MIN_HOLD_S = 45 * 60

# Cooldown-Dauer nach der Haltephase bevor Entladen wieder erlaubt wird (s).
# 30 Minuten Pause um sicherzustellen dass das BMS den Zustand gespeichert hat.
ALLOW_DISCHARGE_AFTER_S = 30 * 60

# Maximale Anzahl Batterien die gleichzeitig/pro Tag einen BMS-Zyklus durchlaufen.
# 1 = Batterien werden nacheinander vollgeladen, nie parallel.
# Verhindert dass alle Batterien gleichzeitig viel Strom aus dem Netz ziehen.
MAX_BMS_BATTERIES_PER_DAY = 1

# Ladeleistung waehrend des BMS-Zyklus in Watt.
# Hoeher als normale PV-Ladung damit der Zyklus auch bei bewolktem Himmel abgeschlossen wird.
BMS_FORCE_CHARGE_W = 1200

# Pfad zur JSON-Datei fuer den persistenten BMS-Zustand.
# Speichert: letzter Volllade-Zeitpunkt, aktive BMS-Batterien, Rotations-Index.
# Datei wird bei AppDaemon-Neustart gelesen um den Zustand wiederherzustellen.
BMS_STATE_FILE = "/config/appdaemon/data/pv_control_bms_state.json"


# =============================================================================
# 15. Control-Register-Werte (Modbus Register 42000)
#
#    Register 42000 steuert ob die RS485-Fernsteuerung aktiv ist.
#    Nur wenn ein Aktivierungswert aus CONTROL_ENABLE_CANDIDATES steht,
#    werden Schreibkommandos auf Lade/Entlade-Register angenommen.
# =============================================================================

# Werte die Register 42000 annehmen kann wenn Steuerung aktiv/freigegeben ist.
# pv_control.py schreibt 21930 um die Steuerung zu aktivieren.
# Beide Werte werden beim Lesen als "aktiv" akzeptiert.
CONTROL_ENABLE_CANDIDATES = [21930, 21931]

# Set-Version fuer schnellen Membership-Test (wird aus CANDIDATES erzeugt)
CONTROL_ENABLED_VALUES = set(CONTROL_ENABLE_CANDIDATES)

# Werte die als "Steuerung deaktiviert" interpretiert werden.
# 21947 = Modbus-Steuerung deaktiviert (Geraet im Standalone-Modus)
# 0     = Register noch nicht beschrieben / nach Werksreset
CONTROL_DISABLED_VALUES = {21947, 0}


# =============================================================================
# 16. Smartmeter / Netz-Zaehler
#
#    Home Assistant Sensor der die aktuelle Netzleistung liefert.
#    pv_control.py liest diesen Sensor um den Grid-Follow-Algorithmus zu fuettern.
#    Positiver Wert = Netzbezug, negativer Wert = Einspeisung (oder umgekehrt,
#    je nach Zaehler – in diesem Fall in POWER_SENSOR "invert": True setzen).
# =============================================================================

# enabled: True | entity: Netz-Zaehler-Sensor (W oder kW) | invert: True = Vorzeichen drehen
POWER_SENSOR = {"enabled": True, "entity": "sensor.netz_leistung", "invert": False}


# =============================================================================
# 17. Modbus-Register-Adressen
#
#    Holding-Register-Adressen fuer alle Steuer- und Lesezugriffe.
#    Aendern nur noetig wenn ein abweichendes Geraet (anderer Hersteller,
#    andere Firmware-Version) andere Adressen verwendet.
#
#    Tupel-Format (addr, cnt) fuer 32-Bit-Register (2 Modbus-Register).
# =============================================================================

REGISTERS = {
    # SoC in Prozent (0-100), 16-Bit unsigned
    "soc":           32104,

    # Batterieleistung in Watt, 32-Bit signed (Tupel: Startadresse, Anzahl Register)
    "battery_power": (32102, 2),

    # RS485-Steuerungs-Register: muss 21930/21931 enthalten fuer Fernsteuerung
    "control":       42000,

    # Betriebs-Modus-Register: 0=Auto, 1=Manuell, 2=Laden, 3=Entladen, 4=Stopp
    "mode":          42010,

    # Lade-Sollwert in Watt (0-max_charge_w), nur wirksam wenn mode=2
    "charge_set":    42020,

    # Entlade-Sollwert in Watt (0-max_discharge_w), nur wirksam wenn mode=3
    "discharge_set": 42021,

    # AC-Leistung am Netzanschluss, 32-Bit signed (Lese-Register, nicht schreibbar)
    "ac_power":      (32202, 2),
}


# =============================================================================
# 18. Batterie-Konfiguration
#
#    Kernkonfiguration: eine Batterie pro Eintrag.
#    Schluessel (battery1, battery2, ...) werden intern als Bezeichner verwendet
#    und erscheinen in Log-Ausgaben und HA-Entity-IDs.
#
#    FELDER:
#
#    enabled        : False = Batterie wird komplett ignoriert (kein Lesen, kein Schreiben)
#
#    comm_mode      : Kommunikationsprotokoll fuer diese Batterie.
#                     "modbus" = Modbus TCP (benoetigt LAN/RS485-Adapter)
#                     "api"    = Marstek Open API (UDP direkt an die Batterie)
#                     Gilt fuer ALLES: Steuerung UND HA-Sensor-Export.
#
#    modbus_host    : IP-Adresse des Modbus-Adapters (RS485-zu-LAN-Converter).
#                     Wird ignoriert wenn comm_mode="api".
#
#    api_host       : WLAN-IP-Adresse der Batterie selbst.
#                     Wird ignoriert wenn comm_mode="modbus".
#                     Tipp: Statische IP im Router vergeben!
#
#    port           : Modbus TCP Port (Standard: 502).
#                     Wird ignoriert wenn comm_mode="api".
#
#    unit           : Modbus Slave-ID (1-247).
#                     Bei Daisy-Chain mehrerer Batterien an einem Adapter: je Batterie 1,2,3...
#
#    api_port       : UDP-Port der Batterie fuer die Open API.
#                     Standard laut Dokumentation: 30000
#                     Ab Firmware ~V155 in der Marstek App aenderbar (typisch 1025).
#                     Wichtig: Muss mit dem Port in der Marstek App uebereinstimmen!
#
#    api_cd_time    : Passive-Mode Countdown in Sekunden.
#                     Das Geraet wechselt nach dieser Zeit selbststaendig auf STOP
#                     wenn kein neues Kommando eingeht.
#                     Muss groesser als INTERVAL sein. Empfehlung: 90s.
#
#    capacity_kwh   : Nutzbare Kapazitaet in kWh (nicht die Nennkapazitaet!).
#                     Wird fuer die Gewichtsberechnung bei der Lastverteilung benoetigt.
#                     Venus E 3.0 = 5.6 kWh nutzbar (Nennkapazitaet 6.0 kWh)
#
#    max_charge_w   : Maximale Ladeleistung in Watt die an die Batterie geschickt wird.
#                     Sollwerte werden auf diesen Wert begrenzt.
#
#    max_discharge_w: Maximale Entladeleistung in Watt.
#
#    min_soc        : Unterste SoC-Grenze in Prozent.
#                     Unter diesem Wert wird kein Entladekommando mehr gesendet.
#
#    max_soc        : Oberste SoC-Grenze in Prozent.
#                     Ueber diesem Wert wird kein Ladekommando mehr gesendet.
# =============================================================================

BATTERY_CONFIG = {
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

# ---------------------------------------------------------------------------
# BATTERY_META  (automatisch aus BATTERY_CONFIG erzeugt)
#
# idx wird aus dem Zahlen-Suffix des Batterienamens abgeleitet (battery1->1).
# Eintraege in _BATTERY_META_OVERRIDE (oben) ueberschreiben den Automatismus.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Batterie-Verwaltung aus dem Dashboard (pv_batteries.json)
# "add": dort angelegte Batterien, "remove": dort geloeschte Batterien.
# Beim Werksreset wird diese Datei entfernt.
# ---------------------------------------------------------------------------
try:
    import json as _json, os as _os
    _bf = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "pv_batteries.json")
    if _os.path.exists(_bf):
        with open(_bf, encoding="utf-8") as _fh:
            _bo = _json.load(_fh)
        for _k in _bo.get("remove", []):
            BATTERY_CONFIG.pop(_k, None)
        for _k, _v in (_bo.get("add") or {}).items():
            BATTERY_CONFIG[_k] = dict(_v)
        del _fh, _bo
    del _bf
except Exception as _e:
    print("pv_batteries.json fehlerhaft - ignoriert:", _e)

import re as _re
BATTERY_META = {}
for _bname in BATTERY_CONFIG:
    if _bname in _BATTERY_META_OVERRIDE:
        BATTERY_META[_bname] = dict(_BATTERY_META_OVERRIDE[_bname])
    else:
        _m = _re.search(r'(\d+)$', _bname)
        BATTERY_META[_bname] = {"idx": int(_m.group(1)) if _m else 0}
del _bname, _m


# =============================================================================
# 19. Tabellen-Layout (Log-Ausgabe)
#
#    Steuert die tabellarische Status-Ausgabe die pv_control.py jeden Zyklus
#    in den Log schreibt. Spalten koennen deaktiviert werden indem sie aus
#    COLUMNS entfernt werden. Neue Spalten erfordern zusaetzlich Aenderungen
#    in pv_control.py (vals[] im Format-Code).
# =============================================================================

# Reihenfolge der Spalten in der Log-Tabelle.
COLUMNS = [
    "bat",      # Batterie-Bezeichner (battery1, battery2, ...)
    "soc",      # Aktueller SoC in Prozent
    "ac",       # AC-Leistung in Watt (positiv = Einspeisung)
    "zustand",  # Aktueller Betriebszustand (laden/entladen/stopp/fehler)
    "drv",      # Aktiver Treiber (Modbus/API)
    "ctrl",     # Control-Register-Wert (21930=aktiv, 21947=inaktiv)
    "mode",     # Modbus-Modus-Register-Wert
    "last",     # Letzter gesendeter Sollwert (W)
    "soll",     # Aktuell berechneter Sollwert (W)
    "maxch",    # Konfigurierter max_charge_w-Wert
    "maxdis",   # Konfigurierter max_discharge_w-Wert
    "minsoc",   # Konfigurierter min_soc-Wert
    "maxsoc",   # Konfigurierter max_soc-Wert
    "bms",      # BMS-Pflegestatus (z.B. "BMS-VL" waehrend Vollladung)
    "pv",       # PV-Ueberschuss-Flag (True/False)
    "soc11",    # Zeit seit letztem SoC < min_soc (fuer SOC-Safety-Timer)
    "err"       # Fehlerzaehler seit letztem Start
]

# Spalten-Ueberschriften (Reihenfolge muss COLUMNS entsprechen).
HEADERS = [
    "Bat", "SoC", "AC(W)", "Zustand",
    "Drv",
    "Ctrl", "Mode",
    "Last", "Soll",
    "MxCh", "MxDs", "MinSoC", "MaxSoC",
    "BMS", "PV", "SOC11_SEIT", "Err"
]

# Spaltenbreiten in Zeichen fuer die formatierte Log-Ausgabe.
COL_WIDTH = {
    "bat": 5, "soc": 5, "ac": 7,
    "zustand": 18,          # Zustandstext ist am laengsten
    "drv": 5,
    "ctrl": 6, "mode": 5,
    "last": 6, "soll": 6,
    "maxch": 6, "maxdis": 6, "minsoc": 6, "maxsoc": 6,
    "bms": 7,
    "pv": 3, "soc11": 12, "err": 3
}

# Uebersetzungstabelle fuer interne Modus-Bezeichner -> deutsche Log-Texte.
MODE_DE = {"charge": "laden", "discharge": "entladen", "stop": "stopp"}

# =============================================================================
# Ergaenzungen 26.09.2026 (FIX 19 / FIX 20)
# =============================================================================
# Netz-Zaehler: Werte ueber diesem Betrag (W) gelten als Messfehler und werden verworfen.
METER_MAX_ABS_W = 25000
# Netz-Zaehler: ist der letzte Wert aelter als diese Sekunden, gehen alle Batterien auf STOP.
# 0 = Pruefung aus.
METER_MAX_AGE_S = 60
# BMS-Pflege (Vollladung) nur bei PV-Ueberschuss STARTEN. Eine laufende Vollladung
# wird zu Ende gefuehrt. False = altes Verhalten (Start jederzeit, auch nachts aus dem Netz).
BMS_CARE_REQUIRE_PV = True

# Log: Status-Tabelle nur jeden N-ten Zyklus (4 x 15 s = 1 min).
# Bei Lesefehlern, Moduswechsel oder Zaehlerproblemen wird sie sofort geschrieben. 1 = jeder Zyklus.
LOG_TABLE_EVERY_N = 4

# PV-Vorrang fuer Notladung und BMS-Pflege (FIX 24/25):
# Beide warten zuerst auf PV-Ueberschuss. Kommt in der Wartezeit keine Sonne,
# wird aus dem Netz geladen.
# SoC-Schutz: nach SOC_SAFETY_AFTER_HOURS noch max. so viele Stunden auf PV warten.
SOC_SAFETY_PV_WAIT_H = 12
# BMS-Pflege: nach Faelligkeit noch max. so viele Stunden auf PV warten.
BMS_CARE_PV_WAIT_H = 48

# Hauptschalter / Batterie-Schalter (FIX 29, auch im Dashboard schaltbar).
# False = das Script steuert nicht: Batterie bekommt einmal STOP und wird danach
# nicht mehr angesteuert (auch keine Notladung / BMS-Pflege).
CONTROL_ENABLE = True
# FIX 40: Beobachtungsmodus. True = Werte lesen und anzeigen, aber KEINE Befehle an die Batterien
# senden (zum Testen, wenn noch eine andere Steuerung laeuft oder der Netz-Zaehler noch fehlt).
# Auch im Dashboard schaltbar (Konfiguration -> Hauptschalter & Batterien -> Nur beobachten).
READ_ONLY = False
BATTERY1_ACTIVE = True
