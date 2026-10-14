# Marstek Venus control for Home Assistant

[Deutsch](README.md) · **English**

Controls one or more **Marstek Venus batteries** (Venus E, Venus E 3.0) from Home Assistant so that your
electricity meter shows **as little import and export as possible**: surplus from your balcony solar or PV
system goes into the batteries, in the evening and at night the batteries cover the household consumption.

The batteries are reached either over **Wi-Fi (Marstek Open API, no extra hardware)** or through
**RS485-to-LAN adapters (Modbus TCP)** – or both, with automatic fallback.

Everything else is set up in the included **"Marstek" dashboard** (the dashboard texts are in German).

---

## Features

- **Zero export / grid balancing** – charge on surplus, discharge on import, with dead band and protection against constant switching
- **Multiple batteries** – power is split by state of charge; with little PV power only one battery is charged (more efficient), the order rotates
- **Two connection paths per battery** – Modbus *or* Wi-Fi API *or* "Modbus, otherwise API" / "API, otherwise Modbus". A battery that answers on neither path gets no power share
- **SoC protection** – a battery stuck at its minimum for too long is gently recharged (preferably with sun)
- **BMS care** – regular full charge for cell balancing, one battery after the other, only with PV surplus (pauses when clouds come or at night)
- **Safety** – implausible or stale meter values stop the batteries; main switch and switch per battery
- **Dashboard** – overview with traffic lights, state of charge, power, energy *today / yesterday / this week*, all settings changeable live (no restart), add and delete batteries, save settings
- **Firmware detection** – different Open API units per firmware (e.g. temperature, energy counters) are converted automatically

---

## Requirements

| | |
|---|---|
| **Home Assistant** | Home Assistant OS or Supervised (with add-ons) |
| **Battery** | Marstek Venus E / Venus E 3.0 (other Venus models untested) |
| **Connection** | **Wi-Fi:** enable the *Open API* in the Marstek app (see below) – **or** – **Modbus:** one RS485-to-LAN adapter per battery |
| **Grid meter** | a sensor in Home Assistant that shows the current power at your grid connection (e.g. IR reader on the meter, Shelly 3EM). W or kW, **positive on import, negative on export** (the other way round works too: run the installer with --meter and answer the sign question with "no") |

You do **not** need to install Mosquitto (MQTT) or AppDaemon yourself – the installer does that.

---

## Installation in 3 steps

**1. Install the terminal**

Settings → **Apps** → Install app → install and start **"Terminal & SSH"**, switch on "Show in sidebar".
(In older Home Assistant versions: Settings → Add-ons → Add-on store. "Advanced SSH & Web Terminal" works as well.)

**2. Open the terminal and paste this one command**

    curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash -s -- --lang en

**3. Wait – the rest runs by itself**

The installer sets everything up without questions:

- It installs **Mosquitto (MQTT)** and **AppDaemon** with the required Python packages and sets up the MQTT integration
- It **searches your Wi-Fi for batteries** (the Open API must be on in the Marstek app) and adds them
- It **detects your grid meter** (sensor for the power at your grid connection)
- It creates the **"Marstek" dashboard** in the sidebar and finally checks that the batteries deliver values
- On a new installation Home Assistant restarts once at the end (about 1 minute)

It only asks if something cannot be detected clearly (no battery found, several possible meters) –
**Enter** accepts the suggestion. To answer everything yourself: option --ask.

**Observe mode:** if another control is currently driving the batteries (e.g. a second Home Assistant) or
the grid meter is missing, the control starts in observe mode – it shows all values but sends no commands.
Switch it off in the dashboard: Konfiguration → Hauptschalter & Batterien → "Nur beobachten".

> **Tip:** give every battery (and every RS485 adapter) a **fixed IP address** in your router.

---

## Enable the Open API in the Marstek app (for Wi-Fi)

1. Open the Marstek app and select the battery
2. Settings → **Open API** (in some versions "Local API" / "Network") → **enable**
3. Enter **UDP port 30000** (default)
4. Note the battery's **Wi-Fi IP** shown there

If the battery does not answer during the installer's test, the Open API is usually still off or the port is wrong.

---

## Update

Just run **the same command again**. The installer then asks whether to keep your settings (Enter)
or to set up everything afresh. Your settings (batteries, IPs, meter, all values changed in the
dashboard) are kept; every replaced file is backed up in the *backup* folder first. The dashboard is only
replaced if you confirm.

**More options** – append *bash -s --* and the option, e.g.:

    curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash -s -- --lang en --reconfigure

| Option | Effect |
|---|---|
| --fresh | fresh setup: delete batteries, meter and dashboard values (with backup) |
| --diag | troubleshooting: creates a report to share (no passwords) and offers to fix the problems found. Existing MQTT users and passwords are never changed |
| --fix | like --diag, but fixes the problems without asking |
| --meter | pick the grid meter again only (e.g. after setting up the meter) |
| --reconfigure | ask batteries and grid meter again (e.g. RS485 adapters, other meter) |
| --ask | new installation with all questions instead of automatic |
| --dashboard | rebuild the dashboard only (e.g. after adding another battery) |
| --dry-run | test run: only shows what would happen |
| --yes | update without questions |
| --lang de | installer in German |
| --uninstall | remove the control (with backup) |

---

## FAQ

**The dashboard shows "Beobachtungsmodus" (observe mode) – is that an error?**
No. The control only reads and sends no commands. This happens automatically while the grid meter is missing
or another control is driving the batteries (e.g. the Marstek app in "Passive" mode or a second Home Assistant).
Switch it off under Konfiguration → Hauptschalter & Batterien → "Nur beobachten" – but only once no other
control is running. **Only one control may drive the batteries.**

**I set up the meter after the installation.**
Run the installer with *--meter*. It detects the meter, stores it and switches observe mode off if nothing
else speaks against it.

**I got another battery.**
Enable the Open API in the Marstek app and simply run the normal installer command again – new Wi-Fi
batteries are found and added, the dashboard is rebuilt. Batteries with RS485 adapter: *--reconfigure*.

**The batteries charge when they should discharge.**
The meter sign is inverted: run the installer with *--meter* and answer the sign question with "no".

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Battery delivers no values (Wi-Fi) | Open API enabled in the app? Port 30000? Wi-Fi IP correct? Check in the dashboard under Konfiguration → Venus N |
| Battery delivers no values (Modbus) | check adapter IP and port 502, slave ID usually 1; only **one** connection per adapter is allowed (no second Modbus integration in parallel) |
| "Netz-Zähler nicht verfügbar" (meter unavailable) | check the sensor in Home Assistant; with stale values (over 60 s) the control stops for safety |
| Batteries charge when they should discharge | meter sign wrong → run the installer with --reconfigure and answer the sign question with "no" |
| Configuration page shows red boxes | the *fold-entity-row* extension is missing → run the installer with --dashboard or install it via HACS |
| Dashboard shows "Beobachtungsmodus" | intended while another control runs or the meter is missing – then switch off "Nur beobachten" under Konfiguration → Hauptschalter & Batterien |
| Many fields show "Entity not found" | switches and settings come via MQTT – usually the MQTT integration is missing or the MQTT password does not match. Run the normal install command again; if that does not help, create a report with --diag |
| Anything else | run the installer with **--diag** and send the report, or Settings → Apps (Add-ons) → AppDaemon → **Log** |

---

## Structure

| File | Purpose |
|---|---|
| apps/marstek_venus/pv_control.py | the control (AppDaemon app, 15 s cycle) |
| apps/marstek_venus/pv_mqtt.py | bridge to the dashboard (MQTT entities "Marstek Steuerung") |
| apps/marstek_venus/pv_config.py | all settings with explanations (German) |
| dashboard/marstek_dashboard.json | dashboard template |
| installer/ha_setup.py, install.sh | installer / updater |

Installed to */addon_configs/a0d7b954_appdaemon/apps/marstek_venus/* (Home Assistant 2026.7 and later: */app_configs/…*). Values changed in the dashboard are
stored in *pv_mqtt_overrides.json*, added batteries in *pv_batteries.json*, MQTT credentials in
*pv_mqtt_login.json* – these files are never touched by updates.

**Own MQTT broker:** If Home Assistant uses another MQTT broker instead of Mosquitto, the control adopts that broker automatically on install, update and `--fix` (login from the MQTT integration, connection test first). Nothing is changed on the broker or any password, Mosquitto is not installed then. If the test fails, create a user on the broker and enter it in *pv_mqtt_login.json* - it is kept on every update.

**Meter reports rarely:** If the grid meter sends no new value for more than 60 s, the batteries go to STOP for safety. The wait time can be set in the dashboard under *Konfiguration → Zähler & zweiter Speicher*; the **meter info** next to it shows how often the meter really delivers new values and suggests a value.

**Limits per battery:** max. charge and discharge power, min/max SoC and capacity of each Venus can be changed in the dashboard under *Konfiguration → Hauptschalter & Batterien → Venus X → Grenzwerte*.

**Without the installer** (advanced): AppDaemon add-on with the Python packages *pymodbus==3.9.2* and
*paho-mqtt*, copy the folder *apps/marstek_venus* into the AppDaemon apps folder, add to *apps.yaml*:

    marstek_venus:
      module: pv_control
      class: PVControlApp

then adjust *pv_config.py* (BATTERY_CONFIG, POWER_SENSOR) and create *pv_mqtt_login.json* with
host / port / user / password of your MQTT broker.

The old single-file version (until 03/2026) is in the [legacy](legacy/) folder.

---

## Liability, license, contact

The control actively charges and discharges your batteries. Use at your own risk – please read the
[disclaimer](DISCLAIMER.md) (German). This project is not affiliated with Marstek.

License: [MIT](LICENSE) · Developed and maintained by **Neue Medien Becker** · info@neuemedienbecker.de

If this project helps you, a star ⭐ on GitHub or a voluntary donation is appreciated.
