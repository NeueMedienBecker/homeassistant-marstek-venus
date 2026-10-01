#!/usr/bin/env bash
# =============================================================================
# Marstek Venus Steuerung - Installer / Updater
#
# Im Home-Assistant-Add-on "Terminal & SSH" (oder "Advanced SSH & Web Terminal")
# diesen einen Befehl eingeben:
#
#   curl -fsSL https://raw.githubusercontent.com/NeueMedienBecker/homeassistant-marstek-venus/main/install.sh | bash
#
# Run the same command in the Home Assistant terminal add-on to install or update.
#
# Optionen / options (nach "bash -s --" anhaengen, z. B. ... | bash -s -- --lang en):
#   --lang de|en      Sprache / language
#   --dry-run         nur zeigen, nichts veraendern / show only, change nothing
#   --reconfigure     Batterien und Zaehler neu abfragen / ask batteries and meter again
#   --dashboard       nur das Dashboard neu erzeugen / rebuild the dashboard only
#   --uninstall       entfernen / remove
#   --yes             Update ohne Rueckfragen / update without questions
# =============================================================================
set -e

REPO="NeueMedienBecker/homeassistant-marstek-venus"
REF="$MARSTEK_REF"
[ -n "$REF" ] || REF="main"
WORK="/tmp/marstek-venus-install"

say() { printf '%s\n' "$*"; }

if [ -z "$SUPERVISOR_TOKEN" ]; then
  say "FEHLER: Bitte im Home-Assistant-Add-on 'Terminal & SSH' ausfuehren."
  say "ERROR: Please run this in the Home Assistant add-on 'Terminal & SSH'."
  exit 1
fi

# Python 3 und curl bereitstellen (die Terminal-Add-ons basieren auf Alpine Linux)
need=""
command -v python3 >/dev/null 2>&1 || need="$need python3"
command -v curl >/dev/null 2>&1 || need="$need curl"
command -v tar >/dev/null 2>&1 || need="$need tar"
if [ -n "$need" ]; then
  say "Installiere / installing:$need"
  apk add --no-cache $need >/dev/null
fi

# Quellen: lokaler Ordner (wenn install.sh aus einem entpackten Paket gestartet wird) oder GitHub
SRC=""
case "$0" in
  *install.sh)
    here="$(cd "$(dirname "$0")" && pwd)"
    if [ -f "$here/installer/ha_setup.py" ]; then SRC="$here"; fi
    ;;
esac

if [ -z "$SRC" ]; then
  rm -rf "$WORK"
  mkdir -p "$WORK"
  say "Lade / downloading $REPO ($REF) ..."
  url="https://codeload.github.com/$REPO/tar.gz/$REF"
  if ! curl -fsSL "$url" | tar xz -C "$WORK" --strip-components=1; then
    url="https://codeload.github.com/$REPO/tar.gz/refs/tags/$REF"
    curl -fsSL "$url" | tar xz -C "$WORK" --strip-components=1
  fi
  SRC="$WORK"
fi

if [ ! -f "$SRC/installer/ha_setup.py" ]; then
  say "FEHLER: Download unvollstaendig / ERROR: download incomplete."
  exit 1
fi

# ha_setup.py liest die Eingaben direkt von der Tastatur (/dev/tty), auch bei "curl | bash"
exec python3 "$SRC/installer/ha_setup.py" "$@"
