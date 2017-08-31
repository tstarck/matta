#!/bin/bash
set +eu
. /etc/default/matta
exec /usr/local/bin/matta -c "$BROKER" -t "$TOPIC" -f "$DATABASE"
