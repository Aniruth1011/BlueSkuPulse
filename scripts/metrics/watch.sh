#!/usr/bin/env bash

# shellcheck disable=SC1091
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

log "Following Bronze and Spark logs; press Ctrl-C to stop watching"
"${COMPOSE[@]}" logs -f --tail=100 bronze spark redpanda

