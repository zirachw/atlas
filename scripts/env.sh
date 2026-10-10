#!/usr/bin/env bash
# Writes .env, or the given path, from .env.example. Every empty value gets a fresh secret.
set -euo pipefail

out=$(realpath -m "${1:-.env}")
cd "$(dirname "$0")/.."

# Alphanumeric only, because database passwords are embedded in connection URLs.
secret() { openssl rand -base64 48 | tr -d '\n=+/' | cut -c1-32; }

key=$(mktemp)
trap 'rm -f "$key"' EXIT

openssl genpkey -algorithm Ed25519 -out "$key" 2>/dev/null
private=$(base64 < "$key" | tr -d '\n')
public=$(openssl pkey -in "$key" -pubout | base64 | tr -d '\n')

while IFS= read -r line || [[ -n $line ]]; do
  line=${line%$'\r'}
  name=${line%%=*}

  case $line in
    AUTH_ED25519_PRIVATE_KEY_B64=) line="$name=$private" ;;
    AUTH_ED25519_PUBLIC_KEY_B64=)  line="$name=$public" ;;
    PVMBG_TOKEN=)                  line="$name=pvmbg.atlas.$(secret)" ;;
    [A-Z]*=)                       line="$name=$(secret)" ;;
  esac

  printf '%s\n' "$line"
done < .env.example > "$out"

chmod 600 "$out"

echo "Wrote $out. If the stack ran before, reset its databases: docker compose down -v"
