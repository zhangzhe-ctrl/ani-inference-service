#!/usr/bin/env bash
set -euo pipefail
umask 077
: "${WIRING_RUN:?authorized task run directory is required}"
private="$WIRING_RUN/private/c02-admission"
mkdir -p "$private"
test ! -e "$private/ca.key"
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 2 -subj /CN=c02-software-only-ca -keyout "$private/ca.key" -out "$private/ca.pem"
for identity in server apiserver untrusted; do
  dns=c02-inference.test
  usage=serverAuth
  if test "$identity" = apiserver; then dns=c02-apiserver.test; usage=clientAuth; fi
  if test "$identity" = untrusted; then dns=c02-other-client.test; usage=clientAuth; fi
  openssl req -newkey rsa:2048 -sha256 -nodes -subj "/CN=$dns" -keyout "$private/$identity.key" -out "$private/$identity.csr"
  printf 'subjectAltName=DNS:%s\nextendedKeyUsage=%s\nkeyUsage=digitalSignature,keyEncipherment\n' "$dns" "$usage" > "$private/$identity.ext"
  openssl x509 -req -sha256 -days 2 -in "$private/$identity.csr" -CA "$private/ca.pem" -CAkey "$private/ca.key" -CAcreateserial -extfile "$private/$identity.ext" -out "$private/$identity.pem"
done
chmod 600 "$private"/*.key "$private"/*.pem "$private"/*.csr "$private"/*.ext "$private"/*.srl
