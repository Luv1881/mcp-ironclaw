#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKI="${PKI_DIR:-$ROOT/deploy/pki/out}"
OUT="${OUT:-$ROOT/loadtest/out}"
PASSWORD="${KEYSTORE_PASSWORD:-ironclaw}"
DOCKER="${DOCKER:-docker}"
JMETER_IMAGE="${JMETER_IMAGE:-justb4/jmeter:5.5}"

mkdir -p "$OUT/parts"
rm -f "$OUT/ironclaw.p12"

devices=0
for cert in "$PKI"/device-*.crt; do
    [ -e "$cert" ] || continue
    name="$(basename "$cert" .crt)"

    openssl pkcs12 -export \
        -inkey "$PKI/$name.key" \
        -in "$cert" \
        -certfile "$PKI/ca.crt" \
        -name "$name" \
        -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1 \
        -passout "pass:$PASSWORD" \
        -out "$OUT/parts/$name.p12" 2>/dev/null

    devices=$((devices + 1))
done

if [ "$devices" -eq 0 ]; then
    echo "no device certificates found in $PKI; run: make pki" >&2
    exit 1
fi

$DOCKER run --rm --user "$(id -u):$(id -g)" -v "$OUT:/work" --entrypoint sh "$JMETER_IMAGE" -c "
    set -e
    for part in /work/parts/*.p12; do
        keytool -importkeystore -noprompt \
            -srckeystore \"\$part\" -srcstoretype PKCS12 -srcstorepass $PASSWORD \
            -destkeystore /work/ironclaw.p12 -deststoretype PKCS12 -deststorepass $PASSWORD \
            >/dev/null 2>&1
    done
    keytool -list -keystore /work/ironclaw.p12 -storepass $PASSWORD -storetype PKCS12 2>/dev/null | grep -c PrivateKeyEntry
" | tail -1 | xargs -I{} echo "keystore holds {} device identities: $OUT/ironclaw.p12"
