#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-./pki-out}"
DEVICE_COUNT="${DEVICE_COUNT:-3}"
CA_DAYS="${CA_DAYS:-3650}"
LEAF_DAYS="${LEAF_DAYS:-30}"
INGEST_CN="${INGEST_CN:-ingest.ironclaw.internal}"
EDGE_CN="${EDGE_CN:-edge.ironclaw.internal}"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

already_consistent() {
    [ -f ca.crt ] && [ -f edge.pem ] && [ -f ingest.pem ] && [ -f devices.crl ] || return 1

    local index name
    for index in $(seq 0 $((DEVICE_COUNT - 1))); do
        name="$(printf 'device-%03d' "$index")"
        [ -f "${name}.crt" ] || return 1
        openssl verify -CAfile ca.crt "${name}.crt" >/dev/null 2>&1 || return 1
    done

    openssl verify -CAfile ca.crt edge.crt >/dev/null 2>&1 || return 1
    openssl verify -CAfile ca.crt ingest.crt >/dev/null 2>&1 || return 1

    return 0
}

if already_consistent; then
    echo "reusing consistent PKI in ${OUT_DIR} (${DEVICE_COUNT} devices)"
    exit 0
fi

rm -f ./*.crt ./*.key ./*.pem ./*.csr ./*.ext ./*.srl ./*.crl index.txt* crlnumber* ca.cnf

openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days "$CA_DAYS" \
    -subj "/O=IronClaw/CN=IronClaw Device CA" -out ca.crt 2>/dev/null

issue_server() {
    local name="$1" cn="$2" eku="${3:-serverAuth}"

    openssl genrsa -out "${name}.key" 2048 2>/dev/null
    openssl req -new -key "${name}.key" -subj "/O=IronClaw/CN=${cn}" -out "${name}.csr" 2>/dev/null

    cat > "${name}.ext" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = ${eku}
subjectAltName = DNS:${cn}
EOF

    openssl x509 -req -in "${name}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "${name}.crt" -days "$LEAF_DAYS" -sha256 -extfile "${name}.ext" 2>/dev/null

    cat "${name}.crt" "${name}.key" > "${name}.pem"
    rm -f "${name}.csr" "${name}.ext"
}

issue_device() {
    local device_id="$1"

    openssl genrsa -out "${device_id}.key" 2048 2>/dev/null
    openssl req -new -key "${device_id}.key" -subj "/O=IronClaw/OU=devices/CN=${device_id}" -out "${device_id}.csr" 2>/dev/null

    cat > "${device_id}.ext" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature
extendedKeyUsage = clientAuth
EOF

    openssl x509 -req -in "${device_id}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "${device_id}.crt" -days "$LEAF_DAYS" -sha256 -extfile "${device_id}.ext" 2>/dev/null

    rm -f "${device_id}.csr" "${device_id}.ext"
}

record_in_database() {
    local cert="$1"
    [ -f ca.cnf ] || return 0

    local serial subject expiry
    serial=$(openssl x509 -in "$cert" -noout -serial | cut -d= -f2)
    subject=$(openssl x509 -in "$cert" -noout -subject -nameopt compat | sed 's/^subject=//')
    expiry=$(openssl x509 -in "$cert" -noout -enddate | cut -d= -f2)
    expiry=$(date -u -d "$expiry" +%y%m%d%H%M%SZ 2>/dev/null) || return 0

    printf 'V\t%s\t\t%s\tunknown\t%s\n' "$expiry" "$serial" "$subject" >> index.txt
}

write_ca_config() {
    cat > ca.cnf <<'EOF'
[ ca ]
default_ca = CA_default

[ CA_default ]
dir               = .
database          = $dir/index.txt
crlnumber         = $dir/crlnumber
certificate       = $dir/ca.crt
private_key       = $dir/ca.key
default_md        = sha256
default_crl_days  = 30
policy            = policy_any
unique_subject    = no

[ policy_any ]
commonName = supplied
EOF

    touch index.txt
    [ -f crlnumber ] || echo 1000 > crlnumber
}

generate_crl() {
    openssl ca -config ca.cnf -gencrl -out devices.crl 2>/dev/null
}

write_ca_config

issue_server edge "$EDGE_CN" "serverAuth, clientAuth"
issue_server ingest "$INGEST_CN"

for index in $(seq 0 $((DEVICE_COUNT - 1))); do
    issue_device "$(printf 'device-%03d' "$index")"
done

generate_crl

echo "issued into ${OUT_DIR}:"
ls -1 *.crt devices.crl
