# bash deps/minio/gen_ca.sh

#!/usr/bin/env bash
set -euo pipefail

# --- 1. Configuration ---
CN_CA=${1:-"MinIO Local Root"}
CN_SERVER=${2:-"localhost"}
CN_CLIENT=${3:-"marketAccess"}

# SANs (Subject Alternative Names)
SAN_DNS_1="localhost"
SAN_DNS_2="luyao-storage-head.ddns.comp.nus.edu.sg"
SAN_DNS_3="luyao-s0.ddns.comp.nus.edu.sg"
SAN_IP_1="127.0.0.1"

# Validity
DAYS_CA=3650
DAYS_SERVER=825

# Locations
MINIO_CERT_DIR="/opt/minio/certs"
MINIO_USER="minio-user"
MC_CERT_DIR="${HOME}/.mc/certs"

# --- 2. Preparation ---
rm -rf server-certs client-certs CAs
mkdir -p server-certs client-certs CAs
sudo mkdir -p "${MINIO_CERT_DIR}/CAs"
mkdir -p "${MC_CERT_DIR}/CAs"

echo "==> [1/5] Generating Root CA..."
openssl ecparam -name prime256v1 -genkey -noout -out CAs/minio-ca.key
openssl req -x509 -new -key CAs/minio-ca.key -sha256 \
  -subj "/CN=${CN_CA}" -days "${DAYS_CA}" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -out CAs/minio-ca.crt

echo "==> [2/5] Generating Server Certificate..."
openssl ecparam -name prime256v1 -genkey -noout -out server-certs/private.key
openssl req -new -key server-certs/private.key \
  -subj "/CN=${CN_SERVER}" \
  -addext "subjectAltName=DNS:${SAN_DNS_1},DNS:${SAN_DNS_2},DNS:${SAN_DNS_3},IP:${SAN_IP_1}" \
  -out server-certs/server.csr

openssl x509 -req -in server-certs/server.csr \
  -CA CAs/minio-ca.crt -CAkey CAs/minio-ca.key -CAcreateserial \
  -days "${DAYS_SERVER}" -sha256 \
  -extfile <(cat <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyAgreement
extendedKeyUsage=serverAuth
subjectAltName=DNS:${SAN_DNS_1},DNS:${SAN_DNS_2},DNS:${SAN_DNS_3},IP:${SAN_IP_1}
authorityKeyIdentifier=keyid,issuer
subjectKeyIdentifier=hash
EOF
) -out server-certs/public.crt

echo "==> [3/5] Generating Client Certificate (Identity: ${CN_CLIENT})..."
openssl ecparam -name prime256v1 -genkey -noout -out client-certs/private.key
openssl req -new -key client-certs/private.key \
  -subj "/CN=${CN_CLIENT}" \
  -out client-certs/client.csr

openssl x509 -req -in client-certs/client.csr \
  -CA CAs/minio-ca.crt -CAkey CAs/minio-ca.key -CAcreateserial \
  -days "${DAYS_SERVER}" -sha256 \
  -extfile <(cat <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyAgreement
extendedKeyUsage=clientAuth
authorityKeyIdentifier=keyid,issuer
subjectKeyIdentifier=hash
EOF
) -out client-certs/public.crt

echo "==> [4/5] Installing to MinIO Server (${MINIO_CERT_DIR})..."
# Copy files
sudo cp server-certs/private.key "${MINIO_CERT_DIR}/private.key"
sudo cp server-certs/public.crt  "${MINIO_CERT_DIR}/public.crt"
sudo cp CAs/minio-ca.crt         "${MINIO_CERT_DIR}/CAs/public.crt"

# Set Permissions (Critical for Systemd Service)
sudo chown -R "${MINIO_USER}":"${MINIO_USER}" "${MINIO_CERT_DIR}"
sudo chmod 750 "${MINIO_CERT_DIR}"
sudo chmod 750 "${MINIO_CERT_DIR}/CAs"
sudo chmod 640 "${MINIO_CERT_DIR}/public.crt" "${MINIO_CERT_DIR}/private.key" "${MINIO_CERT_DIR}/CAs/public.crt"

echo "==> [5/5] Installing to local mc Client (${MC_CERT_DIR})..."
# mc requires EXACT filenames: private.key, public.crt
cp client-certs/private.key "${MC_CERT_DIR}/private.key"
cp client-certs/public.crt  "${MC_CERT_DIR}/public.crt"
cp CAs/minio-ca.crt         "${MC_CERT_DIR}/CAs/public.crt"

echo "==> Done."