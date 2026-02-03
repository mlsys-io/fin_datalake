#!/usr/bin/env bash
set -euo pipefail

# --- Config (edit if needed) ---
CN_CA="MinIO Local Root"
CN_SERVER="localhost"
SAN_DNS_1="localhost"
SAN_DNS_2="luyao-storage-head.ddns.comp.nus.edu.sg"
SAN_IP_1="127.0.0.1"
SAN_IP_2="172.28.176.117"

DAYS_CA=3650        # ~10 years
DAYS_SERVER=825     # ~27 months

# MinIO cert locations (adjust if your MinIO path/user differ)
MINIO_CERT_DIR="/opt/minio/certs"
MINIO_USER="minio-user"

# mc trust store (client trust for 'mc' CLI)
MC_CA_DIR="$HOME/.mc/certs/CAs"

# --- Output dirs ---
mkdir -p server-certs CAs
mkdir -p "${MINIO_CERT_DIR}/CAs"
mkdir -p "${MC_CA_DIR}"

echo "==> Generating ECDSA P-256 Root CA key..."
openssl ecparam -name prime256v1 -genkey -noout -out CAs/minio-ca.key

echo "==> Self-signing Root CA certificate..."
openssl req -x509 -new -key CAs/minio-ca.key -sha256 \
  -subj "/CN=${CN_CA}" \
  -days "${DAYS_CA}" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -out CAs/minio-ca.crt

echo "==> Generating ECDSA P-256 server key..."
openssl ecparam -name prime256v1 -genkey -noout -out server-certs/private.key

echo "==> Creating server CSR with SANs..."
openssl req -new -key server-certs/private.key \
  -subj "/CN=${CN_SERVER}" \
  -addext "subjectAltName=DNS:${SAN_DNS_1},DNS:${SAN_DNS_2},IP:${SAN_IP_1},IP:${SAN_IP_2}" \
  -out server-certs/server.csr

echo "==> Signing server certificate with CA..."
# Note: keyUsage for ECDSA TLS: digitalSignature is sufficient; adding keyAgreement is OK.
# ExtendedKeyUsage must include serverAuth.
openssl x509 -req -in server-certs/server.csr \
  -CA CAs/minio-ca.crt -CAkey CAs/minio-ca.key -CAcreateserial \
  -days "${DAYS_SERVER}" -sha256 \
  -extfile <(cat <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyAgreement
extendedKeyUsage=serverAuth
subjectAltName=DNS:${SAN_DNS_1},DNS:${SAN_DNS_2},IP:${SAN_IP_1},IP:${SAN_IP_2}
authorityKeyIdentifier=keyid,issuer
subjectKeyIdentifier=hash
EOF
) \
  -out server-certs/public.crt

echo "==> Verifying server certificate against CA..."
openssl verify -CAfile CAs/minio-ca.crt server-certs/public.crt

echo "==> Installing certificates for MinIO..."
sudo cp server-certs/private.key "${MINIO_CERT_DIR}/private.key"
sudo cp server-certs/public.crt  "${MINIO_CERT_DIR}/public.crt"
sudo cp CAs/minio-ca.crt        "${MINIO_CERT_DIR}/CAs/public.crt"
sudo chown -R "${MINIO_USER}":"${MINIO_USER}" "${MINIO_CERT_DIR}"

echo "==> (Optional) Installing CA for mc CLI trust..."
cp CAs/minio-ca.crt "${MC_CA_DIR}/public.crt"

echo "==> Certificate summary:"
openssl x509 -in server-certs/public.crt -noout -text | egrep -A2 "Issuer:|Subject:|Basic Constraints|Extended Key Usage|Subject Alternative Name"

echo "==> Done. Restart MinIO to pick up the new certs."
