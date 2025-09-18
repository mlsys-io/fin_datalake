mkdir -p server-certs
mkdir -p CAs

openssl req -x509 -newkey ed25519 -nodes \
  -subj "/CN=MinIO Local Root" \
  -keyout CAs/minio-ca.key -out CAs/minio-ca.crt -days 3650 \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash"

openssl req -new -newkey ed25519 -nodes \
  -subj "/CN=localhost" \
  -keyout server-certs/private.key -out server-certs/server.csr \
  -addext "subjectAltName=DNS:localhost,DNS:luyao-storage-head.ddns.comp.nus.edu.sg,IP:127.0.0.1,IP:192.168.0.202"

openssl x509 -req -in server-certs/server.csr \
  -CA CAs/minio-ca.crt -CAkey CAs/minio-ca.key -CAcreateserial \
  -days 825 \
  -extfile <(printf "%s" \
"basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,DNS:luyao-storage-head.ddns.comp.nus.edu.sg,IP:127.0.0.1,IP:192.168.0.202
authorityKeyIdentifier=keyid,issuer
subjectKeyIdentifier=hash
") \
  -out server-certs/public.crt

sudo cp server-certs/private.key /opt/minio/certs/private.key
sudo cp server-certs/public.crt /opt/minio/certs/public.crt
sudo cp CAs/minio-ca.crt /opt/minio/certs/CAs/public.crt
sudo chown -R minio-user /opt/minio/certs
cp CAs/minio-ca.crt $HOME/.mc/certs/CAs/public.crt

openssl x509 -in server-certs/public.crt -noout -text | egrep -A2 "Issuer:|Subject:|Basic Constraints|Extended Key Usage|Subject Alternative Name"
# Expect: Basic Constraints: CA:FALSE
#         Extended Key Usage: TLS Web Server Authentication
#         SAN includes your DNS/IP entries
