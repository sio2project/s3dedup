#!/bin/sh
# Initialize Garage v2 S3 storage for s3dedup development/testing.
# Uses Garage v2 admin API with fixed predetermined credentials
# so that no manual credential retrieval is needed.
#
# This runs automatically via docker-compose (garage-init service).

set -e

ADMIN="http://garage:3902"
TOKEN="s3dedup-test-admin-token"
AUTH="Authorization: Bearer ${TOKEN}"

# Fixed dev credentials (match .env.example and config.json)
ACCESS_KEY="GK0123456789abcdef01234567"
SECRET_KEY="abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

echo "[init] Waiting for Garage admin API..."
for i in $(seq 1 30); do
    if curl -s -o /dev/null "${ADMIN}/v2/GetClusterStatus" -H "${AUTH}" 2>/dev/null; then
        echo "[init] Garage admin API is ready."
        break
    fi
    if [ "$i" = "30" ]; then
        echo "[init] ERROR: Timeout waiting for Garage"
        exit 1
    fi
    sleep 1
done

# Helper: extract first JSON string value for a key from (possibly pretty-printed) JSON
json_val() {
    grep -o "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | head -1 | sed 's/.*:[[:space:]]*"\([^"]*\)"/\1/'
}
json_int() {
    grep -o "\"$1\"[[:space:]]*:[[:space:]]*[0-9]*" | head -1 | sed 's/.*:[[:space:]]*//'
}

# 1. Get node ID and configure layout
echo "[init] Getting cluster status..."
STATUS=$(curl -sf -H "${AUTH}" "${ADMIN}/v2/GetClusterStatus")
NODE_ID=$(echo "$STATUS" | json_val id)

if [ -z "$NODE_ID" ]; then
    echo "[init] ERROR: Failed to get node ID"
    exit 1
fi
echo "[init] Node: ${NODE_ID}"

# Check if layout already has roles assigned
LAYOUT=$(curl -sf -H "${AUTH}" "${ADMIN}/v2/GetClusterLayout")
LAYOUT_VERSION=$(echo "$LAYOUT" | json_int version)

if echo "$LAYOUT" | grep -q '"roles": \[\]'; then
    echo "[init] Configuring cluster layout..."
    curl -sf -X POST \
        -H "${AUTH}" \
        -H "Content-Type: application/json" \
        -d "{\"roles\":[{\"id\":\"${NODE_ID}\",\"zone\":\"dc1\",\"capacity\":1073741824,\"tags\":[]}]}" \
        "${ADMIN}/v2/UpdateClusterLayout" > /dev/null

    NEW_VERSION=$((LAYOUT_VERSION + 1))
    curl -sf -X POST \
        -H "${AUTH}" \
        -H "Content-Type: application/json" \
        -d "{\"version\":${NEW_VERSION}}" \
        "${ADMIN}/v2/ApplyClusterLayout" > /dev/null

    echo "[init] Layout applied (version ${NEW_VERSION})."
    sleep 2
else
    echo "[init] Layout already configured."
fi

# 2. Import fixed API key
echo "[init] Importing API key..."
curl -sf -X POST \
    -H "${AUTH}" \
    -H "Content-Type: application/json" \
    -d "{\"accessKeyId\":\"${ACCESS_KEY}\",\"secretAccessKey\":\"${SECRET_KEY}\",\"name\":\"s3dedup-key\"}" \
    "${ADMIN}/v2/ImportKey" > /dev/null 2>&1 || echo "[init] Key already exists (OK)."

# Grant createBucket permission
curl -sf -X POST \
    -H "${AUTH}" \
    -H "Content-Type: application/json" \
    -d "{\"allow\":{\"createBucket\":true}}" \
    "${ADMIN}/v2/UpdateKey?id=${ACCESS_KEY}" > /dev/null

echo "[init] API key ready."

# 3. Create default bucket
echo "[init] Creating default bucket..."
BUCKET_RESP=$(curl -sf -X POST \
    -H "${AUTH}" \
    -H "Content-Type: application/json" \
    -d "{\"globalAlias\":\"default\"}" \
    "${ADMIN}/v2/CreateBucket" 2>/dev/null) || true

if [ -n "$BUCKET_RESP" ]; then
    BUCKET_ID=$(echo "$BUCKET_RESP" | json_val id)
    if [ -n "$BUCKET_ID" ]; then
        # Grant full permissions on the bucket to our key
        curl -sf -X POST \
            -H "${AUTH}" \
            -H "Content-Type: application/json" \
            -d "{\"bucketId\":\"${BUCKET_ID}\",\"accessKeyId\":\"${ACCESS_KEY}\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":true}}" \
            "${ADMIN}/v2/AllowBucketKey" > /dev/null
    fi
fi

echo "[init] ======================================="
echo "[init] Garage S3 initialization complete!"
echo "[init] Endpoint:    http://localhost:3900"
echo "[init] Access Key:  ${ACCESS_KEY}"
echo "[init] Secret Key:  ${SECRET_KEY}"
echo "[init] ======================================="
