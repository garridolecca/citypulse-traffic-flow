#!/bin/bash
# Upload Vision Zero notebooks to Fabric workspace
# CTS-Geo Analytics GBD / Jhonatan_Garrido-Lecca / Vision_Zero

WORKSPACE_ID="98c9df51-4c36-4af6-8a05-dab9fea1e5d6"
VZ_FOLDER="11463cb8-6d3d-49fd-9f53-1d5d4fe64082"
API_BASE="https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID"
RESOURCE="https://analysis.windows.net/powerbi/api"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

upload_notebook() {
    local FILE="$1"
    local NAME="$2"

    echo "Uploading $NAME..."

    # Base64 encode the notebook content
    local B64=$(base64 -w 0 "$FILE" 2>/dev/null || base64 -i "$FILE" 2>/dev/null)

    # Create the notebook via Fabric API
    local BODY=$(cat <<EOF
{
    "displayName": "$NAME",
    "type": "Notebook",
    "folderId": "$VZ_FOLDER",
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.ipynb",
                "payload": "$B64",
                "payloadType": "InlineBase64"
            }
        ]
    }
}
EOF
)

    RESULT=$(az rest --method post --resource "$RESOURCE" \
        --url "$API_BASE/items" \
        --body "$BODY" 2>&1)

    echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  Created: {d.get(\"displayName\",\"?\")} (ID: {d.get(\"id\",\"?\")})')" 2>/dev/null || echo "  Response: $RESULT"
}

echo "=== Uploading Vision Zero Notebooks to Fabric ==="
echo ""

upload_notebook "$SCRIPT_DIR/01_bronze_ingest.ipynb" "01_bronze_ingest"
upload_notebook "$SCRIPT_DIR/02_silver_clean_enrich.ipynb" "02_silver_clean_enrich"
upload_notebook "$SCRIPT_DIR/03_gold_geoanalytics.ipynb" "03_gold_geoanalytics"
upload_notebook "$SCRIPT_DIR/04_export_viz_json.ipynb" "04_export_viz_json"

echo ""
echo "=== Done ==="
echo "Check: Fabric > CTS-Geo Analytics GBD > Jhonatan_Garrido-Lecca > Vision_Zero"
