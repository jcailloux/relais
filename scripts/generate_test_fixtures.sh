#!/bin/bash
# =============================================================================
# Generate test fixtures for smartrepo
# =============================================================================
#
# This script generates:
#   1. FlatBuffer C++ headers from .fbs schemas
#   2. Drogon ORM models from database (requires DB to exist)
#   3. Entity wrappers via generate_entities.py
#
# Usage: ./scripts/generate_test_fixtures.sh
#
# Prerequisites:
#   - flatc (FlatBuffers compiler)
#   - drogon_ctl (Drogon CLI tool)
#   - Python 3 with pyyaml
#   - PostgreSQL database initialized (run init_test_db.sh first)
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TESTS_DIR="$PROJECT_DIR/tests"
SCHEMAS_DIR="$TESTS_DIR/schemas"
FIXTURES_DIR="$TESTS_DIR/fixtures"
GENERATED_DIR="$FIXTURES_DIR/generated"

# Database config (must match init_test_db.sh)
DB_NAME="smartrepo_test"
DB_USER="smartrepo_test"
DB_PASS="smartrepo_test"
DB_HOST="localhost"
DB_PORT="5432"

echo "=== Generate Test Fixtures ==="
echo ""

# Create output directories
mkdir -p "$GENERATED_DIR"

# -------------------------------------------------------------------------
# Step 1: Generate FlatBuffer C++ headers
# -------------------------------------------------------------------------
echo "Step 1: Generating FlatBuffer headers..."

if ! command -v flatc &> /dev/null; then
    echo "  Warning: flatc not found, skipping FlatBuffer generation"
else
    for fbs in "$SCHEMAS_DIR"/*.fbs; do
        if [ -f "$fbs" ]; then
            echo "  Processing: $(basename "$fbs")"
            flatc --cpp -o "$GENERATED_DIR" "$fbs"
        fi
    done
    echo "  Done."
fi
echo ""

# -------------------------------------------------------------------------
# Step 2: Generate Drogon ORM models
# -------------------------------------------------------------------------
echo "Step 2: Generating Drogon ORM models..."

DROGON_CTL="${DROGON_CTL:-drogon_ctl}"

if ! command -v "$DROGON_CTL" &> /dev/null && [ ! -x "$DROGON_CTL" ]; then
    echo "  Warning: drogon_ctl not found, skipping ORM generation"
    echo "  Set DROGON_CTL env var if installed elsewhere"
else
    # Check if DB is accessible
    if PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" &> /dev/null; then
        echo "  Generating models from database..."

        # drogon_ctl expects a directory containing model.json
        MODEL_JSON="$GENERATED_DIR/model.json"
        cat > "$MODEL_JSON" << EOF
{
    "rdbms": "postgresql",
    "host": "$DB_HOST",
    "port": $DB_PORT,
    "dbname": "$DB_NAME",
    "user": "$DB_USER",
    "passwd": "$DB_PASS",
    "tables": ["smartrepo_test_items"]
}
EOF

        # Generate models (drogon_ctl reads model.json from the given directory)
        "$DROGON_CTL" create model "$GENERATED_DIR"

        # Cleanup model.json (contains credentials)
        rm -f "$MODEL_JSON"

        echo "  Done."
    else
        echo "  Warning: Cannot connect to database, skipping ORM generation"
        echo "  Run init_test_db.sh first and ensure pg_hba.conf allows password auth"
    fi
fi
echo ""

# -------------------------------------------------------------------------
# Step 3: Generate entity wrappers
# -------------------------------------------------------------------------
echo "Step 3: Generating entity wrappers..."

if [ -f "$TESTS_DIR/entities.yaml" ]; then
    if command -v python3 &> /dev/null; then
        python3 "$SCRIPT_DIR/generate_entities.py" \
            --config "$TESTS_DIR/entities.yaml" \
            --schema-dir "$SCHEMAS_DIR" \
            --output-dir "$TESTS_DIR" || {
            echo "  Warning: Entity generation failed"
        }
        echo "  Done."
    else
        echo "  Warning: python3 not found, skipping entity generation"
    fi
else
    echo "  Warning: entities.yaml not found"
fi
echo ""

echo "=== Generation Complete ==="
echo ""
echo "Generated files in: $GENERATED_DIR"
ls -la "$GENERATED_DIR" 2>/dev/null || echo "  (directory empty or doesn't exist)"