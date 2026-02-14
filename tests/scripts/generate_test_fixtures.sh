#!/bin/bash
# =============================================================================
# Generate test fixtures for relais
# =============================================================================
#
# This script generates entity wrappers via generate_entities.py
#
# Usage: ./scripts/generate_test_fixtures.sh
#
# Prerequisites: Python 3
#
# =============================================================================

set -e

PROJECT_DIR="$(cd . && pwd)"
SCRIPT_PATH="$PROJECT_DIR/scripts/generate_entities.py"
TESTS_DIR="$PROJECT_DIR/tests"
FIXTURES_DIR="$TESTS_DIR/fixtures"
GENERATED_DIR="$FIXTURES_DIR/generated"

# Database config (must match init_test_db.sh)
DB_NAME="relais_test"
DB_USER="relais_test"
DB_PASS="relais_test"
DB_HOST="localhost"
DB_PORT="5432"

echo "=== Generate Test Fixtures ==="
echo ""

# Create output directories
mkdir -p "$GENERATED_DIR"

echo "Generating entity wrappers..."

if [ -d "$TESTS_DIR/fixtures" ]; then
  if command -v python3 &> /dev/null; then
      python3 "$SCRIPT_PATH" \
          --scan "$FIXTURES_DIR" \
          --output-dir "$TESTS_DIR" || {
          echo "  Warning: Entity generation failed"
      }
      echo "  Done."
  else
      echo "  Warning: python3 not found, skipping entity generation"
  fi
else
  echo "folder $TESTS_DIR/fixtures not found"
fi
echo ""

echo "=== Generation Complete ==="
echo ""
echo "Generated files in: $GENERATED_DIR"
ls -la "$GENERATED_DIR" 2>/dev/null || echo "  (directory empty or doesn't exist)"