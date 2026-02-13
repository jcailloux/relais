#!/bin/bash
# =============================================================================
# Setup everything needed for relais tests
# =============================================================================
#
# Usage: ./scripts/setup_tests.sh
#
# This script:
#   1. Initializes the test database (PostgreSQL)
#   2. Generates test fixtures (FlatBuffers, ORM models, entities)
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================="
echo "  Relais Test Setup"
echo "============================================="
echo ""

# Step 1: Initialize database
echo "[1/2] Initializing test database..."
echo ""
bash "$SCRIPT_DIR/init_test_db.sh"
echo ""

# Step 2: Generate test fixtures
echo "[2/2] Generating test fixtures..."
echo ""
bash "$SCRIPT_DIR/generate_test_fixtures.sh"
echo ""

echo "============================================="
echo "  Setup Complete"
echo "============================================="
echo ""
echo "To build and run tests:"
echo "  cmake -B build && cmake --build build"
echo "  cd build && ctest -L integration --output-on-failure"