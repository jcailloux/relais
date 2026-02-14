#!/bin/bash
# =============================================================================
# Initialize the relais test database
# =============================================================================
#
# Usage: ./scripts/init_test_db.sh
#
# Fixed values:
#   Database: relais_test
#   User: relais_test
#   Password: relais_test
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MIGRATIONS_DIR="$PROJECT_DIR/tests/migrations"

DB_NAME="relais_test"
DB_USER="relais_test"
DB_PASS="relais_test"

echo "=== Relais Test Database Setup ==="
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "Error: psql not found. Please install PostgreSQL client."
    exit 1
fi

# Create user if it doesn't exist
echo "Creating user (if not exists)..."
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname = '$DB_USER'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS'"

# Create database if it doesn't exist
echo "Creating database (if not exists)..."
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER"

# Grant privileges
echo "Granting privileges..."
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER"

# Run migrations (read file as current user, pipe to psql as postgres)
echo "Running migrations..."
for migration in "$MIGRATIONS_DIR"/*.sql; do
    if [ -f "$migration" ]; then
        echo "  Applying: $(basename "$migration")"
        cat "$migration" | sudo -u postgres psql -d "$DB_NAME"
    fi
done

# Grant table permissions to test user
echo "Granting table permissions..."
sudo -u postgres psql -d "$DB_NAME" -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO $DB_USER"
sudo -u postgres psql -d "$DB_NAME" -c "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO $DB_USER"

echo ""
echo "=== Setup Complete ==="
echo ""

# Check if pg_hba.conf needs updating
if ! PGPASSWORD="$DB_PASS" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" &> /dev/null; then
    echo "WARNING: Cannot connect via TCP with password authentication."
    echo ""
    echo "Add these lines to pg_hba.conf (before the generic 'host all all' lines):"
    echo "  local   all   $DB_USER                      scram-sha-256"
    echo "  host    all   $DB_USER   127.0.0.1/32       scram-sha-256"
    echo "  host    all   $DB_USER   ::1/128            scram-sha-256"
    echo ""
    echo "Then restart PostgreSQL: sudo systemctl restart postgresql"
else
    echo "Database is ready. Tests can now connect."
fi
echo ""
echo "Next steps:"
echo "  1. Build tests: cmake -B build && cmake --build build"
echo "  2. Run tests: cd build && ctest -L integration --output-on-failure"