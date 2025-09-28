#!/bin/bash

# ================================================================================
# Apache Superset Initialization Script
# Handles database setup, admin user creation, and role initialization
# ================================================================================

set -e

echo "🚀 Starting Superset initialization..."

# Wait for database to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
until PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U admin -d superset -c '\q'; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "✅ PostgreSQL is ready!"

# Load Superset configuration and initialize logging
echo "📋 Loading Superset configuration..."
python -c "
import sys
sys.path.insert(0, '/app')
from superset_config import configure_logging
configure_logging()
print('✅ Superset configuration loaded successfully')
"

# Initialize database schema
echo "🗄️ Initializing Superset database schema..."
superset db upgrade
echo "✅ Database schema initialized"

# Create admin user (handle existing user gracefully)
echo "👤 Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@admin.com \
    --password admin123 \
    2>/dev/null || echo "ℹ️ Admin user already exists, skipping creation"
echo "✅ Admin user ready"

# Initialize default roles and permissions
echo "🔐 Initializing roles and permissions..."
superset init
echo "✅ Roles and permissions initialized"

# Load example data (optional)
if [ "${SUPERSET_LOAD_EXAMPLES:-false}" = "true" ]; then
    echo "📊 Loading example data..."
    superset load-examples
    echo "✅ Example data loaded"
else
    echo "ℹ️ Skipping example data (SUPERSET_LOAD_EXAMPLES not enabled)"
fi

echo "🎉 Superset initialization complete!"
echo "📍 Superset will be available at: http://localhost:8088"
echo "🔑 Login: admin@admin.com / admin123"
echo ""
echo "🚀 Starting Superset server..."