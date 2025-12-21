#!/bin/bash

# WordMash database migration script
# Usage: ./migrate.sh [apply|mark|list|rollback] [migration_file]

# Load environment variables
export DB_HOST=44.225.148.34
export DB_PORT=3306
export DB_USER=DCAdminUser
export DB_PASSWORD='@potAtoSplatWTF99'
export DB_NAME=wordmash

# Build database URL (yoyo uses mysql:// scheme with pymysql driver)
DATABASE_URL="mysql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_YOYO="${SCRIPT_DIR}/venv/bin/yoyo"
MIGRATIONS_DIR="${SCRIPT_DIR}/migrations"

case "$1" in
    apply)
        echo "Applying pending migrations..."
        $VENV_YOYO apply --database "$DATABASE_URL" "$MIGRATIONS_DIR" --batch
        ;;
    mark)
        if [ -z "$2" ]; then
            echo "Usage: ./migrate.sh mark <migration_pattern>"
            echo "Example: ./migrate.sh mark 0001"
            exit 1
        fi
        echo "Marking migrations matching '$2' as applied..."
        $VENV_YOYO mark --database "$DATABASE_URL" "$MIGRATIONS_DIR" -m "$2" --batch
        ;;
    list)
        echo "Listing migrations..."
        $VENV_YOYO list --database "$DATABASE_URL" "$MIGRATIONS_DIR"
        ;;
    rollback)
        echo "Rolling back last migration..."
        $VENV_YOYO rollback --database "$DATABASE_URL" "$MIGRATIONS_DIR" --batch
        ;;
    new)
        if [ -z "$2" ]; then
            echo "Usage: ./migrate.sh new <migration_name>"
            exit 1
        fi
        # Generate next migration number
        LAST_NUM=$(ls -1 "$MIGRATIONS_DIR"/*.py 2>/dev/null | sed 's/.*\/\([0-9]*\)_.*/\1/' | sort -n | tail -1)
        NEXT_NUM=$(printf "%04d" $((10#${LAST_NUM:-0} + 1)))
        FILENAME="${NEXT_NUM}_${2}.py"

        cat > "$MIGRATIONS_DIR/$FILENAME" << 'TEMPLATE'
"""
Migration: MIGRATION_NAME
"""

from yoyo import step

__depends__ = []

steps = [
    step(
        """
        -- Your SQL here
        """,
        """
        -- Rollback SQL here
        """
    ),
]
TEMPLATE
        sed -i "s/MIGRATION_NAME/$2/" "$MIGRATIONS_DIR/$FILENAME"
        echo "Created: $MIGRATIONS_DIR/$FILENAME"
        ;;
    *)
        echo "WordMash Database Migrations"
        echo ""
        echo "Usage: ./migrate.sh <command> [args]"
        echo ""
        echo "Commands:"
        echo "  apply              Apply all pending migrations"
        echo "  mark <file>        Mark a migration as applied (for existing schemas)"
        echo "  list               List all migrations and their status"
        echo "  rollback           Rollback the last applied migration"
        echo "  new <name>         Create a new migration file"
        ;;
esac
