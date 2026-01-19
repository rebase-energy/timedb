#!/bin/bash
set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  TimeDB Setup Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Load environment variables from .env
if [ ! -f .env ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Loading DATABASE_URL from .env"
export $(grep -v '^#' .env | xargs)

if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}Error: DATABASE_URL not found in .env${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Database connection configured"
echo ""

# Step 1: Delete all tables
echo -e "${YELLOW}Step 1: Cleaning database (deleting all tables)${NC}"
timedb delete tables --yes
echo -e "${GREEN}✓${NC} All tables deleted"
echo ""

# Step 2: Create tables with users support
echo -e "${YELLOW}Step 2: Creating tables${NC}"
timedb create tables --with-users --yes
echo -e "${GREEN}✓${NC} Tables created"
echo ""

# Step 3: Create a user
echo -e "${YELLOW}Step 3: Creating user${NC}"

# Generate a UUID for tenant_id or use environment variable if set
TENANT_ID=${TENANT_ID:-$(uuidgen | tr '[:upper:]' '[:lower:]')}
USER_EMAIL=${USER_EMAIL:-"admin@example.com"}

echo "  Tenant ID: ${TENANT_ID}"
echo "  Email: ${USER_EMAIL}"

timedb users create --tenant-id "${TENANT_ID}" --email "${USER_EMAIL}"
echo -e "${GREEN}✓${NC} User created"
echo ""

# Step 4: Deploy to Modal
echo -e "${YELLOW}Step 4: Deploying to Modal${NC}"
modal deploy deploy_modal.py
echo -e "${GREEN}✓${NC} Deployed to Modal"
echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Environment:"
echo -e "  Tenant ID: ${GREEN}${TENANT_ID}${NC}"
echo -e "  User Email: ${GREEN}${USER_EMAIL}${NC}"
echo ""
echo -e "${YELLOW}Note: Save the API key shown above - it won't be displayed again${NC}"
