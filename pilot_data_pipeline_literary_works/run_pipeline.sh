#!/bin/bash
#
# Literary Works Data Pipeline
# Run all pipeline steps in sequence
#
# Usage:
#   ./run_pipeline.sh           # Run full pipeline
#   ./run_pipeline.sh --no-openai  # Run only steps 1-4 (no OpenAI)
#   ./run_pipeline.sh --openai-only # Run only steps 5-7 (OpenAI enrichment)
#

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   Literary Works Data Pipeline${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Parse arguments
RUN_CORE=true
RUN_OPENAI=true

if [[ "$1" == "--no-openai" ]]; then
    RUN_OPENAI=false
    echo -e "${YELLOW}Running in no-OpenAI mode (steps 1-4 only)${NC}"
elif [[ "$1" == "--openai-only" ]]; then
    RUN_CORE=false
    echo -e "${YELLOW}Running OpenAI enrichment only (steps 5-7)${NC}"
fi
echo ""

# Activate virtual environment if it exists
if [[ -f ".venv/bin/activate" ]]; then
    echo -e "${GREEN}Activating virtual environment...${NC}"
    source .venv/bin/activate
elif [[ -f "../.venv/bin/activate" ]]; then
    echo -e "${GREEN}Activating virtual environment...${NC}"
    source ../.venv/bin/activate
fi

# Core pipeline (no OpenAI)
if [[ "$RUN_CORE" == true ]]; then
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}   CORE PIPELINE (Steps 1-4)${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""

    # Step 1: Fetch from Wikidata - SKIPPED (run manually if needed)
    # The CSV files in enriched_literary_works/ are already populated
    # To re-fetch, run: python scripts/01_fetch_wikidata.py
    echo -e "${YELLOW}Step 1: Skipped (CSV files already exist in enriched_literary_works/)${NC}"
    echo ""

    # Step 2: Create database
    echo -e "${GREEN}Step 2: Creating SQLite database...${NC}"
    python scripts/02_create_database.py
    echo ""

    # Step 3: Enrich dates
    echo -e "${GREEN}Step 3: Adding year column...${NC}"
    python scripts/03_enrich_dates.py
    echo ""

    # Step 4: Filter instances
    echo -e "${GREEN}Step 4: Marking excluded instance types...${NC}"
    python scripts/04_filter_instances.py
    echo ""
fi

# OpenAI enrichment
if [[ "$RUN_OPENAI" == true ]]; then
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}   OPENAI ENRICHMENT (Steps 5-7)${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""

    # Check for API key or cache
    if [[ -z "$OPENAI_API_KEY" ]] && [[ ! -f "cache/country_mappings.json" ]]; then
        echo -e "${YELLOW}Warning: OPENAI_API_KEY not set and no cache found.${NC}"
        echo -e "${YELLOW}OpenAI steps will fail unless cache files exist.${NC}"
        echo ""
    fi

    # Step 5: Enrich countries
    echo -e "${GREEN}Step 5: Mapping countries to modern equivalents...${NC}"
    python scripts/05_enrich_countries_openai.py
    echo ""

    # Step 6: Enrich languages
    echo -e "${GREEN}Step 6: Mapping languages to modern countries...${NC}"
    python scripts/06_enrich_languages_openai.py
    echo ""

    # Step 7: Add regions
    echo -e "${GREEN}Step 7: Adding region classification...${NC}"
    python scripts/07_add_regions_openai.py
    echo ""
fi

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}   Pipeline completed successfully!${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo "Database: output/literary_works.db"
echo ""
