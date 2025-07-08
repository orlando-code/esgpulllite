#!/bin/bash

# Script to chmod and run all .sh scripts in a directory (including subdirectories)
# Usage: ./run_all_scripts.sh [directory_path] [max_parallel_jobs]

# Set the directory to search (default to current directory)
SEARCH_DIR="${1:-.}"
# Set max parallel jobs (default to number of CPU cores)
MAX_JOBS="${2:-$(nproc)}"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Running all .sh scripts in: $SEARCH_DIR (max $MAX_JOBS parallel jobs) ===${NC}"
echo

# Check if directory exists
if [ ! -d "$SEARCH_DIR" ]; then
    echo -e "${RED}Error: Directory '$SEARCH_DIR' does not exist${NC}"
    exit 1
fi

# Check if GNU parallel is available
if command -v parallel >/dev/null 2>&1; then
    USE_PARALLEL=true
    echo -e "${GREEN}Using GNU parallel for execution${NC}"
else
    USE_PARALLEL=false
    echo -e "${YELLOW}GNU parallel not found, using background jobs${NC}"
fi

# Find all .sh files recursively
SCRIPT_COUNT=0
SUCCESS_COUNT=0
FAILED_COUNT=0
TEMP_DIR=$(mktemp -d)
SCRIPT_LIST="$TEMP_DIR/scripts.txt"

# First pass: chmod all .sh files and build script list
echo -e "${YELLOW}Step 1: Making all .sh files executable and cataloging...${NC}"
while IFS= read -r -d '' script; do
    echo -e "  chmod +x ${script}"
    chmod +x "$script"
    echo "$script" >> "$SCRIPT_LIST"
    SCRIPT_COUNT=$((SCRIPT_COUNT + 1))
done < <(find "$SEARCH_DIR" -name "*.sh" -type f -print0)

echo -e "Found $SCRIPT_COUNT scripts to execute"
echo

# Function to run a single script
run_script() {
    local script="$1"
    local script_dir=$(dirname "$script")
    local script_name=$(basename "$script")
    local log_file="$TEMP_DIR/$(basename "$script").log"
    
    echo -e "${BLUE}[$$] Running: $script${NC}" | tee "$log_file"
    echo "----------------------------------------" | tee -a "$log_file"
    
    # Change to the script's directory and run it
    if (cd "$script_dir" && "./$script_name") >> "$log_file" 2>&1; then
        echo -e "${GREEN}✓ SUCCESS: $script${NC}" | tee -a "$log_file"
        echo "SUCCESS" > "$log_file.status"
    else
        local exit_code=$?
        echo -e "${RED}✗ FAILED: $script (exit code: $exit_code)${NC}" | tee -a "$log_file"
        echo "FAILED" > "$log_file.status"
    fi
}

# Export the function so it can be used by parallel
export -f run_script
export TEMP_DIR RED GREEN YELLOW BLUE NC

echo

# Second pass: run all .sh files in parallel
echo -e "${YELLOW}Step 2: Executing all .sh scripts in parallel...${NC}"

if [ "$USE_PARALLEL" = true ]; then
    # Use GNU parallel
    parallel -j "$MAX_JOBS" --line-buffer run_script :::: "$SCRIPT_LIST"
else
    # Use background jobs with job control
    job_count=0
    while IFS= read -r script; do
        # Wait if we have too many background jobs
        while [ $(jobs -r | wc -l) -ge "$MAX_JOBS" ]; do
            sleep 0.1
        done
        
        # Run script in background
        run_script "$script" &
        job_count=$((job_count + 1))
    done < "$SCRIPT_LIST"
    
    # Wait for all background jobs to complete
    wait
fi

echo -e "${YELLOW}All scripts completed. Processing results...${NC}"

# Count results
for script in $(cat "$SCRIPT_LIST"); do
    local log_file="$TEMP_DIR/$(basename "$script").log"
    if [ -f "$log_file.status" ]; then
        status=$(cat "$log_file.status")
        if [ "$status" = "SUCCESS" ]; then
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAILED_COUNT=$((FAILED_COUNT + 1))
        fi
        
        # Display the log
        echo "========================================"
        cat "$log_file"
        echo "========================================"
        echo
    fi
done

# Summary
echo -e "${BLUE}=== SUMMARY ===${NC}"
echo -e "Total scripts found: $SCRIPT_COUNT"
echo -e "${GREEN}Successful: $SUCCESS_COUNT${NC}"
echo -e "${RED}Failed: $FAILED_COUNT${NC}"
echo -e "Max parallel jobs used: $MAX_JOBS"

# Cleanup
rm -rf "$TEMP_DIR"

if [ $FAILED_COUNT -eq 0 ]; then
    echo -e "${GREEN}All scripts completed successfully!${NC}"
    exit 0
else
    echo -e "${RED}Some scripts failed. Check output above for details.${NC}"
    exit 1
fi
