#!/bin/bash
#
# Example usage scenarios for check_unique_replicas.py
#
# Before running, ensure:
# 1. Rucio client is installed
# 2. Rucio credentials are configured
# 3. Script is executable: chmod +x check_unique_replicas.py
#

set -e

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Rucio Unique Replica Checker - Example Usage ===${NC}\n"

# Example 1: Basic usage with minimal options
echo -e "${YELLOW}Example 1: Basic Usage${NC}"
echo "Description: Check for unique replicas at a single RSE with default settings"
echo "Command:"
cat <<'EOF'
./check_unique_replicas.py --rse TIER2_US_MIT
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 2: Debug mode with custom output
echo -e "${YELLOW}Example 2: Debug Mode${NC}"
echo "Description: Run in debug mode with detailed logging and custom output file"
echo "Command:"
cat <<'EOF'
./check_unique_replicas.py \
    --rse TIER1_US_FNAL \
    --output results/fnal_unique_replicas.json \
    --log-level DEBUG
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 3: High performance mode
echo -e "${YELLOW}Example 3: High Performance Mode${NC}"
echo "Description: Use maximum parallelism for faster processing"
echo "Command:"
cat <<'EOF'
./check_unique_replicas.py \
    --rse CERN-PROD \
    --workers 20 \
    --rate-limit 50 \
    --time-window 1.0 \
    --output results/cern_prod.json
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 4: Conservative mode
echo -e "${YELLOW}Example 4: Conservative Mode${NC}"
echo "Description: Use minimal load for busy or rate-limited servers"
echo "Command:"
cat <<'EOF'
./check_unique_replicas.py \
    --rse BUSY_RSE \
    --workers 2 \
    --rate-limit 5 \
    --time-window 10 \
    --log-level INFO
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 5: Multiple RSE checks
echo -e "${YELLOW}Example 5: Check Multiple RSEs${NC}"
echo "Description: Check multiple RSEs sequentially"
echo "Command:"
cat <<'EOF'
#!/bin/bash
for rse in TIER1_US_FNAL TIER2_US_MIT TIER2_US_UCSD; do
    echo "Checking RSE: $rse"
    ./check_unique_replicas.py \
        --rse "$rse" \
        --output "results/${rse}_unique.json" \
        --workers 5 \
        --rate-limit 10 \
        --log-level INFO
    sleep 5  # Wait between RSEs to avoid overload
done
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 6: Analyze results
echo -e "${YELLOW}Example 6: Analyze Results${NC}"
echo "Description: Quick analysis of results using jq"
echo "Commands:"
cat <<'EOF'
# Count unique files by scope
jq '.unique_files | to_entries | .[] | {scope: .key, count: (.value | length)}' unique_replicas.json

# Get total unique file count
jq '[.unique_files | to_entries | .[].value | length] | add' unique_replicas.json

# View statistics
jq '.statistics' unique_replicas.json

# Extract all unique files for a specific scope
jq '.unique_files["mc16_13TeV"]' unique_replicas.json

# Count files per scope and sort
jq '.unique_files | to_entries | map({scope: .key, count: (.value | length)}) | sort_by(.count) | reverse' unique_replicas.json
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 7: Process CSV results
echo -e "${YELLOW}Example 7: Process CSV Results${NC}"
echo "Description: Work with the CSV output file"
echo "Commands:"
cat <<'EOF'
# Count unique files
tail -n +2 unique_replicas.csv | wc -l

# Get unique scopes
tail -n +2 unique_replicas.csv | cut -d',' -f1 | sort -u

# Count files per scope
tail -n +2 unique_replicas.csv | cut -d',' -f1 | sort | uniq -c

# Filter files by scope
grep "^mc16_13TeV," unique_replicas.csv

# Create scope-specific files
for scope in $(tail -n +2 unique_replicas.csv | cut -d',' -f1 | sort -u); do
    grep "^${scope}," unique_replicas.csv > "results/${scope}_files.csv"
done
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 8: Monitor progress in real-time
echo -e "${YELLOW}Example 8: Monitor Progress${NC}"
echo "Description: Run in background and monitor logs"
echo "Commands:"
cat <<'EOF'
# Run in background
./check_unique_replicas.py --rse TIER2_US_MIT > /dev/null 2>&1 &
PID=$!

# Monitor the log file
tail -f logs/unique_replicas_TIER2_US_MIT_*.log

# Or monitor just the statistics
tail -f logs/unique_replicas_TIER2_US_MIT_*.log | grep -E "(Statistics|files checked|unique files)"

# Wait for completion
wait $PID
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 9: Error recovery
echo -e "${YELLOW}Example 9: Error Recovery${NC}"
echo "Description: Handle interruptions gracefully"
echo "Command:"
cat <<'EOF'
# The program automatically saves results on Ctrl+C
# To resume, you would need to implement checkpoint/resume logic
# For now, just restart with the same parameters

# If interrupted, check partial results
ls -lh unique_replicas.json
jq '.statistics' unique_replicas.json
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Example 10: Scheduled execution
echo -e "${YELLOW}Example 10: Scheduled Execution${NC}"
echo "Description: Run via cron for regular checks"
echo "Crontab entry:"
cat <<'EOF'
# Run daily at 2 AM
0 2 * * * /path/to/check_unique_replicas.py --rse TIER2_US_MIT --output /path/to/results/daily_$(date +\%Y\%m\%d).json 2>&1 | /usr/bin/logger -t unique_replicas

# Run weekly on Sunday at 3 AM with email notification
0 3 * * 0 /path/to/check_unique_replicas.py --rse TIER1_US_FNAL --output /path/to/results/weekly_$(date +\%Y\%m\%d).json
EOF
echo ""
read -p "Press Enter to continue..."
echo ""

# Summary
echo -e "${GREEN}=== Summary ===${NC}"
cat <<EOF

Key Points:
1. Start with default settings and adjust based on performance
2. Use DEBUG mode for troubleshooting
3. Monitor logs to optimize worker count and rate limits
4. Both JSON and CSV outputs are created automatically
5. Results are saved even if interrupted with Ctrl+C
6. Adjust parallelism based on server load and response times

Common Parameter Combinations:
- Development/Testing: --workers 2 --rate-limit 5 --log-level DEBUG
- Production: --workers 5 --rate-limit 10 --log-level INFO
- High Performance: --workers 20 --rate-limit 50 --log-level WARNING

For more information, see README_unique_replicas.md
EOF

echo -e "\n${GREEN}Done!${NC}"
