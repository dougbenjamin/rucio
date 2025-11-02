# Quick Start Guide - Rucio Unique Replica Checker

## Prerequisites

Before using this tool, you need:

1. **Python 3.6 or higher**
   ```bash
   python3 --version
   ```

2. **Rucio client installed**

   Option A - Install from this repository:
   ```bash
   cd /path/to/rucio
   pip install -e .
   ```

   Option B - Install from PyPI:
   ```bash
   pip install rucio-clients
   ```

3. **Rucio credentials configured**

   Set up your Rucio account credentials:
   ```bash
   # Required environment variables
   export RUCIO_ACCOUNT=your_rucio_account
   export RUCIO_AUTH_TYPE=userpass  # or x509, oidc, gss, etc.

   # For userpass authentication:
   export RUCIO_USERNAME=your_username
   export RUCIO_PASSWORD=your_password

   # For x509 authentication:
   export RUCIO_AUTH_TYPE=x509
   export X509_USER_PROXY=/path/to/your/proxy

   # Set Rucio server host
   export RUCIO_HOST=https://your-rucio-server:443
   ```

   Verify your credentials:
   ```bash
   rucio whoami
   ```

## Installation

1. **Make the script executable:**
   ```bash
   chmod +x check_unique_replicas.py
   ```

2. **Verify the script:**
   ```bash
   ./check_unique_replicas.py --help
   ```

## Basic Usage

### Step 1: Simple Test Run

Start with a simple check on a small RSE:

```bash
./check_unique_replicas.py --rse YOUR_RSE_NAME
```

Example:
```bash
./check_unique_replicas.py --rse TIER2_US_MIT
```

### Step 2: Monitor Progress

The program will:
1. Connect to Rucio
2. List all datasets at the RSE
3. Process each dataset in parallel
4. Check replica locations
5. Save results to `unique_replicas.json` and `unique_replicas.csv`

Output will appear in real-time:
```
2024-01-15 10:30:45,123 - INFO - [MainThread] - Rucio Unique Replica Checker
2024-01-15 10:30:45,124 - INFO - [MainThread] - Target RSE: TIER2_US_MIT
2024-01-15 10:30:45,125 - INFO - [MainThread] - Fetching datasets at RSE: TIER2_US_MIT
2024-01-15 10:30:46,234 - INFO - [MainThread] - Found 150 datasets at TIER2_US_MIT
2024-01-15 10:30:46,235 - INFO - [ThreadPoolExecutor-0_0] - Processing dataset: mc16_13TeV:dataset1
...
```

### Step 3: View Results

After completion, check the results:

```bash
# View JSON results
cat unique_replicas.json

# View CSV results
cat unique_replicas.csv

# Quick summary using jq
jq '.statistics' unique_replicas.json
```

## Common Scenarios

### Scenario 1: Debug Mode

If you encounter issues, enable debug logging:

```bash
./check_unique_replicas.py \
    --rse TIER2_US_MIT \
    --log-level DEBUG
```

### Scenario 2: Custom Output Location

Specify where to save results:

```bash
mkdir -p results
./check_unique_replicas.py \
    --rse TIER1_US_FNAL \
    --output results/fnal_unique.json
```

### Scenario 3: Adjust Performance

For faster processing (if server allows):

```bash
./check_unique_replicas.py \
    --rse CERN-PROD \
    --workers 10 \
    --rate-limit 20
```

For slower, more conservative processing:

```bash
./check_unique_replicas.py \
    --rse BUSY_RSE \
    --workers 2 \
    --rate-limit 5 \
    --time-window 2.0
```

## Understanding the Output

### JSON Output Structure

```json
{
  "rse": "TIER2_US_MIT",                    // RSE that was checked
  "timestamp": "2024-01-15T10:30:45.123",   // When the check was performed
  "statistics": {
    "datasets_found": 150,                  // Total datasets at RSE
    "datasets_processed": 150,              // Successfully processed
    "files_checked": 15000,                 // Total files examined
    "unique_files_found": 234,              // Files with only 1 copy
    "errors": 2,                            // Errors encountered
    "skipped": 5                            // Datasets skipped
  },
  "unique_files": {
    "scope1": ["file1.root", "file2.root"], // Files organized by scope
    "scope2": ["file3.root"]
  }
}
```

### CSV Output Structure

Simple two-column format:
```
scope,name
mc16_13TeV,file1.root
mc16_13TeV,file2.root
data18_13TeV,file3.root
```

## Analyzing Results

### Count unique files:

```bash
jq '[.unique_files | to_entries | .[].value | length] | add' unique_replicas.json
```

### List unique files by scope:

```bash
jq '.unique_files | to_entries | .[] | {scope: .key, count: (.value | length)}' unique_replicas.json
```

### Extract files for a specific scope:

```bash
jq '.unique_files["mc16_13TeV"]' unique_replicas.json
```

### Get statistics:

```bash
jq '.statistics' unique_replicas.json
```

## Troubleshooting

### Problem: "Failed to initialize Rucio client"

**Solution:** Check your Rucio credentials:
```bash
# Verify environment variables
echo $RUCIO_ACCOUNT
echo $RUCIO_AUTH_TYPE
echo $RUCIO_HOST

# Test Rucio connection
rucio whoami
```

### Problem: "RSE not found"

**Solution:** Verify the RSE name:
```bash
# List all RSEs
rucio list-rses

# Search for specific RSE
rucio list-rses | grep TIER2
```

### Problem: Program runs too slowly

**Solution:** Increase parallelism:
```bash
./check_unique_replicas.py \
    --rse YOUR_RSE \
    --workers 10 \
    --rate-limit 50
```

### Problem: Getting rate limit errors

**Solution:** Reduce load on server:
```bash
./check_unique_replicas.py \
    --rse YOUR_RSE \
    --workers 2 \
    --rate-limit 5 \
    --time-window 2.0
```

### Problem: Want to see more details

**Solution:** Enable debug mode:
```bash
./check_unique_replicas.py \
    --rse YOUR_RSE \
    --log-level DEBUG
```

## Next Steps

1. **Read the full documentation:** See `README_unique_replicas.md` for detailed information
2. **Review examples:** Check `example_usage.sh` for more usage scenarios
3. **Customize:** Adjust worker count and rate limits based on your needs
4. **Automate:** Set up cron jobs for regular checks

## Tips for Best Results

1. **Start small:** Test with a small RSE first to understand the behavior
2. **Monitor logs:** Keep an eye on the log files in the `logs/` directory
3. **Adjust gradually:** Increase workers and rate limits incrementally
4. **Save results:** Keep historical results for tracking changes over time
5. **Use CSV for processing:** The CSV output is easier to work with in scripts

## Example Workflow

```bash
# 1. Set up environment
export RUCIO_ACCOUNT=myaccount
export RUCIO_AUTH_TYPE=x509
export X509_USER_PROXY=/tmp/x509up_u$(id -u)

# 2. Verify credentials
rucio whoami

# 3. Create results directory
mkdir -p results logs

# 4. Run the checker
./check_unique_replicas.py \
    --rse TIER2_US_MIT \
    --output results/mit_$(date +%Y%m%d).json \
    --workers 5 \
    --rate-limit 10 \
    --log-level INFO

# 5. Analyze results
jq '.statistics' results/mit_$(date +%Y%m%d).json

# 6. Extract unique files to process
jq -r '.unique_files | to_entries | .[] | .value[] as $name | "\(.key):\($name)"' \
    results/mit_$(date +%Y%m%d).json > files_to_replicate.txt
```

## Getting Help

```bash
# Show help message
./check_unique_replicas.py --help

# View detailed documentation
less README_unique_replicas.md

# Check log files
tail -f logs/unique_replicas_*.log
```

---

**Happy checking!** 🚀
