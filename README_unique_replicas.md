# Rucio Unique Replica Checker

A multithreaded Python tool to identify files with unique replicas at a specified Rucio Storage Element (RSE).

## Overview

This program finds all datasets in a specified Rucio Storage Element (RSE), then checks all file replicas at that RSE in the AVAILABLE state to see if another copy exists anywhere else. Files with only one copy (at the specified RSE) are stored in a data file organized by scope and name.

## Features

- **Multithreaded Processing**: Concurrent dataset processing using ThreadPoolExecutor
- **Rate Limiting**: Configurable rate limiter to prevent API overload
- **Comprehensive Logging**: Dual logging to both file and console
- **Debug Mode**: Verbose logging for troubleshooting
- **Progress Tracking**: Real-time statistics and progress updates
- **Flexible Output**: Results saved in both JSON and CSV formats
- **Error Handling**: Robust error handling with detailed error reporting
- **Thread-Safe**: All data structures protected with locks

## Requirements

- Python 3.6+
- Rucio client libraries (from this repository)
- Valid Rucio account and credentials

## Installation

1. Ensure you have the Rucio client libraries installed:
```bash
cd /path/to/rucio
pip install -e .
```

2. Configure your Rucio credentials:
```bash
export RUCIO_ACCOUNT=your_account
export RUCIO_AUTH_TYPE=userpass  # or x509, oidc, etc.
# Additional auth-specific environment variables as needed
```

3. Make the script executable:
```bash
chmod +x check_unique_replicas.py
```

## Usage

### Basic Usage

```bash
./check_unique_replicas.py --rse RSE_NAME
```

### Advanced Usage

```bash
./check_unique_replicas.py \
    --rse CERN-PROD \
    --output results/cern_unique.json \
    --workers 10 \
    --rate-limit 20 \
    --time-window 1.0 \
    --log-level DEBUG
```

## Command Line Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--rse` | Yes | - | RSE name to check for unique replicas |
| `--output` | No | `unique_replicas.json` | Output file path for results |
| `--workers` | No | `5` | Number of worker threads |
| `--rate-limit` | No | `10` | Maximum API calls per time window |
| `--time-window` | No | `1.0` | Time window for rate limiting (seconds) |
| `--log-level` | No | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Examples

### Example 1: Basic Check
```bash
./check_unique_replicas.py --rse TIER2_US_MIT
```

### Example 2: Debug Mode with Custom Output
```bash
./check_unique_replicas.py \
    --rse TIER1_US_FNAL \
    --output fnal_results.json \
    --log-level DEBUG
```

### Example 3: High Performance Mode
```bash
./check_unique_replicas.py \
    --rse CERN-PROD \
    --workers 20 \
    --rate-limit 50 \
    --time-window 1.0
```

### Example 4: Conservative Mode for Busy Servers
```bash
./check_unique_replicas.py \
    --rse BUSY_RSE \
    --workers 2 \
    --rate-limit 5 \
    --time-window 10
```

## Output Files

### JSON Format (`unique_replicas.json`)

```json
{
  "rse": "TIER2_US_MIT",
  "timestamp": "2024-01-15T10:30:45.123456",
  "statistics": {
    "datasets_found": 150,
    "datasets_processed": 150,
    "files_checked": 15000,
    "unique_files_found": 234,
    "errors": 2,
    "skipped": 5
  },
  "unique_files": {
    "mc16_13TeV": [
      "file1.root",
      "file2.root"
    ],
    "data18_13TeV": [
      "file3.root"
    ]
  }
}
```

### CSV Format (`unique_replicas.csv`)

```csv
scope,name
data18_13TeV,file3.root
mc16_13TeV,file1.root
mc16_13TeV,file2.root
```

## Log Files

Logs are saved to the `logs/` directory with the naming pattern:
```
logs/unique_replicas_{RSE_NAME}_{TIMESTAMP}.log
```

Example log output:
```
2024-01-15 10:30:45,123 - INFO - [MainThread] - Logging to file: logs/unique_replicas_TIER2_US_MIT_20240115_103045.log
2024-01-15 10:30:45,124 - INFO - [MainThread] - Log level: INFO
2024-01-15 10:30:45,125 - INFO - [MainThread] - ============================================================
2024-01-15 10:30:45,126 - INFO - [MainThread] - Rucio Unique Replica Checker
2024-01-15 10:30:45,127 - INFO - [MainThread] - ============================================================
2024-01-15 10:30:45,128 - INFO - [MainThread] - Target RSE: TIER2_US_MIT
2024-01-15 10:30:45,129 - INFO - [MainThread] - Output file: unique_replicas.json
```

## How It Works

1. **Initialize**: Sets up Rucio client, rate limiter, and logging
2. **Discover**: Queries all datasets at the specified RSE
3. **Process**: For each dataset (in parallel):
   - Lists all files in the dataset
   - Checks replica locations for each file
   - Identifies files that only exist (in AVAILABLE state) at the target RSE
4. **Report**: Saves unique files to JSON and CSV, with statistics

## Rate Limiting

The program uses a token bucket algorithm for rate limiting:

- `--rate-limit`: Maximum number of API calls allowed
- `--time-window`: Time period (in seconds) for the rate limit

For example, `--rate-limit 10 --time-window 1.0` allows 10 calls per second.

### Recommended Settings

| Server Load | Workers | Rate Limit | Time Window |
|-------------|---------|------------|-------------|
| Light | 10 | 50 | 1.0 |
| Medium | 5 | 20 | 1.0 |
| Heavy | 2 | 5 | 2.0 |

## Performance Tuning

### Factors Affecting Performance

1. **Number of Workers** (`--workers`):
   - More workers = faster processing
   - Too many workers = API overload
   - Recommended: 5-10 for most cases

2. **Rate Limiting** (`--rate-limit`, `--time-window`):
   - Higher rate limit = faster processing
   - Too high = server errors
   - Balance with worker count

3. **Network Latency**:
   - Higher latency = need more workers to maintain throughput
   - Lower latency = fewer workers needed

### Example Configurations

**Fast processing (low server load):**
```bash
--workers 20 --rate-limit 100 --time-window 1.0
```

**Balanced (medium server load):**
```bash
--workers 5 --rate-limit 10 --time-window 1.0
```

**Conservative (high server load):**
```bash
--workers 2 --rate-limit 5 --time-window 2.0
```

## Troubleshooting

### Issue: "Failed to initialize Rucio client"

**Solution**: Ensure Rucio credentials are properly configured:
```bash
export RUCIO_ACCOUNT=your_account
export RUCIO_AUTH_TYPE=userpass
# Add other required environment variables
```

### Issue: "RSE not found"

**Solution**: Verify the RSE name is correct:
```bash
rucio list-rses
```

### Issue: Rate limit errors

**Solution**: Reduce rate limit and workers:
```bash
--workers 2 --rate-limit 5 --time-window 2.0
```

### Issue: Program too slow

**Solution**: Increase parallelism:
```bash
--workers 10 --rate-limit 50
```

### Issue: Memory usage too high

**Solution**: Reduce worker count:
```bash
--workers 2
```

## Architecture

### Class: `RateLimiter`

Thread-safe rate limiter using token bucket algorithm.

**Methods:**
- `acquire()`: Wait if necessary and acquire permission for an API call
- `release()`: Release after call completes

### Class: `UniqueReplicaChecker`

Main checker class coordinating all operations.

**Key Methods:**
- `get_datasets_at_rse()`: Fetch all datasets at RSE
- `get_files_in_dataset()`: Get files in a dataset
- `check_replica_locations()`: Query replica locations
- `is_unique_at_rse()`: Determine if file is unique
- `process_dataset()`: Process single dataset (runs in thread pool)
- `run()`: Main execution orchestration

### Thread Safety

All shared data structures are protected:
- `unique_files`: Protected by `unique_files_lock`
- `stats`: Protected by `stats_lock`
- `rate_limiter`: Uses internal semaphore and lock

## Statistics Tracking

The program tracks:
- **datasets_found**: Total datasets discovered at RSE
- **datasets_processed**: Datasets successfully processed
- **files_checked**: Total files examined
- **unique_files_found**: Files with only one copy
- **errors**: Number of errors encountered
- **skipped**: Datasets skipped (e.g., empty)

## Contributing

To contribute improvements:
1. Test changes thoroughly
2. Update documentation
3. Follow existing code style
4. Add logging for new operations

## License

This tool uses the Rucio client libraries. See the Rucio project for license information.

## Support

For issues related to:
- **This tool**: Check logs in debug mode (`--log-level DEBUG`)
- **Rucio client**: Consult Rucio documentation
- **RSE access**: Contact your Rucio administrator

## Author

Created as part of the Rucio ecosystem for replica management.
