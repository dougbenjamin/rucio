#!/usr/bin/env python3
"""
Rucio Unique Replica Checker

This program finds all datasets in a specified Rucio Storage Element (RSE),
then checks all file replicas at that RSE in the AVAILABLE state to see if
another copy exists anywhere else. Files with only one copy (at the specified RSE)
are stored in a data file organized by scope and name.

Features:
- Multithreaded processing
- Rate limiting to prevent server overload
- Verbose/debug logging
- File-based logging
- Command-line configuration
"""

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock, Semaphore
from typing import Dict, List, Set, Tuple

try:
    from rucio.client import Client
    from rucio.common.exception import (
        DataIdentifierNotFound,
        RSENotFound,
        RucioException
    )
except ImportError as e:
    print(f"Error: Failed to import Rucio client libraries: {e}", file=sys.stderr)
    print("Please ensure the Rucio client is properly installed.", file=sys.stderr)
    sys.exit(1)


class RateLimiter:
    """
    Thread-safe rate limiter using token bucket algorithm.
    """

    def __init__(self, max_calls: int, time_window: float):
        """
        Initialize rate limiter.

        Args:
            max_calls: Maximum number of calls allowed in the time window
            time_window: Time window in seconds
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.semaphore = Semaphore(max_calls)
        self.lock = Lock()
        self.call_times = []

    def __call__(self, func):
        """Decorator to rate limit a function."""
        def wrapper(*args, **kwargs):
            self.acquire()
            try:
                return func(*args, **kwargs)
            finally:
                self.release()
        return wrapper

    def acquire(self):
        """Acquire permission to make a call."""
        with self.lock:
            now = time.time()
            # Remove calls outside the time window
            self.call_times = [t for t in self.call_times if now - t < self.time_window]

            # If we're at the limit, wait
            if len(self.call_times) >= self.max_calls:
                sleep_time = self.time_window - (now - self.call_times[0])
                if sleep_time > 0:
                    logging.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    # Clean up again after sleeping
                    now = time.time()
                    self.call_times = [t for t in self.call_times if now - t < self.time_window]

            self.call_times.append(now)

    def release(self):
        """Release after a call completes."""
        pass


class UniqueReplicaChecker:
    """
    Main class to check for unique replicas at a specified RSE.
    """

    def __init__(self, rse: str, output_file: str, rate_limit: int,
                 time_window: float, max_workers: int, log_level: str):
        """
        Initialize the checker.

        Args:
            rse: RSE name to check
            output_file: Output file path for results
            rate_limit: Maximum API calls per time window
            time_window: Time window for rate limiting (seconds)
            max_workers: Maximum number of worker threads
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.rse = rse
        self.output_file = output_file
        self.max_workers = max_workers
        self.log_level = log_level

        # Initialize Rucio client
        try:
            self.client = Client()
            logging.info("Successfully initialized Rucio client")
        except Exception as e:
            logging.error(f"Failed to initialize Rucio client: {e}")
            raise

        # Rate limiter
        self.rate_limiter = RateLimiter(rate_limit, time_window)

        # Thread-safe data structures
        self.unique_files_lock = Lock()
        self.unique_files: Dict[str, List[str]] = defaultdict(list)  # scope -> [names]

        # Statistics
        self.stats_lock = Lock()
        self.stats = {
            'datasets_found': 0,
            'datasets_processed': 0,
            'files_checked': 0,
            'unique_files_found': 0,
            'errors': 0,
            'skipped': 0
        }

    def setup_logging(self):
        """Configure logging to file and console with immediate flushing."""
        # Create logs directory if it doesn't exist
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        # Create log filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f'unique_replicas_{self.rse}_{timestamp}.log'

        # Configure logging
        log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'

        # Get numeric log level
        numeric_level = getattr(logging, self.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f'Invalid log level: {self.log_level}')

        # Create handlers with immediate flushing
        # File handler - flush after every log message
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(logging.Formatter(log_format))

        # Stream handler - flush after every log message
        # Force unbuffered output by using line buffering
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(numeric_level)
        stream_handler.setFormatter(logging.Formatter(log_format))

        # Get root logger and configure it
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)

        # Force flush after each log record
        # Store handlers for manual flushing if needed
        self.log_handlers = [file_handler, stream_handler]

        # Ensure stdout is line buffered (flush on newline)
        if hasattr(sys.stdout, 'reconfigure'):
            try:
                sys.stdout.reconfigure(line_buffering=True)
            except Exception:
                pass  # Ignore if reconfigure not supported

        logging.info(f"Logging to file: {log_file}")
        logging.info(f"Log level: {self.log_level}")
        self.flush_logs()

    def flush_logs(self):
        """Force flush all log handlers."""
        if hasattr(self, 'log_handlers'):
            for handler in self.log_handlers:
                handler.flush()
        # Also flush stdout/stderr
        sys.stdout.flush()
        sys.stderr.flush()

    def update_stats(self, **kwargs):
        """Thread-safe statistics update."""
        with self.stats_lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value

    def print_stats(self):
        """Print current statistics."""
        with self.stats_lock:
            logging.info("=" * 60)
            logging.info("Current Statistics:")
            logging.info(f"  Datasets found: {self.stats['datasets_found']}")
            logging.info(f"  Datasets processed: {self.stats['datasets_processed']}")
            logging.info(f"  Files checked: {self.stats['files_checked']}")
            logging.info(f"  Unique files found: {self.stats['unique_files_found']}")
            logging.info(f"  Errors: {self.stats['errors']}")
            logging.info(f"  Skipped: {self.stats['skipped']}")
            logging.info("=" * 60)
        self.flush_logs()

    def get_datasets_at_rse(self) -> List[Dict[str, str]]:
        """
        Get all datasets at the specified RSE.

        Returns:
            List of dictionaries with 'scope' and 'name' keys
        """
        logging.info(f"Fetching datasets at RSE: {self.rse}")

        try:
            self.rate_limiter.acquire()
            datasets = list(self.client.list_datasets_per_rse(rse=self.rse))
            self.rate_limiter.release()

            logging.info(f"Found {len(datasets)} datasets at {self.rse}")
            self.flush_logs()
            self.update_stats(datasets_found=len(datasets))

            return datasets

        except RSENotFound:
            logging.error(f"RSE not found: {self.rse}")
            raise
        except RucioException as e:
            logging.error(f"Rucio error while fetching datasets: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while fetching datasets: {e}")
            raise

    def get_files_in_dataset(self, scope: str, name: str) -> List[Dict]:
        """
        Get all files in a dataset.

        Args:
            scope: Dataset scope
            name: Dataset name

        Returns:
            List of file dictionaries
        """
        try:
            self.rate_limiter.acquire()
            files = list(self.client.list_files(scope=scope, name=name))
            self.rate_limiter.release()

            logging.debug(f"Dataset {scope}:{name} contains {len(files)} files")
            return files

        except DataIdentifierNotFound:
            logging.warning(f"Dataset not found: {scope}:{name}")
            self.update_stats(errors=1)
            return []
        except RucioException as e:
            logging.error(f"Rucio error while fetching files for {scope}:{name}: {e}")
            self.update_stats(errors=1)
            return []
        except Exception as e:
            logging.error(f"Unexpected error while fetching files for {scope}:{name}: {e}")
            self.update_stats(errors=1)
            return []

    def check_replica_locations(self, dids: List[Dict[str, str]]) -> Dict[str, Dict]:
        """
        Check replica locations for a list of DIDs.

        Args:
            dids: List of dictionaries with 'scope' and 'name' keys

        Returns:
            Dictionary mapping "scope:name" to replica information
        """
        if not dids:
            return {}

        try:
            self.rate_limiter.acquire()
            replicas = self.client.list_replicas(
                dids=dids,
                all_states=True,  # Include all replica states
                ignore_availability=False
            )
            self.rate_limiter.release()

            # Convert generator to dictionary for easier lookup
            replica_dict = {}
            for replica in replicas:
                key = f"{replica['scope']}:{replica['name']}"
                replica_dict[key] = replica

            return replica_dict

        except RucioException as e:
            logging.error(f"Rucio error while checking replicas: {e}")
            self.update_stats(errors=len(dids))
            return {}
        except Exception as e:
            logging.error(f"Unexpected error while checking replicas: {e}")
            self.update_stats(errors=len(dids))
            return {}

    def is_unique_at_rse(self, replica_info: Dict) -> bool:
        """
        Check if a file replica is unique (only exists at the target RSE).

        Args:
            replica_info: Replica information dictionary

        Returns:
            True if the file only has an AVAILABLE copy at the target RSE
        """
        rses = replica_info.get('rses', {})
        states = replica_info.get('states', {})

        # Check if file exists at target RSE and is AVAILABLE
        if self.rse not in rses:
            logging.debug(f"File {replica_info['name']} not found at {self.rse}")
            return False

        if states.get(self.rse) != 'AVAILABLE':
            logging.debug(f"File {replica_info['name']} at {self.rse} is not AVAILABLE (state: {states.get(self.rse)})")
            return False

        # Check if file exists at any other RSE in AVAILABLE state
        available_rses = []
        for rse, state in states.items():
            if state == 'AVAILABLE':
                available_rses.append(rse)

        # File is unique if it's only available at the target RSE
        is_unique = len(available_rses) == 1 and available_rses[0] == self.rse

        if is_unique:
            logging.debug(f"File {replica_info['scope']}:{replica_info['name']} is unique at {self.rse}")
        else:
            logging.debug(f"File {replica_info['name']} also available at: {[rse for rse in available_rses if rse != self.rse]}")

        return is_unique

    def process_dataset(self, dataset: Dict[str, str]) -> Tuple[int, int]:
        """
        Process a single dataset to find unique replicas.

        Args:
            dataset: Dictionary with 'scope' and 'name' keys

        Returns:
            Tuple of (files_checked, unique_files_found)
        """
        scope = dataset['scope']
        name = dataset['name']

        logging.info(f"Processing dataset: {scope}:{name}")

        # Get all files in the dataset
        files = self.get_files_in_dataset(scope, name)
        if not files:
            logging.warning(f"No files found in dataset {scope}:{name}")
            self.update_stats(skipped=1)
            return 0, 0

        # Process files in batches to avoid overwhelming the API
        batch_size = 100
        files_checked = 0
        unique_files_found = 0

        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]

            # Prepare DIDs for replica check
            dids = [{'scope': f['scope'], 'name': f['name']} for f in batch]

            # Get replica information
            replicas = self.check_replica_locations(dids)

            # Check each file for uniqueness
            for file_info in batch:
                file_key = f"{file_info['scope']}:{file_info['name']}"

                if file_key not in replicas:
                    logging.warning(f"No replica information found for {file_key}")
                    continue

                files_checked += 1

                # Check if this file is unique at our RSE
                if self.is_unique_at_rse(replicas[file_key]):
                    with self.unique_files_lock:
                        self.unique_files[file_info['scope']].append(file_info['name'])
                    unique_files_found += 1
                    logging.info(f"Found unique file: {file_key}")
                    self.flush_logs()

        self.update_stats(
            datasets_processed=1,
            files_checked=files_checked,
            unique_files_found=unique_files_found
        )

        logging.info(f"Completed dataset {scope}:{name}: {files_checked} files checked, {unique_files_found} unique")
        self.flush_logs()

        return files_checked, unique_files_found

    def save_results(self):
        """Save unique files to output file."""
        logging.info(f"Saving results to {self.output_file}")

        try:
            output_data = {
                'rse': self.rse,
                'timestamp': datetime.now().isoformat(),
                'statistics': dict(self.stats),
                'unique_files': dict(self.unique_files)
            }

            output_path = Path(self.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2, sort_keys=True)

            logging.info(f"Successfully saved results to {self.output_file}")
            self.flush_logs()

            # Also save a simple CSV format for easy processing
            csv_file = output_path.with_suffix('.csv')
            with open(csv_file, 'w') as f:
                f.write("scope,name\n")
                for scope, names in sorted(self.unique_files.items()):
                    for name in sorted(names):
                        f.write(f"{scope},{name}\n")

            logging.info(f"Also saved CSV format to {csv_file}")
            self.flush_logs()

        except Exception as e:
            logging.error(f"Failed to save results: {e}")
            raise

    def run(self):
        """Main execution method."""
        start_time = time.time()

        try:
            # Setup logging
            self.setup_logging()

            logging.info("=" * 60)
            logging.info("Rucio Unique Replica Checker")
            logging.info("=" * 60)
            logging.info(f"Target RSE: {self.rse}")
            logging.info(f"Output file: {self.output_file}")
            logging.info(f"Max workers: {self.max_workers}")
            logging.info(f"Rate limit: {self.rate_limiter.max_calls} calls per {self.rate_limiter.time_window}s")
            logging.info("=" * 60)
            self.flush_logs()

            # Get all datasets at the RSE
            datasets = self.get_datasets_at_rse()

            if not datasets:
                logging.warning(f"No datasets found at RSE {self.rse}")
                return

            # Process datasets using thread pool
            logging.info(f"Starting multithreaded processing with {self.max_workers} workers")
            self.flush_logs()

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all dataset processing tasks
                future_to_dataset = {
                    executor.submit(self.process_dataset, dataset): dataset
                    for dataset in datasets
                }

                # Process completed tasks
                for future in as_completed(future_to_dataset):
                    dataset = future_to_dataset[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error processing dataset {dataset['scope']}:{dataset['name']}: {e}")
                        self.flush_logs()
                        self.update_stats(errors=1)

                # Print periodic statistics
                if self.stats['datasets_processed'] % 10 == 0:
                    self.print_stats()

            # Final statistics
            elapsed_time = time.time() - start_time
            logging.info("=" * 60)
            logging.info("Processing Complete!")
            logging.info("=" * 60)
            self.print_stats()
            logging.info(f"Total elapsed time: {elapsed_time:.2f} seconds")
            logging.info("=" * 60)

            # Save results
            self.save_results()

            # Print summary
            with self.unique_files_lock:
                total_unique = sum(len(names) for names in self.unique_files.values())
                logging.info(f"\nFound {total_unique} unique files across {len(self.unique_files)} scopes")

                if total_unique > 0:
                    logging.info("\nUnique files by scope:")
                    for scope in sorted(self.unique_files.keys()):
                        count = len(self.unique_files[scope])
                        logging.info(f"  {scope}: {count} files")

            self.flush_logs()

        except KeyboardInterrupt:
            logging.warning("\nReceived interrupt signal, shutting down...")
            self.flush_logs()
            self.save_results()
            sys.exit(1)
        except Exception as e:
            logging.error(f"Fatal error: {e}", exc_info=True)
            self.flush_logs()
            sys.exit(1)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Check for unique file replicas at a Rucio RSE',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check for unique replicas at CERN-PROD RSE
  %(prog)s --rse CERN-PROD

  # Use debug mode with custom output file
  %(prog)s --rse TIER2_US_MIT --output results_mit.json --log-level DEBUG

  # Increase parallelism and rate limit
  %(prog)s --rse TIER1_US_FNAL --workers 10 --rate-limit 50

  # Conservative rate limiting for busy servers
  %(prog)s --rse BUSY_RSE --rate-limit 5 --time-window 10
        """
    )

    parser.add_argument(
        '--rse',
        required=True,
        help='RSE name to check for unique replicas'
    )

    parser.add_argument(
        '--output',
        default='unique_replicas.json',
        help='Output file path for results (default: unique_replicas.json)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of worker threads (default: 5)'
    )

    parser.add_argument(
        '--rate-limit',
        type=int,
        default=10,
        help='Maximum API calls per time window (default: 10)'
    )

    parser.add_argument(
        '--time-window',
        type=float,
        default=1.0,
        help='Time window for rate limiting in seconds (default: 1.0)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()

    checker = UniqueReplicaChecker(
        rse=args.rse,
        output_file=args.output,
        rate_limit=args.rate_limit,
        time_window=args.time_window,
        max_workers=args.workers,
        log_level=args.log_level
    )

    checker.run()


if __name__ == '__main__':
    main()
