#!/usr/bin/env python3
"""
MAUDE Batch Extractor
Standalone script to extract raw data from FDA MAUDE database for batch links

This script extracts data from the FDA MAUDE database using the FDA API
for a list of MDRFOI__ID values provided in batch links.
"""

import requests
import json
import csv
import time
import re
import glob
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import os

class FDAExtractor:
    def __init__(self):
        self.base_url = "https://api.fda.gov/device/event.json"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.request_delay = 0.0  # Start with no delay (optimal based on testing)
        self.rate_limit_errors = 0
    
    def auto_detect_maude_file(self):
        """Auto-detect FDA MAUDE URL files in current directory"""
        # Look for common file patterns
        patterns = ['*.txt', '*.csv', 'Batch*', '*maude*', '*fda*', '*url*']
        candidates = []
        
        for pattern in patterns:
            files = glob.glob(pattern)
            for file in files:
                if os.path.isfile(file) and not file.startswith('.'):
                    # Check if file contains FDA MAUDE URLs
                    try:
                        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read(1000)  # Read first 1000 chars
                            if 'MDRFOI__ID=' in content and 'accessdata.fda.gov' in content:
                                candidates.append(file)
                    except:
                        continue
        
        if not candidates:
            return None
        
        # Return the first candidate (or could be more sophisticated)
        return candidates[0]
        
    def parse_batch_links(self, file_path):
        """Parse the batch links file to extract MDRFOI__ID values"""
        mdrfoi_ids = []
        
        print(f"Reading batch links from: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and line.startswith('https://'):
                    # Extract MDRFOI__ID from URL
                    try:
                        parsed_url = urlparse(line)
                        query_params = parse_qs(parsed_url.query)
                        mdrfoi_id = query_params.get('MDRFOI__ID', [None])[0]
                        if mdrfoi_id:
                            mdrfoi_ids.append(mdrfoi_id)
                        else:
                            print(f"Warning: Could not extract MDRFOI__ID from line {line_num}: {line}")
                    except Exception as e:
                        print(f"Error parsing line {line_num}: {e}")
        
        print(f"Extracted {len(mdrfoi_ids)} MDRFOI__ID values")
        return mdrfoi_ids
    
    def fetch_record_by_id(self, mdrfoi_id, verbose=False):
        """Fetch a single record by MDRFOI__ID using FDA API with adaptive rate limiting"""
        search_query = f"mdr_report_key:{mdrfoi_id}"
        url = f"{self.base_url}?search={search_query}&limit=1"
        
        try:
            if verbose:
                print(f"Fetching data for MDRFOI__ID: {mdrfoi_id}")
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if results:
                    # Reset rate limit errors on successful request
                    self.rate_limit_errors = 0
                    return results[0]  # Return the first (and should be only) result
                else:
                    if verbose:
                        print(f"No results found for MDRFOi__ID: {mdrfoi_id}")
                    return None
            elif response.status_code == 429:  # Rate limited
                self.rate_limit_errors += 1
                # Adaptive backoff: increase delay exponentially
                self.request_delay = min(2.0, 0.5 * (2 ** self.rate_limit_errors))
                if verbose:
                    print(f"Rate limited! Increasing delay to {self.request_delay}s")
                time.sleep(self.request_delay)
                return None
            else:
                if verbose:
                    print(f"API Error {response.status_code} for MDRFOI__ID: {mdrfoi_id}")
                return None
                
        except requests.exceptions.RequestException as e:
            if verbose:
                print(f"Request error for MDRFOI__ID {mdrfoi_id}: {e}")
            return None
        except Exception as e:
            if verbose:
                print(f"Unexpected error for MDRFOI__ID {mdrfoi_id}: {e}")
            return None
    
    def extract_all_fields(self, record):
        """Extract all fields from the raw JSON record (as-is from database)"""
        if not record:
            return {}
        
        # Return the complete raw record as-is
        return record
    
    def save_to_json(self, data, filename):
        """Save extracted data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to JSON file: {filename}")
    
    def save_to_csv(self, data, filename):
        """Save extracted data to CSV file (flattened)"""
        if not data:
            print("No data to save to CSV")
            return
        
        # Flatten the data for CSV
        flattened_data = []
        for record in data:
            flattened_record = self.flatten_dict(record)
            flattened_data.append(flattened_record)
        
        if flattened_data:
            # Get all unique keys
            all_keys = set()
            for record in flattened_data:
                all_keys.update(record.keys())
            
            # Write CSV
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                writer.writeheader()
                writer.writerows(flattened_data)
            print(f"Data saved to CSV file: {filename}")
    
    def flatten_dict(self, d, parent_key='', sep='_'):
        """Recursively flatten nested dictionaries"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Handle lists by creating indexed keys
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def extract_batch_data(self, batch_links_file, output_dir="extracted_data", max_records=None, verbose=False):
        """Main function to extract data for all batch links"""
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Parse batch links
        mdrfoi_ids = self.parse_batch_links(batch_links_file)
        
        if not mdrfoi_ids:
            print("No valid MDRFOI__ID values found in batch links file")
            return []
        
        # Apply limit if specified
        if max_records and max_records < len(mdrfoi_ids):
            mdrfoi_ids = mdrfoi_ids[:max_records]
            print(f"Limited to first {max_records} records for processing")
        
        # Extract data for each ID
        all_data = []
        successful_extractions = 0
        failed_extractions = 0
        
        print(f"\nStarting data extraction for {len(mdrfoi_ids)} records...")
        print("=" * 60)
        
        for i, mdrfoi_id in enumerate(mdrfoi_ids, 1):
            if verbose or i % 10 == 0 or i == len(mdrfoi_ids):
                print(f"\nProgress: {i}/{len(mdrfoi_ids)}")
            
            # Fetch raw data
            raw_record = self.fetch_record_by_id(mdrfoi_id, verbose=verbose)
            
            if raw_record:
                # Add metadata
                record_with_meta = {
                    'extraction_timestamp': datetime.now().isoformat(),
                    'mdrfoi_id': mdrfoi_id,
                    'raw_data': raw_record
                }
                all_data.append(record_with_meta)
                successful_extractions += 1
                if verbose:
                    print(f"✓ Successfully extracted data for MDRFOI__ID: {mdrfoi_id}")
            else:
                failed_extractions += 1
                if verbose:
                    print(f"✗ Failed to extract data for MDRFOI__ID: {mdrfoi_id}")
            
            # Add delay to be respectful to the API
            time.sleep(getattr(self, 'request_delay', 0.5))
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save raw JSON data
        json_filename = os.path.join(output_dir, f"maude_batch_data_{timestamp}.json")
        self.save_to_json(all_data, json_filename)
        
        # Save flattened CSV data
        csv_filename = os.path.join(output_dir, f"maude_batch_data_{timestamp}.csv")
        self.save_to_csv(all_data, csv_filename)
        
        # Print summary
        print("\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Total records processed: {len(mdrfoi_ids)}")
        print(f"Successful extractions: {successful_extractions}")
        print(f"Failed extractions: {failed_extractions}")
        print(f"Success rate: {(successful_extractions/len(mdrfoi_ids)*100):.1f}%")
        print(f"\nOutput files:")
        print(f"  - JSON: {json_filename}")
        print(f"  - CSV: {csv_filename}")
        
        return all_data

def main():
    """Main function"""
    import sys
    import argparse
    
    print("MAUDE Batch Extractor")
    print("=" * 40)
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description="Extract FDA MAUDE data from a list of URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 batch_extractor.py                           # Use default 'Batch links' file
  python3 batch_extractor.py my_urls.txt               # Use custom file
  python3 batch_extractor.py my_urls.txt -o results    # Custom output directory
  python3 batch_extractor.py my_urls.txt --delay 0.5   # Custom delay between requests
        """
    )
    
    parser.add_argument(
        'input_file', 
        nargs='?', 
        default=None,
        help='Text file containing FDA MAUDE URLs (default: auto-detect in current directory)'
    )
    
    parser.add_argument(
        '-o', '--output', 
        default='extracted_data',
        help='Output directory for extracted data (default: "extracted_data")'
    )
    
    parser.add_argument(
        '--delay', 
        type=float, 
        default=0.0,
        help='Delay in seconds between API requests (default: 0.0 - optimal speed)'
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Limit number of records to process (for testing)'
    )
    
    parser.add_argument(
        '--verbose', '-v', 
        action='store_true',
        help='Show detailed progress information'
    )
    
    args = parser.parse_args()
    
    # Check if input file exists (only if specified)
    if args.input_file is not None and not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found!")
        print(f"Please provide a text file containing FDA MAUDE URLs.")
        print(f"Each URL should be on a separate line.")
        return 1
    
    # Initialize extractor
    extractor = FDAExtractor()
    
    # Auto-detect input file if not specified
    input_file = args.input_file
    if input_file is None:
        detected_file = extractor.auto_detect_maude_file()
        if detected_file:
            input_file = detected_file
            print(f"Auto-detected FDA MAUDE file: {input_file}")
        else:
            print("Error: No FDA MAUDE URL file found in current directory.")
            print("Please specify a file or ensure you have a file containing FDA MAUDE URLs.")
            print("Supported patterns: *.txt, *.csv, Batch*, *maude*, *fda*, *url*")
            return 1
    
    # Set custom delay if specified
    if args.delay != 0.0:
        extractor.request_delay = args.delay
        print(f"Using custom delay: {args.delay} seconds between requests")
    else:
        print(f"Using adaptive delay: starts at {extractor.request_delay}s, adjusts based on API response")
    
    try:
        # Extract data with custom parameters
        result = extractor.extract_batch_data(
            input_file, 
            args.output, 
            max_records=args.limit,
            verbose=args.verbose
        )
        
        if result:
            print(f"\n✅ Extraction completed successfully!")
            return 0
        else:
            print(f"\n❌ Extraction failed!")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Extraction interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error during extraction: {e}")
        return 1

if __name__ == "__main__":
    main()
