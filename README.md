# MAUDE Batch Extractor

A simple Python script to extract raw data from FDA MAUDE database using the FDA API.

## Quick Start

1. **Download the script:**
   ```bash
   git clone https://github.com/MohamedMaroufMD/maude-batch-extractor.git
   cd maude-batch-extractor
   ```

2. **Create a file with FDA MAUDE URLs:**
   ```
   https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID=21944442&pc=HQL
   https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID=21933846&pc=HQL
   ```

3. **Run the script:**
   ```bash
   python3 batch_extractor.py your_file.txt
   ```

## Usage

```bash
# Auto-detect FDA MAUDE files in current directory
python3 batch_extractor.py

# Use specific file
python3 batch_extractor.py my_urls.txt

# Test with limited records
python3 batch_extractor.py my_urls.txt --limit 10

# Custom output directory
python3 batch_extractor.py my_urls.txt -o results

# Show help
python3 batch_extractor.py --help
```

## Output

The script creates:
- `extracted_data/maude_batch_data_TIMESTAMP.json` - Raw data from FDA API
- `extracted_data/maude_batch_data_TIMESTAMP.csv` - Flattened data for analysis

## Features

- ✅ Works with any filename
- ✅ Auto-detects FDA MAUDE files
- ✅ Fast extraction (optimized API calls)
- ✅ Raw data preservation
- ✅ JSON and CSV output formats

## Requirements

- Python 3.6+
- No additional packages needed (uses built-in libraries)

## License

MIT License - see [LICENSE](LICENSE) file for details.