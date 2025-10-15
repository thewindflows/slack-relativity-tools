# Slack JSON to RelativityOne Converter

This script packages loose Slack JSON files (e.g., message arrays from search exports) into a ZIP structure compatible with RelativityOne's Slack-to-RSMF conversion for proper chat display.

## Usage
Run the script from the command line:
```bash
python package_slack_export_with_report.py /path/to/json/folder [optional_output_zip_path]
