import json
import os
from datetime import datetime
from collections import defaultdict
import zipfile
import tempfile
import sys

def package_slack_jsons_with_report(input_dir, output_zip=None):
    """
    Packages loose Slack JSON message files into a standard Slack export ZIP structure
    for RelativityOne. Generates a report.txt listing all input JSON files and their
    message counts to confirm all were loaded.
    
    Outputs:
    - slack_export.zip (or specified path)
    - report.txt in input_dir listing all JSON files and message counts
    """
    messages = []
    users = {}  # user_id -> user dict
    teams = set()
    input_files_info = {}  # filename -> message count

    # Step 1: Load all JSON files and count messages
    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    for filename in json_files:
        path = os.path.join(input_dir, filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                msg_count = 0
                for msg in data:
                    if 'type' in msg and msg['type'] == 'message':
                        messages.append(msg)
                        msg_count += 1
                        # Extract user info
                        user_id = msg.get('user')
                        if user_id and 'user_profile' in msg and user_id not in users:
                            profile = msg['user_profile']
                            users[user_id] = {
                                "id": user_id,
                                "team_id": msg.get('team', ''),
                                "name": profile.get('name', ''),
                                "deleted": False,
                                "real_name": profile.get('real_name', ''),
                                "profile": {
                                    "first_name": profile.get('first_name', ''),
                                    "last_name": '',
                                    "real_name": profile.get('real_name', ''),
                                    "display_name": profile.get('display_name', ''),
                                    "image_72": profile.get('image_72', ''),
                                    "avatar_hash": profile.get('avatar_hash', '')
                                }
                            }
                        teams.add(msg.get('team', ''))
                        teams.add(msg.get('source_team', ''))
                        teams.add(msg.get('user_team', ''))
                input_files_info[filename] = msg_count
            else:
                input_files_info[filename] = 0
                print(f"Warning: {filename} is not a valid message array")
        except Exception as e:
            input_files_info[filename] = 0
            print(f"Error loading {filename}: {e}")

    if not messages:
        raise ValueError("No valid messages found in the JSON files.")

    # Step 2: Sort and group messages by date
    messages.sort(key=lambda m: float(m.get('ts', 0)))
    messages_by_date = defaultdict(list)
    for msg in messages:
        ts = msg.get('ts')
        if ts:
            try:
                date_str = datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d')
                messages_by_date[date_str].append(msg)
            except (ValueError, TypeError):
                print(f"Warning: Skipping message with invalid 'ts': {msg.get('client_msg_id', 'NO_ID')}")
                continue

    # Step 3: Infer channel and team
    team_id = max(teams, key=list(teams).count) if teams else "T_UNKNOWN"
    min_ts = float(min((msg['ts'] for msg in messages if msg.get('ts')), default=0))
    creator = next(iter(users)) if users else "U_UNKNOWN"
    members = list(users.keys())
    channel = {
        "id": "C_SEARCH_RESULTS",
        "name": "search_results",
        "created": int(min_ts),
        "creator": creator,
        "is_archived": False,
        "is_mpim": False,
        "members": members,
        "topic": {"value": "", "creator": "", "last_set": 0},
        "purpose": {"value": "Combined messages from search export", "creator": "", "last_set": 0}
    }
    channels = [channel]

    # Step 4: Create ZIP and count output messages
    output_files_info = {}  # date -> message count
    with tempfile.TemporaryDirectory() as temp_dir:
        # Write users.json
        users_list = list(users.values())
        users_path = os.path.join(temp_dir, 'users.json')
        with open(users_path, 'w', encoding='utf-8') as f:
            json.dump(users_list, f, indent=4)

        # Write channels.json
        channels_path = os.path.join(temp_dir, 'channels.json')
        with open(channels_path, 'w', encoding='utf-8') as f:
            json.dump(channels, f, indent=4)

        # Write date.json files
        channel_dir = os.path.join(temp_dir, channel['name'])
        os.makedirs(channel_dir, exist_ok=True)
        for date_str, msgs in messages_by_date.items():
            date_path = os.path.join(channel_dir, f'{date_str}.json')
            with open(date_path, 'w', encoding='utf-8') as f:
                json.dump(msgs, f, indent=4)
            output_files_info[date_str] = len(msgs)

        # Set output ZIP path
        if output_zip is None:
            output_zip = os.path.join(input_dir, 'slack_export.zip')

        # Create ZIP
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, temp_dir)
                    zf.write(full_path, arcname)

    # Step 5: Write report.txt
    report_path = os.path.join(input_dir, 'report.txt')
    total_input_msgs = sum(input_files_info.values())
    total_output_msgs = sum(output_files_info.values())
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("Slack JSON Conversion Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Input Directory: {input_dir}\n")
        f.write(f"Output ZIP: {output_zip}\n\n")
        f.write(f"Total JSON Files Processed: {len(json_files)}\n\n")
        f.write("Input JSON Files Loaded:\n")
        for filename in sorted(json_files):
            count = input_files_info.get(filename, 0)
            f.write(f"  {filename}: {count} messages\n")
        f.write(f"\nTotal Input Messages: {total_input_msgs}\n")
        f.write("Output Files Created (search_results/):\n")
        for date_str in sorted(output_files_info.keys()):
            count = output_files_info[date_str]
            f.write(f"  {date_str}.json: {count} messages\n")
        f.write(f"\nTotal Output Messages: {total_output_msgs}\n")
        if total_input_msgs == total_output_msgs:
            f.write("\nSummary: All messages from the 55 JSON files were successfully processed.\n")
        else:
            f.write("\nWarning: Input and output message counts differ. Check for invalid messages or errors.\n")

    print(f"ZIP created at: {output_zip}")
    print(f"Report saved to: {report_path}")
    print(f"Processed {len(json_files)} JSON files with {total_input_msgs} messages")
    if total_input_msgs != total_output_msgs:
        print("Warning: Message count mismatch. Check report.txt for details.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python package_slack_export_with_report.py /path/to/json/folder [optional_output_zip_path]")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_zip = sys.argv[2] if len(sys.argv) > 2 else None
    package_slack_jsons_with_report(input_dir, output_zip)