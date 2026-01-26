"""Fix truncated JSON file by removing incomplete last entry."""
import json

file_path = "output/extracted_data.json"

print("Reading truncated JSON file...")
with open(file_path, 'r') as f:
    content = f.read()

print(f"File size: {len(content)} bytes")

# Find the last complete entry
# Look for the pattern where one entry ends and next begins: },\n  "Q
last_good_pos = content.rfind('},\n  "Q')

if last_good_pos > 0:
    # Find where the incomplete entry starts
    search_start = last_good_pos + 3
    incomplete_start = content.find('"Q', search_start)

    if incomplete_start > 0:
        # Truncate before the incomplete entry
        truncated = content[:incomplete_start-4].rstrip()
        if truncated.endswith(','):
            truncated = truncated[:-1]
        truncated += '\n}'

        try:
            data = json.loads(truncated)
            print(f"Fixed JSON with {len(data)} complete items")

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Saved fixed file: {file_path}")
        except json.JSONDecodeError as e:
            print(f"Error: {e}")
            print("Trying alternative method...")

            # Find last complete sitelinks array
            last_sitelinks = content.rfind('"sitelinks": [')
            if last_sitelinks > 0:
                bracket_count = 0
                i = last_sitelinks + 14
                while i < len(content):
                    if content[i] == '[':
                        bracket_count += 1
                    elif content[i] == ']':
                        bracket_count -= 1
                        if bracket_count == 0:
                            close_brace = content.find('}', i)
                            if close_brace > 0:
                                truncated2 = content[:close_brace+1] + '\n}'
                                try:
                                    data = json.loads(truncated2)
                                    print(f"Fixed JSON with {len(data)} items")
                                    with open(file_path, 'w') as f:
                                        json.dump(data, f, indent=2)
                                    print("Saved!")
                                except json.JSONDecodeError as e2:
                                    print(f"Failed: {e2}")
                            break
                    i += 1
else:
    print("Could not find entry boundary")

print("Done!")
