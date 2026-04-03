import requests

payload = {
    "branch_file": "./data including branch.xlsx",
    "targets": [
        "./video_catalog_enriched_mapped.xlsx",
        "./video_catalog_with_content_mapped.xlsx"
    ]
}

r = requests.post("http://localhost:8000/notifications/admin/remap-branches", json=payload, timeout=60)
print("status:", r.status_code)
print(r.json())
