"""Debug: inspect raw PatentsView response for WIPO fields."""
import requests, os, json, sys

sys.path.insert(0, ".")
from AssignmentSearch import PATENTSVIEW_API_KEY

key = PATENTSVIEW_API_KEY
print("Key present:", bool(key))

headers = {"X-Api-Key": key, "Accept": "application/json"}
patent_url = "https://search.patentsview.org/api/v1/patent/"

# Use a real patent from the patentnumbers.txt
with open("patentnumbers.txt") as f:
    pids = [ln.strip() for ln in f if ln.strip()][:3]
print("Testing with:", pids)

# Test 1: wipo.sector_title + wipo.field_title from patent endpoint
body1 = {
    "q": {"patent_id": pids},
    "f": ["patent_id", "patent_title",
          "wipo.sector_title", "wipo.field_title", "wipo.wipo_sequence",
          "cpc_current.cpc_group_id", "cpc_current.cpc_sequence"],
    "o": {"size": 10, "pad_patent_id": False},
}
r1 = requests.post(patent_url, headers=headers, json=body1)
print("\n=== Test1: wipo.sector_title + wipo.field_title ===")
print("Status:", r1.status_code)
data1 = r1.json()
for p in data1.get("patents", []):
    print(f"\nPatent {p.get('patent_id')}:")
    print("  wipo:", json.dumps(p.get("wipo"), indent=4))
    print("  cpc_current:", json.dumps(p.get("cpc_current"), indent=4))

# Test 2: bare wipo (no dot notation) — see full structure returned
body2 = {
    "q": {"patent_id": pids[:1]},
    "f": ["patent_id", "wipo"],
    "o": {"size": 5},
}
r2 = requests.post(patent_url, headers=headers, json=body2)
print("\n=== Test2: bare 'wipo' field ===")
print("Status:", r2.status_code)
print(json.dumps(r2.json(), indent=2)[:1500])

# Test 3: check what fields are available on wipo sub-entity
body3 = {
    "q": {"patent_id": pids[:1]},
    "f": ["patent_id",
          "wipo.wipo_id", "wipo.wipo_sequence",
          "wipo.sector_title", "wipo.field_title",
          "wipo.wipo_field", "wipo.wipo_field_id"],
    "o": {"size": 5},
}
r3 = requests.post(patent_url, headers=headers, json=body3)
print("\n=== Test3: all wipo sub-fields ===")
print("Status:", r3.status_code)
print(json.dumps(r3.json(), indent=2)[:2000])
