"""Debug: inspect raw USPTO ODP wrapper response for CPC/WIPO fallback fields."""
import requests, os, json, sys

sys.path.insert(0, ".")
from AssignmentSearch import ODP_API_KEY

key = ODP_API_KEY
print("Key present:", bool(key))

headers = {"X-API-KEY": key, "Accept": "application/json"}
patent_url = "https://api.uspto.gov/api/v1/patent/applications/search"

# Use a real patent from the patentnumbers.txt
with open("patentnumbers.txt") as f:
    pids = [ln.strip() for ln in f if ln.strip()][:3]
print("Testing with:", pids)

# Test 1: ODP wrapper search by patent number
body1 = {
    "q": f'applicationMetaData.patentNumber:{pids[0]}',
    "pagination": {"offset": 0, "limit": 1},
}
r1 = requests.post(patent_url, headers=headers, json=body1)
print("\n=== Test1: wrapper search ===")
print("Status:", r1.status_code)
data1 = r1.json()
for p in data1.get("patentFileWrapperDataBag", []):
    am = p.get("applicationMetaData", {})
    print(f"\nPatent {am.get('patentNumber')}:")
    print("  cpcClassificationBag:", json.dumps(am.get("cpcClassificationBag"), indent=4))
    print("  grant xml:", p.get("grantDocumentMetaData", {}).get("fileLocationURI"))

# Test 2: inspect XML for claims
xml_url = data1.get("patentFileWrapperDataBag", [{}])[0].get("grantDocumentMetaData", {}).get("fileLocationURI")
if xml_url:
    xml = requests.get(xml_url, headers=headers).text
    print("\nXML head:", xml[:800])
