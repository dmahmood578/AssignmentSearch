#!/usr/bin/env python3
"""
AssignmentSearch.py

Modes:
- bypatentnumber: input patent numbers or a .txt file
- byassignee: input assignee names or a .txt file (via USPTO ODP)

Pipeline:
Assignee → USPTO ODP → Patent Numbers → USPTO ODP → Assignment CSV
"""

import argparse
import os
import difflib
import requests
import pandas as pd
import sys
import re
import json
import time
import html
from typing import List, Tuple, Optional, Any, Dict
try:
    from tqdm import tqdm
except Exception:
    tqdm = None
import urllib.parse

from dotenv import load_dotenv
# Load variables from .env file
load_dotenv()

USPTO_API_KEY = os.getenv("USPTO_API_KEY")  # DO NOT hardcode
ODP_API_KEY = USPTO_API_KEY


# -------------------- HEADERS --------------------

def get_headers():
    return {
        "X-API-KEY": ODP_API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


def _get_odp_api_key(api_key: Optional[str] = None) -> Optional[str]:
    return api_key or ODP_API_KEY


def _get_odp_headers(api_key: Optional[str] = None) -> Dict[str, str]:
    key = _get_odp_api_key(api_key)
    return {
        "X-API-KEY": key or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _odp_post_with_retry(url: str, headers: dict, body: dict, max_retries: int = 3, debug: bool = False):
    attempt = 0
    r = None
    while attempt < max_retries:
        r = requests.post(url, headers=headers, json=body)
        if r.status_code == 200:
            return r
        if r.status_code == 429:
            sleep_for = 5
            try:
                detail = r.json().get("detail", "")
                m = re.search(r"(\d+)\s*seconds?", detail)
                if m:
                    sleep_for = int(m.group(1)) + 1
            except Exception:
                pass
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = int(retry_after)
                except Exception:
                    pass
            if debug:
                print(f"⚠️  ODP 429: sleeping {sleep_for}s before retry")
            time.sleep(sleep_for)
            attempt += 1
            continue
        if 500 <= r.status_code < 600:
            backoff = 2 ** attempt
            if debug:
                print(f"⚠️  ODP {r.status_code}: retrying in {backoff}s")
            time.sleep(backoff)
            attempt += 1
            continue
        if debug:
            print(f"❌ ODP {r.status_code}: {r.text}")
        return r
    return r


def _odp_wrapper_search(query: str, api_key: Optional[str] = None, offset: int = 0, limit: int = 100, debug: bool = False):
    payload = {"q": query, "pagination": {"offset": offset, "limit": limit}}
    return _odp_post_with_retry(
        "https://api.uspto.gov/api/v1/patent/applications/search",
        _get_odp_headers(api_key),
        payload,
        debug=debug,
    )


# -------------------- UTILITIES --------------------

def _safe_get_name(item: dict) -> str:
    for k in ("inventorName", "assigneeNameText", "name", "fullName",
              "partyNameText", "applicantNameText"):
        v = item.get(k)
        if v:
            return v.strip()
    first = item.get("firstName", "")
    last = item.get("lastName", "")
    return f"{first} {last}".strip()


def _format_address(addr: Any) -> str:
    """Format one address object (or scalar) into a single readable line."""
    if isinstance(addr, str):
        return addr.strip()
    if not isinstance(addr, dict):
        return ""

    parts = [
        addr.get("nameLineOneText"),
        addr.get("nameLineTwoText"),
        addr.get("addressLineOneText"),
        addr.get("addressLineTwoText"),
        addr.get("addressLineThreeText"),
        addr.get("cityName"),
        addr.get("geographicRegionCode") or addr.get("geographicRegionName"),
        addr.get("postalCode"),
        addr.get("countryName") or addr.get("countryCode"),
    ]
    return ", ".join([str(p).strip() for p in parts if p and str(p).strip()])


def _extract_application_and_entity_status(am: Dict[str, Any]) -> Tuple[str, str]:
    """Extract application status and entity status strings from applicationMetaData."""
    status_desc = am.get("applicationStatusDescriptionText") or ""
    status_code = am.get("applicationStatusCode")
    status_date = am.get("applicationStatusDate")

    status_bits = [status_desc]
    if status_code not in (None, ""):
        status_bits.append(f"code={status_code}")
    if status_date:
        status_bits.append(f"date={status_date}")
    application_status = " | ".join([b for b in status_bits if b])

    es = am.get("entityStatusData") or {}
    # Use businessEntityStatusCategory as it's descriptive ("Regular Undiscounted", "Small", "Micro")
    # No need to add redundant boolean indicators
    category = es.get("businessEntityStatusCategory")
    entity_status = str(category) if category else ""

    return application_status, entity_status


def _extract_wrapper_correspondence_address(patent_data: Dict[str, Any]) -> str:
    """Extract correspondence address from wrapper-level metadata if available."""
    am = patent_data.get("applicationMetaData", {}) or {}
    bags = []
    for key in ("correspondenceAddressBag", "correspondenceAddress"):
        v = am.get(key)
        if isinstance(v, list):
            bags.extend(v)
        elif isinstance(v, dict):
            bags.append(v)
    formatted = [_format_address(x) for x in bags]
    return "; ".join([x for x in formatted if x])


def _extract_assignment_correspondence_address(assignment: Dict[str, Any]) -> str:
    """Extract correspondence address from an assignment record."""
    bags = []
    for key in ("correspondenceAddress", "correspondenceAddressBag"):
        v = assignment.get(key)
        if isinstance(v, list):
            bags.extend(v)
        elif isinstance(v, dict):
            bags.append(v)

    entries = []
    for b in bags:
        if not isinstance(b, dict):
            continue
        name = b.get("correspondentNameText") or b.get("nameLineOneText") or ""
        addr = _format_address(b)
        entry = " | ".join([x for x in [name, addr] if x])
        if entry:
            entries.append(entry)

    return "; ".join(entries)


def _extract_attorney_info(patent_data: Dict[str, Any]) -> Tuple[str, str]:
    """Best-effort extraction of attorney name(s) and address(es) from wrapper payload."""
    am = patent_data.get("applicationMetaData", {}) or {}

    candidates = []
    for holder in (patent_data, am):
        if not isinstance(holder, dict):
            continue
        for key in (
            "attorneyBag",
            "attorneyInformationBag",
            "attorneyDataBag",
            "representativeBag",
            "correspondenceAttorneyBag",
            "powerOfAttorneyBag",
        ):
            v = holder.get(key)
            if isinstance(v, list):
                candidates.extend([x for x in v if isinstance(x, dict)])

    names = []
    addresses = []
    for item in candidates:
        name = (
            item.get("attorneyNameText")
            or item.get("representativeNameText")
            or item.get("name")
            or item.get("nameText")
            or _safe_get_name(item)
        )
        if name and name not in names:
            names.append(name)

        addr_sources = []
        for key in ("address", "addressData"):
            v = item.get(key)
            if isinstance(v, dict):
                addr_sources.append(v)
        for key in ("addressBag", "correspondenceAddress", "correspondenceAddressBag"):
            v = item.get(key)
            if isinstance(v, list):
                addr_sources.extend([x for x in v if isinstance(x, dict)])
            elif isinstance(v, dict):
                addr_sources.append(v)

        # Sometimes address fields are on the same object as attorney fields.
        if any(k in item for k in ("addressLineOneText", "cityName", "postalCode", "countryName")):
            addr_sources.append(item)

        for src in addr_sources:
            text = _format_address(src)
            if text and text not in addresses:
                addresses.append(text)

    # Fallback: some records expose correspondent (often attorney/firm) only within assignmentBag.
    if not names or not addresses:
        for assignment in patent_data.get("assignmentBag", []) or []:
            if not isinstance(assignment, dict):
                continue
            for key in ("correspondenceAddress", "correspondenceAddressBag"):
                cbag = assignment.get(key)
                if isinstance(cbag, dict):
                    cbag = [cbag]
                if not isinstance(cbag, list):
                    continue
                for c in cbag:
                    if not isinstance(c, dict):
                        continue
                    cname = c.get("correspondentNameText") or c.get("nameLineOneText")
                    if cname and cname not in names:
                        names.append(cname)
                    caddr = _format_address(c)
                    if caddr and caddr not in addresses:
                        addresses.append(caddr)

    return "; ".join(names), "; ".join(addresses)


def _xml_local_name(tag: str) -> str:
    return tag.split("}", 1)[-1]


def _xml_text(element: Any) -> str:
    if element is None:
        return ""
    text = "".join(element.itertext()) if hasattr(element, "itertext") else str(element)
    return re.sub(r"\s+", " ", html.unescape(text or "")).strip()


def _extract_patent_xml_text(xml_text: str) -> Tuple[str, str, List[Dict[str, str]]]:
    """Extract title, abstract, and detailed claims from USPTO patent XML."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_text)
    title = ""
    abstract = ""
    claims: List[Dict[str, str]] = []

    for node in root.iter():
        node_name = _xml_local_name(node.tag)
        if not title and node_name == "invention-title":
            title = _xml_text(node)
        elif not abstract and node_name == "abstract":
            abstract = _xml_text(node)
        elif node_name == "claim":
            claim_number = str(node.attrib.get("num") or node.attrib.get("id") or "").strip()
            claim_text_node = None
            for child in node:
                if _xml_local_name(child.tag) == "claim-text":
                    claim_text_node = child
                    break
            claim_text = _xml_text(claim_text_node or node)
            if claim_text:
                claims.append({
                    "claim_number": claim_number.lstrip("0") or claim_number,
                    "claim_sequence": str(len(claims) + 1),
                    "claim_text": claim_text,
                    "claim_dependent": "Yes" if re.match(r"^\s*\d+\s*\.\s*The\s+", claim_text, flags=re.I) and len(claims) > 0 else "No",
                })

    return title, abstract, claims


def _fetch_odp_document_xml(file_url: str, api_key: Optional[str] = None, debug: bool = False) -> str:
    headers = _get_odp_headers(api_key)
    try:
        r = requests.get(file_url, headers=headers, timeout=30)
    except Exception as e:
        if debug:
            print(f"❌ ODP XML request failed for {file_url}: {e}")
        return ""
    if r.status_code != 200:
        if debug:
            print(f"❌ ODP XML HTTP {r.status_code} for {file_url}: {r.text[:300]}")
        return ""
    return r.text


def _wrapper_to_text_row(patent_number: str, wrapper: Dict[str, Any], xml_text: str = "") -> Dict[str, str]:
    am = wrapper.get("applicationMetaData", {}) or {}
    title = str(am.get("inventionTitle") or "").strip()
    abstract = str(am.get("abstract") or am.get("inventionAbstract") or "").strip()
    claims: List[Dict[str, str]] = []
    cpc_primary = _extract_cpc_primary(am.get("cpcClassificationBag") or [])
    if xml_text:
        xml_title, xml_abstract, claims = _extract_patent_xml_text(xml_text)
        if xml_title:
            title = xml_title
        if xml_abstract:
            abstract = xml_abstract
    return {
        "Patent Number": patent_number,
        "Patent Title": title,
        "Abstract": abstract,
        "Technology Field": _derive_wipo_from_cpc(cpc_primary),
        "CPC Primary": cpc_primary,
        "_claims_json": json.dumps(claims),
    }


def _find_odp_wrapper(identifier: str, api_key: Optional[str] = None, debug: bool = False) -> Tuple[Dict[str, Any], str]:
    """Locate a patent or publication wrapper by trying the ODP fields we know about."""
    candidates = [
        f'applicationMetaData.patentNumber:{identifier}',
        f'applicationMetaData.earliestPublicationNumber:{identifier}',
        f'applicationMetaData.publicationSequenceNumberBag:{identifier}',
        f'applicationNumberText:{identifier}',
    ]

    for query in candidates:
        r = _odp_wrapper_search(query, api_key=api_key, offset=0, limit=1, debug=debug)
        if r is None or r.status_code != 200:
            continue
        data = r.json()
        bags = data.get("patentFileWrapperDataBag", []) or []
        if bags:
            return (bags[0] if isinstance(bags[0], dict) else {}, query)

    return {}, ""


def _extract_cpc_primary(cpc_bag: List[Any]) -> str:
    if not cpc_bag:
        return ""
    values = []
    for item in cpc_bag:
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, dict):
            for key in ("cpc_group_id", "cpc_subclass_id", "cpc_classification_text", "cpcCode", "cpc_symbol_text"):
                value = item.get(key)
                if value:
                    values.append(str(value))
                    break
    if not values:
        return ""
    raw = str(values[0]).strip()
    return _cpc_group_id_to_code(raw)


def _derive_wipo_from_cpc(cpc_primary: str) -> str:
    code = (cpc_primary or "").strip().upper()
    if not code:
        return ""
    if code.startswith(("H01L", "G11C")):
        return "Electrical Engineering — Semiconductors"
    if code.startswith(("G06Q",)):
        return "Electrical Engineering — IT methods for management"
    if code.startswith(("G06F", "G06N", "G06K", "G06T")):
        return "Electrical Engineering — Computer technology"
    if code.startswith(("H04L", "H04W", "H04B", "H04J")):
        return "Electrical Engineering — Telecommunications"
    if code.startswith(("H04N", "H04S")):
        return "Electrical Engineering — Audio-visual technology"
    if code.startswith(("H03",)):
        return "Electrical Engineering — Basic communication processes"
    if code.startswith(("G01S",)):
        return "Instruments — Optics"
    if code.startswith(("G01R", "G01D", "G01N", "G01B", "G01C", "G01L")):
        return "Instruments — Measurement"
    if code.startswith(("A61",)):
        return "Instruments — Medical technology"
    if code.startswith(("C12", "C07", "A61K", "A61P")):
        return "Chemistry — Biotechnology"
    if code.startswith(("B60", "B61", "B62", "B63", "B64")):
        return "Mechanical Engineering — Transport"
    if code.startswith(("E",)):
        return "Other Fields — Civil engineering"
    if code.startswith(("F",)):
        return "Mechanical Engineering — Thermal processes and apparatus"
    if code.startswith(("A",)):
        return "Other Fields — Agriculture, food processing"
    return ""


def _fetch_application_metadata(application_number: str, delay: float = 0.0, debug: bool = False) -> Dict[str, Any]:
    """Fetch patent file wrapper metadata for an application number from USPTO /meta-data."""
    app_text = urllib.parse.quote(str(application_number), safe="")
    url = f"https://api.uspto.gov/api/v1/patent/applications/{app_text}/meta-data"

    max_retries = 3
    attempt = 0
    r = None
    while attempt < max_retries:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            break
        if r.status_code == 429:
            retry_after = r.headers.get('Retry-After')
            sleep_for = 5
            if retry_after:
                try:
                    sleep_for = int(retry_after)
                except Exception:
                    sleep_for = 5
            if debug:
                print(f"⚠️  USPTO meta-data API 429: sleeping for {sleep_for}s before retry (app {application_number})")
            time.sleep(sleep_for)
            attempt += 1
            continue
        if 500 <= r.status_code < 600:
            backoff = 2 ** attempt
            if debug:
                print(f"⚠️  USPTO meta-data API server error {r.status_code}, retrying in {backoff}s")
            time.sleep(backoff)
            attempt += 1
            continue
        if debug:
            print(f"❌ USPTO meta-data API error {r.status_code}: {r.text} (app {application_number})")
        return {}

    if r is None or r.status_code != 200:
        return {}

    data = r.json()
    bags = data.get("patentFileWrapperDataBag", [])
    if not bags:
        return {}

    if delay and delay > 0:
        time.sleep(delay)

    return bags[0] if isinstance(bags[0], dict) else {}


# -------------------- ODP ASSIGNMENT PIPELINE (UNCHANGED) --------------------

def extract_inventors_and_date(patent_data: dict) -> Tuple[str, str, str, str]:
    inventors = []
    bags = []

    if isinstance(patent_data.get("inventorBag"), list):
        bags.extend(patent_data["inventorBag"])
    if isinstance(patent_data.get("applicantBag"), list):
        bags.extend(patent_data["applicantBag"])

    am = patent_data.get("applicationMetaData", {}) or {}
    for key in ("inventorBag", "applicantBag"):
        if isinstance(am.get(key), list):
            bags.extend(am[key])

    for b in bags:
        name = _safe_get_name(b)
        if name and name not in inventors:
            inventors.append(name)

    date_regex = re.compile(r"\d{4}-\d{2}-\d{2}")

    def grab(d, keys):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str):
                m = date_regex.search(v)
                if m:
                    return m.group(0)
        return ""

    filing = grab(am, ["filingDate", "effectiveFilingDate"])
    issue = grab(am, ["grantDate", "patentIssueDate"])
    pub = grab(am, ["earliestPublicationDate", "pctPublicationDate"])

    return ("; ".join(inventors), filing, issue, pub)


def process_patent_assignments(patent_number: str, delay: float = 0.0) -> pd.DataFrame:
    """Query USPTO ODP for a patent's wrapper and return assignment rows.
    Includes retry/backoff for 429 and 5xx errors and an optional inter-request delay.
    """
    url = "https://api.uspto.gov/api/v1/patent/applications/search"
    payload = {
        "q": f"applicationMetaData.patentNumber:{patent_number}",
        "pagination": {"offset": 0, "limit": 1}
    }

    max_retries = 3
    attempt = 0
    r = None

    while attempt < max_retries:
        r = requests.post(url, headers=get_headers(), json=payload)
        if r.status_code == 200:
            break
        # Handle 429 (Rate limited)
        if r.status_code == 429:
            # Prefer Retry-After header if present, else try to parse body for seconds
            retry_after = r.headers.get('Retry-After')
            sleep_for = None
            if retry_after:
                try:
                    sleep_for = int(retry_after)
                except Exception:
                    # could be HTTP-date, fallback to parse body
                    sleep_for = None
            if not sleep_for:
                try:
                    detail = r.json().get('detail', '')
                    m = re.search(r"(\d+)\s*seconds?", detail)
                    if m:
                        sleep_for = int(m.group(1)) + 1
                except Exception:
                    sleep_for = 5
            sleep_for = sleep_for or 5
            print(f"⚠️  USPTO 429: sleeping for {sleep_for}s before retry (patent {patent_number})")
            time.sleep(sleep_for)
            attempt += 1
            continue
        # 5xx server errors -> exponential backoff
        if 500 <= r.status_code < 600:
            backoff = (2 ** attempt)
            print(f"⚠️  USPTO server error {r.status_code}, retrying in {backoff}s (patent {patent_number})")
            time.sleep(backoff)
            attempt += 1
            continue
        # Client errors or others: return an error row
        print(f"❌ USPTO error {r.status_code}: {r.text} (patent {patent_number})")
        return pd.DataFrame([{
            "Patent Number": patent_number,
            "Note": f"API error {r.status_code}"
        }])

    if r is None or r.status_code != 200:
        return pd.DataFrame([{
            "Patent Number": patent_number,
            "Note": "Request failed after retries"
        }])

    data = r.json()
    bags = data.get("patentFileWrapperDataBag", [])
    if not bags:
        return pd.DataFrame([{
            "Patent Number": patent_number,
            "Note": "Not found"
        }])

    # optional delay to space out subsequent calls
    if delay and delay > 0:
        time.sleep(delay)

    patent_data = bags[0]
    inventors, filing, issue, pub = extract_inventors_and_date(patent_data)
    am = patent_data.get("applicationMetaData", {}) or {}
    application_status, entity_status = _extract_application_and_entity_status(am)
    wrapper_correspondence = _extract_wrapper_correspondence_address(patent_data)
    attorney_names, attorney_addresses = _extract_attorney_info(patent_data)
    assignments = patent_data.get("assignmentBag", [])

    rows = []
    for a in assignments:
        assignees = [
            _safe_get_name(x)
            for x in a.get("assigneeBag", [])
            if _safe_get_name(x)
        ]

        rows.append({
            "Patent Number": patent_number,
            "Inventors": inventors,
            "Filing Date": filing,
            "Issue Date": issue,
            "Publication Date": pub,
            "Application Status": application_status,
            "Entity Status": entity_status,
            "Recorded Date": a.get("assignmentRecordedDate"),
            "Conveyance": a.get("conveyanceText"),
            "Assignees": "; ".join(assignees),
            "Reel/Frame": a.get("reelAndFrameNumber"),
            "Correspondent Address": _extract_assignment_correspondence_address(a) or wrapper_correspondence,
            "Attorney Name": attorney_names,
            "Attorney Address": attorney_addresses,
        })

    return pd.DataFrame(rows)


def fetch_assignments_from_uspto_assignment_api(patent_number: str, application_number: Optional[str] = None, delay: float = 0.0, debug: bool = False) -> pd.DataFrame:
    """Fetch assignment records from the USPTO Assignment API using applicationNumberText.

    If application_number is not provided, resolve it from the USPTO ODP wrapper for the given patent_number.
    Returns a DataFrame of rows similar to `process_patent_assignments` or a single-row DataFrame with a Note on failure.
    """
    if not application_number:
        wrapper, _ = _find_odp_wrapper(patent_number, api_key=ODP_API_KEY, debug=debug)
        if wrapper:
            application_number = str(wrapper.get("applicationNumberText") or "").strip()
            if not application_number:
                am = wrapper.get("applicationMetaData", {}) or {}
                application_number = str(am.get("applicationConfirmationNumber") or am.get("applicationNumberText") or "").strip()
        if not application_number:
            if debug:
                print(f"🔎 No ODP wrapper/application number found for patent {patent_number}")
            return pd.DataFrame([{"Patent Number": patent_number, "Note": "No application number found for assignment API"}])

    if not application_number:
        return pd.DataFrame([{"Patent Number": patent_number, "Note": "No application number found for assignment API"}])

    # Call USPTO Assignment endpoint for the application number
    app_text = urllib.parse.quote(application_number, safe="")
    url = f"https://api.uspto.gov/api/v1/patent/applications/{app_text}/assignment"

    max_retries = 3
    attempt = 0
    r = None
    while attempt < max_retries:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            break
        if r.status_code == 429:
            retry_after = r.headers.get('Retry-After')
            sleep_for = 5
            if retry_after:
                try:
                    sleep_for = int(retry_after)
                except Exception:
                    sleep_for = 5
            if debug:
                print(f"⚠️  USPTO Assignment API 429: sleeping for {sleep_for}s before retry (patent {patent_number})")
            time.sleep(sleep_for)
            attempt += 1
            continue
        if 500 <= r.status_code < 600:
            backoff = 2 ** attempt
            if debug:
                print(f"⚠️  USPTO Assignment API server error {r.status_code}, retrying in {backoff}s")
            time.sleep(backoff)
            attempt += 1
            continue
        # other client errors -> give up
        if debug:
            print(f"❌ USPTO Assignment API error {r.status_code}: {r.text} (app {application_number})")
        return pd.DataFrame([{"Patent Number": patent_number, "Note": f"Assignment API error {r.status_code}"}])

    if r is None or r.status_code != 200:
        return pd.DataFrame([{"Patent Number": patent_number, "Note": "Assignment request failed after retries"}])

    data = r.json()

    # Default enrichment: fetch /meta-data for status/entity/attorney/correspondence context.
    meta_wrapper = _fetch_application_metadata(application_number, delay=delay, debug=debug)
    meta_am = meta_wrapper.get("applicationMetaData", {}) if isinstance(meta_wrapper, dict) else {}
    meta_inventors, meta_filing, meta_issue, meta_pub = extract_inventors_and_date(meta_wrapper) if meta_wrapper else ("", "", "", "")
    meta_application_status, meta_entity_status = _extract_application_and_entity_status(meta_am) if isinstance(meta_am, dict) else ("", "")
    meta_wrapper_correspondence = _extract_wrapper_correspondence_address(meta_wrapper) if meta_wrapper else ""
    meta_attorney_names, meta_attorney_addresses = _extract_attorney_info(meta_wrapper) if meta_wrapper else ("", "")

    # Detect assignment list in response; check common keys
    candidates = []
    for k in ("assignmentBag", "assignments", "patentAssignmentDataBag", "data", "assignment"):
        v = data.get(k)
        if isinstance(v, list) and v:
            candidates = v
            break

    # If not found under those keys, try to find any list value in the response
    if not candidates:
        for v in data.values():
            if isinstance(v, list) and v:
                candidates = v
                break

    if not candidates:
        return pd.DataFrame([{"Patent Number": patent_number, "Note": "No assignments found in Assignment API response"}])

    rows = []
    for a in candidates:
        # try to extract assignees
        assignees = []
        for bkey in ("assigneeBag", "assignees", "assignee"):
            bag = a.get(bkey)
            if isinstance(bag, list):
                for item in bag:
                    name = _safe_get_name(item)
                    if name:
                        assignees.append(name)
                if assignees:
                    break

        recorded = a.get("assignmentRecordedDate") or a.get("recordedDate") or a.get("recordingDate")
        convey = a.get("conveyanceText") or a.get("conveyance")
        reelframe = a.get("reelAndFrameNumber") or a.get("reel")

        rows.append({
            "Patent Number": patent_number,
            "Inventors": meta_inventors,
            "Filing Date": meta_filing,
            "Issue Date": meta_issue,
            "Publication Date": meta_pub,
            "Application Status": meta_application_status,
            "Entity Status": meta_entity_status,
            "Recorded Date": recorded,
            "Conveyance": convey,
            "Assignees": "; ".join(assignees),
            "Reel/Frame": reelframe,
            "Correspondent Address": _extract_assignment_correspondence_address(a) or meta_wrapper_correspondence,
            "Attorney Name": meta_attorney_names,
            "Attorney Address": meta_attorney_addresses,
        })

    # optional delay
    if delay and delay > 0:
        time.sleep(delay)

    return pd.DataFrame(rows)


def fetch_assignments_from_odp(patent_number: str, api_key: Optional[str] = None, delay: float = 0.0, debug: bool = False) -> pd.DataFrame:
    """Compatibility fallback that now delegates to the ODP assignment pipeline."""
    return process_patent_assignments(patent_number, delay=delay)


# -------------------- INPUT LOADERS --------------------

def load_patent_numbers_from_args(inputs: List[str]) -> List[str]:
    if len(inputs) == 1 and os.path.isfile(inputs[0]):
        with open(inputs[0]) as f:
            return [l.strip() for l in f if l.strip()]
    return [x for x in " ".join(inputs).replace(",", " ").split() if x]


def load_assignees_from_args(inputs: List[str]) -> List[str]:
    if len(inputs) == 1 and os.path.isfile(inputs[0]):
        with open(inputs[0]) as f:
            return [l.strip() for l in f if l.strip()]
    return [x.strip() for x in " ".join(inputs).split(",") if x.strip()]


# -------------------- ODP ASSIGNEE SEARCH --------------------


def get_assignee_ids(assignee_name: str, api_key: Optional[str] = None, debug: bool = False) -> List[str]:
    """Compatibility helper: return patent numbers that match an assignee name in ODP."""
    api_key = _get_odp_api_key(api_key)
    if not api_key:
        return []

    query = f'assignmentBag.assigneeBag.assigneeNameText:"{assignee_name}"'
    patent_numbers: List[str] = []
    seen = set()
    offset = 0
    while offset < 100:
        r = _odp_wrapper_search(query, api_key=api_key, offset=offset, limit=100, debug=debug)
        if r is None or r.status_code != 200:
            break
        data = r.json()
        bags = data.get("patentFileWrapperDataBag", []) or []
        if not bags:
            break
        for wrapper in bags:
            if not isinstance(wrapper, dict):
                continue
            am = wrapper.get("applicationMetaData", {}) or {}
            patent_number = str(am.get("patentNumber") or wrapper.get("applicationNumberText") or "").strip()
            if patent_number and patent_number not in seen:
                seen.add(patent_number)
                patent_numbers.append(patent_number)
        if len(bags) < 100:
            break
        offset += 100

    if debug:
        print(f"🔎 ODP assignee lookup found {len(patent_numbers)} patent numbers for '{assignee_name}'")
    return patent_numbers


def search_patents_by_assignee(assignee_name: str, api_key: Optional[str] = None, per_page: int = 100, max_pages: int = 10, debug: bool = False, duplicate_threshold: int = 3) -> List[str]:
    """Search patents for an assignee using the USPTO ODP wrapper search endpoint."""
    api_key = _get_odp_api_key(api_key)
    if not api_key:
        print("Error: USPTO API key not provided. Set USPTO_API_KEY in your environment or pass --odp-key.")
        sys.exit(2)

    all_patents = set()
    page_bar = tqdm(total=max_pages * 2, desc=f"ODP pages for {assignee_name}", unit="page") if tqdm else None
    queries = [
        f'assignmentBag.assigneeBag.assigneeNameText:"{assignee_name}"',
        f'applicationMetaData.firstApplicantName:"{assignee_name}"',
    ]

    for query in queries:
        offset = 0
        page = 0
        while page < max_pages:
            r = _odp_wrapper_search(query, api_key=api_key, offset=offset, limit=per_page, debug=debug)
            if r is None or r.status_code != 200:
                break
            data = r.json()
            bags = data.get("patentFileWrapperDataBag", []) or []
            if not bags:
                break

            added_this_page = 0
            for wrapper in bags:
                if not isinstance(wrapper, dict):
                    continue
                am = wrapper.get("applicationMetaData", {}) or {}
                patent_number = str(am.get("patentNumber") or wrapper.get("applicationNumberText") or "").strip()
                if patent_number and patent_number not in all_patents:
                    all_patents.add(patent_number)
                    added_this_page += 1

            if debug:
                print(f"🔎 ODP assignee page {page + 1} for '{assignee_name}' returned {len(bags)} records, added {added_this_page}")

            if page_bar:
                page_bar.update(1)

            if len(bags) < per_page:
                break
            offset += per_page
            page += 1

    if page_bar:
        page_bar.close()
    print(f"✅ Found {len(all_patents)} patents for assignee '{assignee_name}'")
    return sorted(all_patents)

# -------------------- PATENT TEXT EXTRACTION --------------------

def _pv_post_with_retry(url: str, headers: dict, body: dict, max_retries: int = 3, debug: bool = False):
    """POST to ODP with retry/backoff for 429 and 5xx."""
    attempt = 0
    r = None
    while attempt < max_retries:
        r = requests.post(url, headers=headers, json=body)
        if r.status_code == 200:
            return r
        if r.status_code == 429:
            sleep_for = 5
            try:
                detail = r.json().get("detail", "")
                m = re.search(r"(\d+)\s*seconds?", detail)
                if m:
                    sleep_for = int(m.group(1)) + 1
            except Exception:
                pass
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = int(retry_after)
                except Exception:
                    pass
            if debug:
                print(f"⚠️  ODP 429: sleeping {sleep_for}s before retry")
            time.sleep(sleep_for)
            attempt += 1
            continue
        if 500 <= r.status_code < 600:
            backoff = 2 ** attempt
            if debug:
                print(f"⚠️  ODP {r.status_code}: retrying in {backoff}s")
            time.sleep(backoff)
            attempt += 1
            continue
        if debug:
            print(f"❌ ODP {r.status_code}: {r.text}")
        return r
    return r


# Hardcoded WIPO 35-field IPC technology concordance table.
# wipo_id (trailing path segment from legacy URL references) → human-readable text.
_WIPO_TABLE: Dict[str, str] = {
    "1":  "Electrical Engineering — Electrical machinery, apparatus, energy",
    "2":  "Electrical Engineering — Audio-visual technology",
    "3":  "Electrical Engineering — Telecommunications",
    "4":  "Electrical Engineering — Digital communication",
    "5":  "Electrical Engineering — Basic communication processes",
    "6":  "Electrical Engineering — Computer technology",
    "7":  "Electrical Engineering — IT methods for management",
    "8":  "Electrical Engineering — Semiconductors",
    "9":  "Instruments — Optics",
    "10": "Instruments — Measurement",
    "11": "Instruments — Analysis of biological materials",
    "12": "Instruments — Control",
    "13": "Instruments — Medical technology",
    "14": "Chemistry — Organic fine chemistry",
    "15": "Chemistry — Biotechnology",
    "16": "Chemistry — Pharmaceuticals",
    "17": "Chemistry — Macromolecular chemistry, polymers",
    "18": "Chemistry — Food chemistry",
    "19": "Chemistry — Basic materials chemistry",
    "20": "Chemistry — Materials, metallurgy",
    "21": "Chemistry — Surface technology, coating",
    "22": "Chemistry — Micro-structural and nano-technology",
    "23": "Chemistry — Chemical engineering",
    "24": "Chemistry — Environmental technology",
    "25": "Mechanical Engineering — Handling",
    "26": "Mechanical Engineering — Machine tools",
    "27": "Mechanical Engineering — Engines, pumps, turbines",
    "28": "Mechanical Engineering — Thermal processes and apparatus",
    "29": "Mechanical Engineering — Mechanical elements",
    "30": "Mechanical Engineering — Transport",
    "31": "Other Fields — Furniture, games",
    "32": "Other Fields — Other consumer goods",
    "33": "Other Fields — Civil engineering",
    "34": "Other Fields — Other special machines",
    "35": "Other Fields — Agriculture, food processing",
}


def _extract_wipo_text(wipo_list: list) -> str:
    """Extract a human-readable technology field from a patent's legacy wipo sub-array.

    Legacy service returned wipo_field values as URL references (strip trailing ID)
    and look it up in the local concordance table.
    Sort by wipo_sequence; join multiple distinct fields with ' | '.
    """
    if not wipo_list:
        return ""
    try:
        wipo_list = sorted(wipo_list, key=lambda x: x.get("wipo_sequence", 99) if isinstance(x, dict) else 99)
    except Exception:
        pass
    seen: List[str] = []
    for w in wipo_list:
        if not isinstance(w, dict):
            continue
        # wipo_field is a URL ref like '.../wipo/10/' — strip trailing ID
        raw = (w.get("wipo_field") or w.get("wipo_field_id") or "").strip().rstrip("/")
        if not raw:
            continue
        wid = raw.rsplit("/", 1)[-1] if raw.startswith("http") else raw
        label = _WIPO_TABLE.get(wid, f"WIPO field {wid}")
        if label not in seen:
            seen.append(label)
    return " | ".join(seen)


def _cpc_group_id_to_code(raw: str) -> str:
    """Convert a CPC group identifier to standard CPC notation.

    Some legacy data sources store CPC group identifiers with ':' instead of '/', e.g.
    'G01S7:4863'. Convert to 'G01S7/4863'.  If the value happens to be a
    URL reference (fallback safety), strip to the path segment first.
    """
    v = raw.strip().rstrip("/")
    if v.startswith("http"):
        v = v.rsplit("/", 1)[-1]
    return v.replace(":", "/")


def _normalize_claim_text(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "")).strip()


def _normalize_claim_dependent(raw: Any) -> str:
    if isinstance(raw, bool):
        return "Yes" if raw else "No"
    value = str(raw or "").strip().lower()
    if value in {"1", "true", "t", "yes", "y"}:
        return "Yes"
    if value in {"0", "false", "f", "no", "n"}:
        return "No"
    return str(raw or "").strip()


def _fetch_claims_batch(
    doc_ids: List[str],
    api_key: str,
    endpoint_url: str,
    id_field: str,
    response_key: str,
    desc: str,
    unit: str,
    batch_size: int = 10,
    delay: float = 0.2,
    debug: bool = False,
) -> Tuple[pd.DataFrame, List[str]]:
    headers = {"X-Api-Key": api_key, "Accept": "application/json"}
    fields = [id_field, "claim_sequence", "claim_number", "claim_text", "claim_dependent"]

    rows = []
    endpoint_errors: List[str] = []
    batches = [doc_ids[i: i + batch_size] for i in range(0, len(doc_ids), batch_size)]
    total_batches = len(batches)
    bar = tqdm(total=len(doc_ids), desc=desc, unit=unit) if tqdm else None

    for b_idx, batch in enumerate(batches, start=1):
        page_size = max(1000, min(10000, len(batch) * 250))
        if len(batch) == 1:
            q_clause: Dict[str, Any] = {id_field: batch[0]}
        else:
            q_clause = {"_or": [{id_field: doc_id} for doc_id in batch]}

        body = {
            "q": q_clause,
            "f": fields,
            "o": {
                "size": page_size,
            },
            "s": [{id_field: "asc"}, {"claim_sequence": "asc"}],
        }
        if id_field == "patent_id":
            body["o"]["pad_patent_id"] = False

        if debug:
            print(f"  [{desc.lower()}] batch {b_idx}/{total_batches} ({len(batch)} ids)")

        r = _pv_post_with_retry(endpoint_url, headers, body, debug=debug)
        if r is None or r.status_code != 200:
            endpoint_errors.append(f"{desc}: API error {getattr(r, 'status_code', 'N/A')}")
            if debug:
                print(f"  [debug] claims batch failed with status {getattr(r, 'status_code', 'N/A')}")
            if bar:
                bar.update(len(batch))
            if delay and delay > 0:
                time.sleep(delay)
            continue

        data = r.json()
        if data.get("error"):
            msg = str(data.get("error")).strip()
            if msg:
                endpoint_errors.append(f"{desc}: {msg}")
                if debug:
                    print(f"  [debug] {desc} API message: {msg}")
        items = data.get(response_key, []) or []
        if not items:
            # Be tolerant to key shape changes in API responses.
            for k in ("g_claims", "pg_claims", "claims", "g_claim", "pg_claim"):
                alt = data.get(k)
                if isinstance(alt, list) and alt:
                    items = alt
                    break
        if not items:
            # Last fallback: first list of dict rows that looks claim-like.
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict) and ("claim_text" in v[0] or "claim_sequence" in v[0]):
                    items = v
                    break
        if debug and len(items) >= page_size:
            print(f"  [debug] {desc} may be truncated for batch {b_idx}: received {len(items)} rows at size limit {page_size}")
        if debug and not items:
            print(f"  [debug] {desc} returned 0 rows for batch {b_idx}; top-level keys: {list(data.keys())}")

        for item in items:
            patent_number = (item.get(id_field) or "").strip()
            claim_text = _normalize_claim_text(item.get("claim_text"))
            if not patent_number or not claim_text:
                continue
            rows.append({
                "Patent Number": patent_number,
                "Claim Number": str(item.get("claim_number") or "").strip(),
                "Claim Sequence": str(item.get("claim_sequence") or "").strip(),
                "Claim Text": claim_text,
                "Is Dependent": _normalize_claim_dependent(item.get("claim_dependent")),
            })

        if bar:
            bar.update(len(batch))
        if delay and delay > 0:
            time.sleep(delay)

    if bar:
        bar.close()
    # De-duplicate while preserving order.
    seen = set()
    unique_errors: List[str] = []
    for e in endpoint_errors:
        if e not in seen:
            seen.add(e)
            unique_errors.append(e)
    return pd.DataFrame(rows), unique_errors


def fetch_granted_claims_batch(
    patent_ids: List[str],
    api_key: str,
    batch_size: int = 10,
    delay: float = 0.2,
    debug: bool = False,
) -> Tuple[pd.DataFrame, List[str]]:
    rows = []
    errors: List[str] = []
    bar = tqdm(total=len(patent_ids), desc="Fetching granted claims", unit="patent") if tqdm else None
    for pid in patent_ids:
        try:
            row = _build_odp_text_row(pid, api_key, debug=debug)
            claims = json.loads(row.get("_claims_json") or "[]")
            if not claims:
                errors.append(f"Granted claims: no claims found for {pid}")
            for claim in claims:
                text = str(claim.get("claim_text") or "").strip()
                if not text:
                    continue
                rows.append({
                    "Patent Number": pid,
                    "Claim Number": str(claim.get("claim_number") or "").strip(),
                    "Claim Sequence": str(claim.get("claim_sequence") or "").strip(),
                    "Claim Text": text,
                    "Is Dependent": str(claim.get("claim_dependent") or "").strip() or "No",
                })
        except Exception as e:
            errors.append(f"Granted claims: {pid}: {e}")
        if bar:
            bar.update(1)
        if delay and delay > 0:
            time.sleep(delay)
    if bar:
        bar.close()
    return pd.DataFrame(rows), errors


def fetch_publication_claims_batch(
    doc_numbers: List[str],
    api_key: str,
    batch_size: int = 10,
    delay: float = 0.2,
    debug: bool = False,
) -> Tuple[pd.DataFrame, List[str]]:
    rows = []
    errors: List[str] = []
    bar = tqdm(total=len(doc_numbers), desc="Fetching publication claims", unit="pub") if tqdm else None
    for dn in doc_numbers:
        try:
            row = _build_odp_text_row(dn, api_key, debug=debug)
            claims = json.loads(row.get("_claims_json") or "[]")
            if not claims:
                errors.append(f"Publication claims: no claims found for {dn}")
            for claim in claims:
                text = str(claim.get("claim_text") or "").strip()
                if not text:
                    continue
                rows.append({
                    "Patent Number": dn,
                    "Claim Number": str(claim.get("claim_number") or "").strip(),
                    "Claim Sequence": str(claim.get("claim_sequence") or "").strip(),
                    "Claim Text": text,
                    "Is Dependent": str(claim.get("claim_dependent") or "").strip() or "No",
                })
        except Exception as e:
            errors.append(f"Publication claims: {dn}: {e}")
        if bar:
            bar.update(1)
        if delay and delay > 0:
            time.sleep(delay)
    if bar:
        bar.close()
    return pd.DataFrame(rows), errors


def _build_odp_text_row(identifier: str, api_key: str, debug: bool = False) -> Dict[str, Any]:
    wrapper, _ = _find_odp_wrapper(identifier, api_key=api_key, debug=debug)
    if not wrapper:
        return {
            "Patent Number": identifier,
            "Patent Title": "",
            "Abstract": "",
            "Technology Field": "",
            "CPC Primary": "",
            "Note": "Not found in ODP",
            "_claims_json": "[]",
        }

    am = wrapper.get("applicationMetaData", {}) or {}
    xml_url = (
        (wrapper.get("grantDocumentMetaData") or {}).get("fileLocationURI")
        or (wrapper.get("pgpubDocumentMetaData") or {}).get("fileLocationURI")
    )
    xml_text = _fetch_odp_document_xml(xml_url, api_key=api_key, debug=debug) if xml_url else ""
    row = _wrapper_to_text_row(identifier, wrapper, xml_text)
    if not row.get("Patent Title"):
        row["Patent Title"] = str(am.get("inventionTitle") or am.get("publicationTitle") or "").strip()
    if not row.get("Abstract"):
        row["Abstract"] = str(am.get("abstract") or am.get("inventionAbstract") or am.get("publicationAbstract") or "").strip()
    row["Note"] = ""
    row["Source"] = "USPTO ODP"
    return row


def _build_claim_summary(claims_df: pd.DataFrame) -> pd.DataFrame:
    if claims_df.empty:
        return pd.DataFrame(columns=["Patent Number", "Claim Count", "Claim 1"])

    sortable = claims_df.copy()
    sortable["_claim_number_sort"] = pd.to_numeric(sortable["Claim Number"], errors="coerce").fillna(10**9)
    sortable["_claim_sequence_sort"] = pd.to_numeric(sortable["Claim Sequence"], errors="coerce").fillna(10**9)
    sortable = sortable.sort_values(
        ["Patent Number", "_claim_number_sort", "_claim_sequence_sort", "Claim Number", "Claim Sequence"]
    )

    counts = sortable.groupby("Patent Number").size().rename("Claim Count").reset_index()
    claim_1 = sortable.groupby("Patent Number", sort=False).first().reset_index()[["Patent Number", "Claim Text"]]
    claim_1 = claim_1.rename(columns={"Claim Text": "Claim 1"})
    return counts.merge(claim_1, on="Patent Number", how="left")


def _html_to_text(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", fragment)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_google_patent_claims_batch(
    patent_ids: List[str],
    delay: float = 0.2,
    debug: bool = False,
) -> pd.DataFrame:
    """Fallback claims fetch from Google Patents HTML pages for granted patents."""
    rows: List[Dict[str, str]] = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    iterator = tqdm(patent_ids, desc="Fallback claims (Google Patents)", unit="patent") if tqdm else patent_ids
    for pid in iterator:
        url = f"https://patents.google.com/patent/US{pid}/en"
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                if debug:
                    print(f"  [debug] Google claims fetch failed for {pid}: HTTP {r.status_code}")
                if delay and delay > 0:
                    time.sleep(delay)
                continue
            page = r.text
            m = re.search(r"<section itemprop=\"claims\"[^>]*>(.*?)</section>", page, flags=re.DOTALL | re.IGNORECASE)
            if not m:
                if debug:
                    print(f"  [debug] Google claims section missing for {pid}")
                if delay and delay > 0:
                    time.sleep(delay)
                continue

            claims_section = m.group(1)
            claim_starts = list(re.finditer(r"<div id=\"CLM-[^\"]+\"\s+num=\"(?P<num>\d+)\"\s+class=\"claim\">", claims_section))
            if not claim_starts:
                if debug:
                    print(f"  [debug] Google claim blocks missing for {pid}")
                if delay and delay > 0:
                    time.sleep(delay)
                continue

            for idx, start in enumerate(claim_starts):
                block_start = start.start()
                block_end = claim_starts[idx + 1].start() if idx + 1 < len(claim_starts) else len(claims_section)
                block = claims_section[block_start:block_end]
                # Heuristic: the wrapper just before the claim block includes class claim-dependent for dependent claims.
                pre = claims_section[max(0, block_start - 180):block_start]
                is_dependent = "Yes" if "claim-dependent" in pre else "No"

                text_parts = re.findall(r"<div class=\"claim-text\">(.*?)</div>", block, flags=re.DOTALL | re.IGNORECASE)
                clean_parts = [_html_to_text(t) for t in text_parts if _html_to_text(t)]
                if not clean_parts:
                    continue

                claim_text = " ".join(clean_parts).strip()
                claim_num_raw = start.group("num")
                claim_num = str(int(claim_num_raw)) if claim_num_raw.isdigit() else claim_num_raw
                rows.append({
                    "Patent Number": pid,
                    "Claim Number": claim_num,
                    "Claim Sequence": claim_num,
                    "Claim Text": claim_text,
                    "Is Dependent": is_dependent,
                })

            if delay and delay > 0:
                time.sleep(delay)
        except Exception as e:
            if debug:
                print(f"  [debug] Google claims exception for {pid}: {e}")
            if delay and delay > 0:
                time.sleep(delay)

    return pd.DataFrame(rows)


def fetch_patent_text_batch(
    patent_ids: List[str],
    api_key: str,
    batch_size: int = 100,
    delay: float = 0.2,
    debug: bool = False,
) -> pd.DataFrame:
    rows = []
    bar = tqdm(total=len(patent_ids), desc="Fetching patent text", unit="patent") if tqdm else None
    for pid in patent_ids:
        row = _build_odp_text_row(pid, api_key, debug=debug)
        row.pop("_claims_json", None)
        rows.append(row)
        if bar:
            bar.update(1)
        if delay and delay > 0:
            time.sleep(delay)

    if bar:
        bar.close()
    return pd.DataFrame(rows)


def fetch_publication_text_batch(
    doc_numbers: List[str],
    api_key: str,
    batch_size: int = 100,
    delay: float = 0.2,
    debug: bool = False,
) -> pd.DataFrame:
    rows = []
    bar = tqdm(total=len(doc_numbers), desc="Fetching publication text", unit="pub") if tqdm else None
    for dn in doc_numbers:
        row = _build_odp_text_row(dn, api_key, debug=debug)
        row["Claim Count"] = 0
        row.pop("_claims_json", None)
        rows.append(row)
        if bar:
            bar.update(1)
        if delay and delay > 0:
            time.sleep(delay)

    if bar:
        bar.close()
    return pd.DataFrame(rows)


def run_patent_text_extraction(
    patent_numbers: List[str],
    odp_key: Optional[str],
    delay: float = 0.2,
    debug: bool = False,
    out_prefix: str = "patent_text",
    claims_source: str = "auto",
) -> None:
    """Fetch patent text summary plus detailed claims and write timestamped files.

    Patents not found in the granted-patent endpoint are retried against the
    pre-grant publication endpoint (useful when patent_numbers includes
    publication doc numbers like 20230XXXXXX).
    """
    api_key = odp_key or ODP_API_KEY
    if not api_key:
        print("Error: USPTO API key required for patent text extraction. "
              "Set USPTO_API_KEY or pass --odp-key.")
        sys.exit(2)

    print(f"\n📄 Patent text extraction: {len(patent_numbers)} unique patent(s)")

    # First pass: try all as granted patents
    df = fetch_patent_text_batch(patent_numbers, api_key, delay=delay, debug=debug)

    # Identify any that were not found in the granted patent endpoint
    not_found_mask = df["Note"].str.contains("pre-grant|Not found", na=False)
    not_found_ids = df.loc[not_found_mask, "Patent Number"].tolist()

    if not_found_ids:
        print(f"  {len(not_found_ids)} patent(s) not found as granted — retrying as pre-grant publications...")
        pub_df = fetch_publication_text_batch(not_found_ids, api_key, delay=delay, debug=debug)

        # Merge: replace the not-found rows with publication results
        df = df[~not_found_mask].copy()
        df = pd.concat([df, pub_df], ignore_index=True)

    granted_ids = sorted(set(patent_numbers) - set(not_found_ids))
    claim_frames = []
    claim_errors: List[str] = []

    use_odp_claims = claims_source in ("auto", "odp")
    use_google_fallback = claims_source in ("auto", "google")

    if use_odp_claims and granted_ids:
        if claims_source == "auto" and len(granted_ids) > 30:
            probe_ids = granted_ids[:30]
            probe_claims, probe_errors = fetch_granted_claims_batch(probe_ids, api_key, delay=delay, debug=debug)
            claim_errors.extend(probe_errors)
            probe_coverage = (len(set(probe_claims["Patent Number"])) / len(probe_ids)) if not probe_claims.empty else 0.0
            if probe_coverage < 0.1:
                print(
                    f"  ODP claims probe coverage {probe_coverage:.0%} on first {len(probe_ids)} patents; "
                    "skipping remaining ODP claims and using Google fallback."
                )
                use_odp_claims = False
            else:
                if not probe_claims.empty:
                    claim_frames.append(probe_claims)
                remaining_ids = granted_ids[len(probe_ids):]
                if remaining_ids:
                    remaining_claims, remaining_errors = fetch_granted_claims_batch(remaining_ids, api_key, delay=delay, debug=debug)
                    claim_errors.extend(remaining_errors)
                    if not remaining_claims.empty:
                        claim_frames.append(remaining_claims)
        else:
            granted_claims, granted_errors = fetch_granted_claims_batch(granted_ids, api_key, delay=delay, debug=debug)
            claim_errors.extend(granted_errors)
            if not granted_claims.empty:
                claim_frames.append(granted_claims)

    if use_odp_claims and not_found_ids:
        publication_claims, publication_errors = fetch_publication_claims_batch(not_found_ids, api_key, delay=delay, debug=debug)
        claim_errors.extend(publication_errors)
        if not publication_claims.empty:
            claim_frames.append(publication_claims)

    claims_df = pd.concat(claim_frames, ignore_index=True) if claim_frames else pd.DataFrame(
        columns=["Patent Number", "Claim Number", "Claim Sequence", "Claim Text", "Is Dependent"]
    )

    # Fallback source: Google Patents HTML claims for granted patents when
    # ODP XML parsing returns no rows or partial coverage.
    if use_google_fallback:
        covered_ids = set(claims_df["Patent Number"].astype(str).tolist()) if not claims_df.empty else set()
        missing_granted_ids = [pid for pid in granted_ids if pid not in covered_ids]
        if missing_granted_ids:
            source_note = "ODP" if use_odp_claims else "selected source"
            print(f"  {len(missing_granted_ids)} granted patent(s) missing claims from {source_note} — trying Google Patents fallback...")
            google_claims_df = fetch_google_patent_claims_batch(missing_granted_ids, delay=delay, debug=debug)
            if not google_claims_df.empty:
                print(f"  ✓ Google Patents fallback returned {len(google_claims_df)} claim row(s)")
                claims_df = pd.concat([claims_df, google_claims_df], ignore_index=True) if not claims_df.empty else google_claims_df
            else:
                claim_errors.append("Google Patents fallback returned no claim rows")

    if not claims_df.empty:
        claim_summary = _build_claim_summary(claims_df)
        claim_summary = claim_summary.rename(columns={"Claim Count": "Claim Count (Detailed)"})
        df = df.merge(claim_summary, on="Patent Number", how="left")
        df["Claim Count"] = df["Claim Count (Detailed)"].fillna(0).astype(int)
        df = df.drop(columns=["Claim Count (Detailed)"])
        df["Claim 1"] = df["Claim 1"].fillna("")
        claims_df["_claim_number_sort"] = pd.to_numeric(claims_df["Claim Number"], errors="coerce").fillna(10**9)
        claims_df["_claim_sequence_sort"] = pd.to_numeric(claims_df["Claim Sequence"], errors="coerce").fillna(10**9)
        claims_df = claims_df.sort_values(
            ["Patent Number", "_claim_number_sort", "_claim_sequence_sort", "Claim Number", "Claim Sequence"]
        ).drop(columns=["_claim_number_sort", "_claim_sequence_sort"]).reset_index(drop=True)
    else:
        df["Claim Count"] = ""
        df["Claim 1"] = ""

    # Sort by Patent Number for clean output
    df = df.sort_values("Patent Number").reset_index(drop=True)

    # Drop the Note column if it's entirely empty (clean run)
    if df["Note"].str.strip().eq("").all():
        df = df.drop(columns=["Note"])

    # Write Excel output
    import datetime
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    text_out_dir = "patent_text_results"
    claims_out_dir = "patent_claims_results"
    os.makedirs(text_out_dir, exist_ok=True)
    xlsx_path = os.path.join(text_out_dir, f"{out_prefix}_{ts}.xlsx")

    try:
        df.to_excel(xlsx_path, index=False, engine="openpyxl")
        print(f"✅ Saved patent text results: {xlsx_path}  ({len(df)} patents)")
    except ImportError:
        csv_path = xlsx_path.replace(".xlsx", ".csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Saved patent text results (CSV fallback): {csv_path}  ({len(df)} patents)")

    if claims_df.empty:
        if claim_errors:
            print("⚠️  Claim text endpoint returned no rows. API message(s):")
            for msg in claim_errors[:3]:
                print(f"   - {msg}")
            if len(claim_errors) > 3:
                print(f"   - ...and {len(claim_errors) - 3} more")
        print("ℹ️  No claim rows were returned from ODP. The export was saved without detailed claims and Claim Count was left blank.")
        return

    os.makedirs(claims_out_dir, exist_ok=True)
    claims_xlsx_path = os.path.join(claims_out_dir, f"patent_claims_{ts}.xlsx")
    try:
        claims_df.to_excel(claims_xlsx_path, index=False, engine="openpyxl")
        print(f"✅ Saved patent claims results: {claims_xlsx_path}  ({len(claims_df)} claims)")
    except ImportError:
        claims_csv_path = claims_xlsx_path.replace(".xlsx", ".csv")
        claims_df.to_csv(claims_csv_path, index=False)
        print(f"✅ Saved patent claims results (CSV fallback): {claims_csv_path}  ({len(claims_df)} claims)")


# -------------------- MAIN --------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputtype", choices=["bypatentnumber", "byassignee"])
    parser.add_argument("inputs", nargs="*")
    parser.add_argument("-o", "--out", default="all_assignments")
    parser.add_argument("--odp-key", dest="odp_key", default=None, help="USPTO ODP X-Api-Key (overrides USPTO_API_KEY env var)")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay in seconds between USPTO requests (helps avoid rate limits). Default: 0.2s")
    parser.add_argument("--per-page", type=int, default=100, help="Number of patents to request per page from ODP (default: 100)")
    parser.add_argument("--max-pages", type=int, default=10, help="Maximum number of pages to request (default: 10)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output for ODP pagination")
    parser.add_argument(
        "--claims-source",
        choices=["auto", "odp", "google"],
        default="auto",
        help=(
            "Claims source strategy for --text mode: "
            "auto (probe ODP and skip when coverage is low), "
            "odp (ODP only), or google (Google Patents only)."
        ),
    )
    parser.add_argument(
        "--text",
        action="store_true",
        help=(
            "Extract patent title, abstract, WIPO field, CPC primary, and claim summary per patent, "
            "plus a separate detailed patent_claims export. Uses USPTO ODP. "
            "When combined with byassignee mode, the distinct set of patents across "
            "all assignees is used. Skips the USPTO assignment pipeline."
        ),
    )
    args = parser.parse_args()

    if args.inputtype == "bypatentnumber":
        patent_numbers = load_patent_numbers_from_args(args.inputs)
    else:
        assignees = load_assignees_from_args(args.inputs)
        patent_numbers = []
        assignee_iter = tqdm(assignees, desc="Searching assignees", unit="assignee") if tqdm else assignees
        for a in assignee_iter:
            patent_numbers.extend(search_patents_by_assignee(a, api_key=args.odp_key, per_page=args.per_page, max_pages=args.max_pages, debug=args.debug))

    patent_numbers = sorted(set(patent_numbers))

    # --text mode: extract summary text fields plus claim data then exit
    if args.text:
        if not patent_numbers:
            print("No patent numbers found.")
            sys.exit(1)
        run_patent_text_extraction(
            patent_numbers,
            odp_key=args.odp_key,
            delay=args.delay,
            debug=args.debug,
            claims_source=args.claims_source,
        )
        return
    if not patent_numbers:
        print("No patent numbers found.")
        sys.exit(1)

    all_rows = []
    failed_patents = []

    # Primary processing loop with optional tqdm progress bar
    if tqdm:
        for pn in tqdm(patent_numbers, desc="Processing patents", unit="patent"):
            df = process_patent_assignments(pn, delay=args.delay)
            if df.empty:
                continue
            if "Note" in df.columns and df.shape[0] == 1 and pd.notna(df.iloc[0].get("Note", None)):
                failed_patents.append(pn)
            else:
                all_rows.append(df)
    else:
        total = len(patent_numbers)
        for idx, pn in enumerate(patent_numbers, start=1):
            print(f"\rProcessing {idx}/{total}: {pn}", end="", flush=True)
            df = process_patent_assignments(pn, delay=args.delay)
            if df.empty:
                continue
            if "Note" in df.columns and df.shape[0] == 1 and pd.notna(df.iloc[0].get("Note", None)):
                failed_patents.append(pn)
            else:
                all_rows.append(df)

    # ensure progress line ends
    print()

    # If any patents failed, save them and retry once (pipes into process_patent_assignments again)
    if failed_patents:
        print(f"\n⚠️  {len(failed_patents)} patents returned no wrapper/assignments on first pass. Saving to not_found_patents.txt and retrying once.")
        with open("not_found_patents.txt", "w") as nf:
            for p in failed_patents:
                nf.write(p + "\n")

        # Retry failed patents once (sometimes transient 404s or rate issues)
        retried = []
        still_failed = []
        if tqdm:
            for pn in tqdm(failed_patents, desc="Retrying patents", unit="patent"):
                df = process_patent_assignments(pn, delay=args.delay)
                if df.empty:
                    still_failed.append(pn)
                    continue
                if "Note" in df.columns and df.shape[0] == 1 and pd.notna(df.iloc[0].get("Note", None)):
                    still_failed.append(pn)
                else:
                    all_rows.append(df)
                    retried.append(pn)
        else:
            retry_total = len(failed_patents)
            for r_idx, pn in enumerate(failed_patents, start=1):
                print(f"\rRetrying {r_idx}/{retry_total}: {pn}", end="", flush=True)
                df = process_patent_assignments(pn, delay=args.delay)
                if df.empty:
                    still_failed.append(pn)
                    continue
                if "Note" in df.columns and df.shape[0] == 1 and pd.notna(df.iloc[0].get("Note", None)):
                    still_failed.append(pn)
                else:
                    all_rows.append(df)
                    retried.append(pn)

        # finish retry progress line
        print()
        print(f"🔁 Retried: {len(retried)} succeeded, {len(still_failed)} still missing")
        if still_failed:
            # Attempt assignment-API fallback for still-missing patents
            print("\n🔁 Attempting USPTO Assignment API fallback for unresolved patents...")
            recovered = []
            remaining = []
            for pn in still_failed:
                # First try ODP per-patent wrapper for assignees
                pv_df = fetch_assignments_from_odp(pn, api_key=args.odp_key, delay=args.delay, debug=args.debug)
                if pv_df is not None and not pv_df.empty:
                    if not ("Note" in pv_df.columns and pv_df.shape[0] == 1 and pd.notna(pv_df.iloc[0].get("Note", None))):
                        all_rows.append(pv_df)
                        recovered.append(pn)
                        continue

                # If ODP per-patent did not yield usable rows, fall back to USPTO Assignment API
                df_assign = fetch_assignments_from_uspto_assignment_api(pn, delay=args.delay, debug=args.debug)
                if df_assign is None or df_assign.empty:
                    remaining.append(pn)
                    continue
                # treat single-row Note as failure
                if "Note" in df_assign.columns and df_assign.shape[0] == 1 and pd.notna(df_assign.iloc[0].get("Note", None)):
                    remaining.append(pn)
                    continue
                # append recovered rows
                all_rows.append(df_assign)
                recovered.append(pn)

            print(f"🔁 Assignment API recovered: {len(recovered)} patents, still missing: {len(remaining)}")
            # overwrite not_found_patents.txt with remaining unresolved patents
            with open("not_found_patents.txt", "w") as nf:
                for p in remaining:
                    nf.write(p + "\n")
            if remaining:
                print(f"Saved list of unresolved patents to not_found_patents.txt")

    if all_rows:
        out = pd.concat(all_rows, ignore_index=True)
        # Save to assignment_results/ with timestamp, like patent_text_results
        import datetime as _dt
        ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = args.out.rsplit('.', 1)[0] if '.' in args.out else args.out
        out_dir = "assignment_results"
        os.makedirs(out_dir, exist_ok=True)
        csv_file = os.path.join(out_dir, f"{base_name}_{ts}.csv")
        xlsx_file = os.path.join(out_dir, f"{base_name}_{ts}.xlsx")

        out.to_csv(csv_file, index=False)
        print(f"\n✅ Saved: {csv_file}")

        try:
            out.to_excel(xlsx_file, index=False, engine='openpyxl')
            print(f"✅ Saved: {xlsx_file}")
        except ImportError:
            print(f"⚠️  Could not save XLSX (install openpyxl: pip install openpyxl)")


if __name__ == "__main__":
    main()