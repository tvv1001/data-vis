#!/usr/bin/env python3
"""
finraScraper.py

Iterates FINRA BrokerCheck API for a list of seed names, builds a local
nodes-and-edges JSON database, and saves it to server/data/finra-graph.json.

Usage:
  python3 finraScraper.py [--seeds path/to/seeds.json] [--out path/to/output.json]

Seed file format (JSON array of name strings):
  ["John Brandon Lively", "Jennifer Lyn", "Jason Walker", "David Finch"]

Output format:
  {
    "nodes": [...],
    "links": [...],
    "meta": { "generated": "ISO timestamp", "total_individuals": N, "total_firms": N }
  }
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEARCH_URL  = "https://api.brokercheck.finra.org/search/individual"
DETAIL_URL  = "https://api.brokercheck.finra.org/search/individual/{crd}"
FIRM_URL    = "https://api.brokercheck.finra.org/search/firm/{firm_id}"
PAGE_SIZE = 12
REQUEST_DELAY = 0.8   # seconds between API calls – be polite
MAX_RESULTS_PER_QUERY = 120  # cap per seed name (10 pages)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)",
    "Accept": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict | None = None, retries: int = 3) -> dict:
    """GET with simple exponential-backoff retry."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                wait = 2 ** (attempt + 2)
                log.warning("Rate-limited. Waiting %ss…", wait)
                time.sleep(wait)
            else:
                log.error("HTTP error: %s", exc)
                raise
        except requests.RequestException as exc:
            log.warning("Request failed (attempt %d): %s", attempt + 1, exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}


def search_individuals(query: str) -> list[dict]:
    """Return all summary records for a name query (handles pagination)."""
    results: list[dict] = []
    start = 0

    while start < MAX_RESULTS_PER_QUERY:
        params = {
            "query": query,
            "includePrevious": "true",
            "hl": "false",
            "nrows": PAGE_SIZE,
            "start": start,
            "r": 25,
            "sort": "bc_lastname_sort+asc,bc_firstname_sort+asc,bc_middlename_sort+asc,score+desc",
            "wt": "json",
        }
        data = _get(SEARCH_URL, params=params)
        hits = data.get("hits", {})
        batch = hits.get("hits", [])
        total = hits.get("total", 0)

        if not batch:
            break

        results.extend(batch)
        start += PAGE_SIZE
        log.info("  page results: %d / %d", len(results), total)

        if len(results) >= total:
            break

        time.sleep(REQUEST_DELAY)

    return results


def fetch_detail(crd: str) -> dict:
    """Fetch the full detail record for a single individual CRD number."""
    data = _get(DETAIL_URL.format(crd=crd))
    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        return {}
    raw_content = hits[0].get("_source", {}).get("content", "{}")
    try:
        return json.loads(raw_content)
    except json.JSONDecodeError:
        return {}


def fetch_firm_detail(firm_id: str) -> dict:
    """Fetch the full Form BD detail for a firm, including directOwners."""
    data = _get(FIRM_URL.format(firm_id=firm_id))
    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        return {}
    raw_content = hits[0].get("_source", {}).get("content", "{}")
    try:
        return json.loads(raw_content)
    except json.JSONDecodeError:
        return {}


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------

def _person_node_id(crd: str) -> str:
    return f"person_{crd}"

def _firm_node_id(firm_id: str) -> str:
    return f"firm_{firm_id}"


def _parse_date(raw: str | None) -> str | None:
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # return as-is if unparseable


def build_person_node(crd: str, summary: dict, detail: dict) -> dict:
    basic = detail.get("basicInformation", {})
    exams_count = detail.get("examsCount", {})
    disclosures = detail.get("disclosures", [])

    first = basic.get("firstName") or summary.get("ind_firstname", "")
    middle = basic.get("middleName") or summary.get("ind_middlename", "")
    last = basic.get("lastName") or summary.get("ind_lastname", "")
    suffix = summary.get("ind_namesuffix", "")
    label_parts = [p for p in [first, middle, last, suffix] if p]
    label = " ".join(label_parts)

    other_names = (
        basic.get("otherNames")
        or summary.get("ind_other_names", [])
    )

    total_exams = (
        exams_count.get("stateExamCount", 0)
        + exams_count.get("principalExamCount", 0)
        + exams_count.get("productExamCount", 0)
    )

    exam_list = (
        [e.get("examCategory") for e in detail.get("stateExamCategory", [])]
        + [e.get("examCategory") for e in detail.get("principalExamCategory", [])]
        + [e.get("examCategory") for e in detail.get("productExamCategory", [])]
    )

    return {
        "id": _person_node_id(crd),
        "crd": crd,
        "label": label,
        "group": "individual",
        "firstName": first,
        "middleName": middle,
        "lastName": last,
        "suffix": suffix,
        "otherNames": other_names,
        "bcScope": basic.get("bcScope") or summary.get("ind_bc_scope", ""),
        "iaScope": basic.get("iaScope") or summary.get("ind_ia_scope", ""),
        "daysInIndustry": basic.get("daysInIndustry"),
        "industryCalDate": summary.get("ind_industry_cal_date"),
        "disclosureFlag": summary.get("ind_bc_disclosure_fl", "N"),
        "disclosureCount": len(disclosures),
        "disclosures": _parse_disclosures(disclosures),
        "examsCount": total_exams,
        "exams": exam_list,
        "registeredStates": [
            r.get("state") for r in detail.get("registeredStates", []) if r.get("state")
        ],
    }


def _parse_disclosures(disclosures: list[dict]) -> list[dict]:
    parsed = []
    for d in disclosures:
        parsed.append({
            "type": d.get("disclosureType"),
            "date": _parse_date(d.get("disclosureDate") or d.get("eventDate")),
            "resolution": d.get("disclosureResolution") or d.get("resolution"),
            "detail": d.get("disclosureDetail") or d.get("allegations"),
        })
    return parsed


def build_firm_node(firm_id: str, firm_name: str, extra: dict | None = None) -> dict:
    node = {
        "id": _firm_node_id(firm_id),
        "firmId": firm_id,
        "label": firm_name,
        "group": "firm",
    }
    if extra:
        node["bdSecNumber"] = extra.get("bdSECNumber")
        node["iaSecNumber"] = extra.get("iaSECNumber")
        node["bcScope"] = extra.get("firmBCScope")
        node["iaScope"] = extra.get("firmIAScope")
    return node


def enrich_firm_node_from_detail(node: dict, detail: dict) -> None:
    """Mutate a firm node in-place with Form BD data fetched from the firm API."""
    basic = detail.get("basicInformation", {})
    if not basic:
        return
    node["firmType"]      = basic.get("firmType")
    node["formedState"]   = basic.get("formedState")
    node["formedDate"]    = _parse_date(basic.get("formedDate"))
    node["firmSize"]      = basic.get("firmSize")
    node["firmStatus"]    = basic.get("firmStatus")
    node["regulator"]     = basic.get("regulator")
    node["bcScope"]       = basic.get("bcScope")
    node["iaScope"]       = basic.get("iaScope")
    node["otherNames"]    = detail.get("basicInformation", {}).get("otherNames", [])
    node["disclosures"]   = [
        {"type": d.get("disclosureType"), "count": d.get("disclosureCount")}
        for d in detail.get("disclosures", [])
    ]
    # Persist the raw directOwners list on the firm node itself for reference
    node["directOwners"]  = detail.get("directOwners", [])


def build_employment_links(crd: str, detail: dict) -> list[dict]:
    """Return one edge per employment record (current + previous)."""
    links = []
    all_jobs = (
        detail.get("currentEmployments", [])
        + detail.get("previousEmployments", [])
        + detail.get("currentIAEmployments", [])
        + detail.get("previousIAEmployments", [])
    )
    seen = set()
    for job in all_jobs:
        firm_id = str(job.get("firmId", ""))
        if not firm_id:
            continue
        # deduplicate identical (person, firm, startDate) triples
        start = _parse_date(job.get("registrationBeginDate"))
        end = _parse_date(job.get("registrationEndDate"))
        key = (crd, firm_id, start, end)
        if key in seen:
            continue
        seen.add(key)

        links.append({
            "source": _person_node_id(crd),
            "target": _firm_node_id(firm_id),
            "relationship": "employed_by",
            "startDate": start,
            "endDate": end,
            "iaOnly": job.get("iaOnly", "N"),
            "city": job.get("city"),
            "state": job.get("state"),
            "firmName": job.get("firmName"),
        })
    return links


def build_control_links(firm_id: str, firm_detail: dict, nodes_by_id: dict) -> list[dict]:
    """
    Build 'controls' edges from Form BD directOwners → firm.

    Each directOwner entry that has a CRD number becomes an edge:
      person_{crd}  -[controls]->  firm_{firm_id}

    If the person node doesn't exist yet (they were not in the seed search),
    a minimal stub node is created so the graph stays connected.
    """
    links = []
    firm_node_id = _firm_node_id(firm_id)

    for owner in firm_detail.get("directOwners", []):
        crd = str(owner.get("crdNumber", "")).strip()
        legal_name = owner.get("legalName", "").strip()
        position = owner.get("position", "").strip()

        if not crd:
            # Entity owner (no CRD) — create an org node if needed
            entity_id = f"entity_{legal_name.lower().replace(' ', '_')[:48]}"
            if entity_id not in nodes_by_id:
                nodes_by_id[entity_id] = {
                    "id": entity_id,
                    "label": legal_name,
                    "group": "entity",
                    "bcScope": owner.get("bcScope", ""),
                }
            links.append({
                "source": entity_id,
                "target": firm_node_id,
                "relationship": "controls",
                "position": position,
                "startDate": None,
                "endDate": None,
            })
            continue

        person_node_id = _person_node_id(crd)
        # Stub the person node if not already discovered via seed search
        if person_node_id not in nodes_by_id:
            # Parse name from "LAST, FIRST MIDDLE" convention FINRA uses
            if ", " in legal_name:
                last_part, rest = legal_name.split(", ", 1)
                name_parts = rest.split()
                first = name_parts[0] if name_parts else ""
                middle = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
            else:
                name_parts = legal_name.split()
                first = name_parts[0] if name_parts else ""
                last_part = name_parts[-1] if len(name_parts) > 1 else ""
                middle = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""

            nodes_by_id[person_node_id] = {
                "id": person_node_id,
                "crd": crd,
                "label": legal_name,
                "group": "individual",
                "firstName": first,
                "middleName": middle,
                "lastName": last_part,
                "bcScope": owner.get("bcScope", ""),
                "stub": True,   # fetched only from Form BD, not from individual search
            }
            log.info("    Stub person from Form BD: %s (CRD %s)", legal_name, crd)

        links.append({
            "source": person_node_id,
            "target": firm_node_id,
            "relationship": "controls",
            "position": position,
            "startDate": None,
            "endDate": None,
        })

    return links


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_graph(seed_names: list[str]) -> dict:
    nodes_by_id: dict[str, dict] = {}
    links: list[dict] = []
    seen_crds: set[str] = set()

    for name in seed_names:
        log.info("Searching: %s", name)
        summaries = search_individuals(name)
        log.info("  → %d result(s)", len(summaries))

        for summary in summaries:
            source = summary.get("_source", {})
            crd = str(source.get("ind_source_id", ""))
            if not crd or crd in seen_crds:
                continue
            seen_crds.add(crd)

            log.info("  Fetching detail for CRD %s (%s %s)", crd,
                     source.get("ind_firstname", ""), source.get("ind_lastname", ""))
            time.sleep(REQUEST_DELAY)

            detail = fetch_detail(crd)

            # Person node
            person_node = build_person_node(crd, source, detail)
            nodes_by_id[person_node["id"]] = person_node

            # Employment edges + implicit firm nodes
            emp_links = build_employment_links(crd, detail)
            for lnk in emp_links:
                firm_node_id = lnk["target"]
                if firm_node_id not in nodes_by_id:
                    firm_id = firm_node_id.replace("firm_", "")
                    all_jobs = (
                        detail.get("currentEmployments", [])
                        + detail.get("previousEmployments", [])
                        + detail.get("currentIAEmployments", [])
                        + detail.get("previousIAEmployments", [])
                    )
                    extra = next(
                        (j for j in all_jobs if str(j.get("firmId", "")) == firm_id),
                        None,
                    )
                    nodes_by_id[firm_node_id] = build_firm_node(
                        firm_id, lnk["firmName"] or firm_id, extra
                    )
            links.extend(emp_links)

    # ------------------------------------------------------------------
    # Phase 2: Fetch Form BD for every firm node discovered in Phase 1
    # and build control/ownership edges from directOwners.
    # ------------------------------------------------------------------
    firm_ids = [
        n["firmId"]
        for n in list(nodes_by_id.values())
        if n.get("group") == "firm"
    ]
    log.info("Fetching Form BD for %d firm(s)…", len(firm_ids))
    seen_firm_fetch: set[str] = set()

    for firm_id in firm_ids:
        if firm_id in seen_firm_fetch:
            continue
        seen_firm_fetch.add(firm_id)

        log.info("  Firm BD: %s", firm_id)
        time.sleep(REQUEST_DELAY)
        firm_detail = fetch_firm_detail(firm_id)
        if not firm_detail:
            continue

        # Enrich existing firm node with Form BD metadata
        enrich_firm_node_from_detail(nodes_by_id[_firm_node_id(firm_id)], firm_detail)

        # Build control edges (may add stub person/entity nodes)
        ctrl_links = build_control_links(firm_id, firm_detail, nodes_by_id)
        links.extend(ctrl_links)
        log.info("    → %d control link(s)", len(ctrl_links))

    individuals = [n for n in nodes_by_id.values() if n["group"] == "individual"]
    firms       = [n for n in nodes_by_id.values() if n["group"] == "firm"]
    entities    = [n for n in nodes_by_id.values() if n["group"] == "entity"]
    log.info(
        "Graph complete: %d individuals (%d stubs), %d firms, %d entities, %d links",
        len(individuals),
        sum(1 for n in individuals if n.get("stub")),
        len(firms),
        len(entities),
        len(links),
    )

    return {
        "nodes": list(nodes_by_id.values()),
        "links": links,
        "meta": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "seedNames": seed_names,
            "totalIndividuals": len(individuals),
            "totalFirms": len(firms),
            "totalEntities": len(entities),
            "totalLinks": len(links),
        },
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build a FINRA BrokerCheck graph JSON.")
    parser.add_argument(
        "--seeds",
        default=None,
        help="Path to a JSON array of name strings. Defaults to server/data/finra-seeds.json",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output file path. Defaults to server/data/finra-graph.json",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent.parent / "data"

    seeds_path = Path(args.seeds) if args.seeds else data_dir / "national" / "finra-seeds.json"
    out_path = Path(args.out) if args.out else data_dir / "national" / "finra-graph.json"

    if not seeds_path.exists():
        log.error("Seeds file not found: %s", seeds_path)
        sys.exit(1)

    with seeds_path.open("r", encoding="utf-8") as fh:
        seed_names: list[str] = json.load(fh)

    if not isinstance(seed_names, list) or not seed_names:
        log.error("Seeds file must be a non-empty JSON array of strings.")
        sys.exit(1)

    log.info("Loaded %d seed name(s) from %s", len(seed_names), seeds_path)
    graph = build_graph(seed_names)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(graph, fh, ensure_ascii=False, indent=2)

    log.info("Saved graph to %s", out_path)


if __name__ == "__main__":
    main()
