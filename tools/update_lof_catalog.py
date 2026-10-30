#!/usr/bin/env python3
"""Refresh the offline LOF type catalog from public SSE/SZSE directories only."""
import argparse
import concurrent.futures
from datetime import date, datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import re
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "docs" / "data" / "lof_catalog.json"
OUTPUTS = (ROOT / "order_bridge" / "lof_catalog.py",
           ROOT / "service" / "ipc_bridge" / "order" / "lof_catalog.py")


def clean(value):
    return html.unescape(re.sub(r"<[^>]+>", "", str(value or ""))).strip()


def fetch(base, params, referer):
    url = base + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": referer})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read()
    return json.loads(raw), {"url": url, "sha256": hashlib.sha256(raw).hexdigest(),
                             "fetched_at": datetime.now(timezone.utc).isoformat()}


def refresh():
    sse = "https://query.sse.com.cn/commonQuery.do?"
    referer = "https://etf.sse.com.cn/fundlist/"
    catalog, receipt = fetch(sse, {"sqlId": "COMMON_JJZWZ_JJLB_L", "type": "inParams", "CATEGORY": "F000",
        "pageHelp.pageSize": 2000, "pageHelp.pageNo": 1, "isPagination": "true"}, referer)
    types, type_receipt = fetch(sse, {"sqlId": "COMMON_JJZWZ_JJLB_JJLX_C"}, referer)
    if catalog.get("actionErrors") or catalog.get("fieldErrors") or types.get("actionErrors") or types.get("fieldErrors"):
        raise ValueError("SSE directory returned errors")
    if len(catalog["result"]) != catalog["pageHelp"]["total"] or catalog["pageHelp"]["pageCount"] != 1:
        raise ValueError("incomplete SSE directory")
    parent = {r["CATEGORY_CODE"]: r["CATEGORY_PARENT_CODE"] for r in types["result"]}
    def is_lof(category):
        seen = set()
        while category in parent and category not in seen:
            if category == "F200":
                return True
            seen.add(category)
            category = parent[category]
        return False
    rows = [{"symbol": r["FUND_CODE"] + ".SH", "kind": "LOF", "classification": r["CATEGORY"],
             "name": r["FUND_EXPANSION_ABBR"], "list_date": r["LISTING_DATE"], "source": receipt["url"]}
            for r in catalog["result"] if is_lof(r["CATEGORY"])]
    sources = [receipt, type_receipt]
    def page(number):
        data, proof = fetch("https://www.szse.cn/api/report/ShowReport/data?",
            {"SHOWTYPE": "JSON", "CATALOGID": "1000_lf", "TABKEY": "tab1", "PAGENO": number},
            "https://fund.szse.cn/marketdata/fundslist/index.html")
        tab = next(t for t in data if t["metadata"]["tabkey"] == "tab1")
        if tab["metadata"]["pageno"] != number:
            raise ValueError("wrong SZSE page")
        return tab, proof
    first, proof = page(1)
    pages = [(first, proof)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        pages += list(pool.map(page, range(2, first["metadata"]["pagecount"] + 1)))
    all_codes = set()
    for tab, proof in pages:
        if any(tab["metadata"][k] != first["metadata"][k] for k in ("recordcount", "subname")):
            raise ValueError("SZSE directory changed during pagination; retry refresh")
        sources.append(proof)
        for row in tab["data"]:
            code = clean(row["sys_key"])
            if not re.fullmatch(r"\d{6}", code) or code in all_codes:
                raise ValueError("invalid or duplicate SZSE code")
            all_codes.add(code)
            if clean(row["jjlb"]) == "LOF":
                rows.append({"symbol": code + ".SZ", "kind": "LOF", "classification": "LOF",
                    "name": clean(row["jjjcurl"]), "list_date": clean(row["ssrq"]), "source": proof["url"]})
    if len(all_codes) != first["metadata"]["recordcount"]:
        raise ValueError("incomplete SZSE directory")
    return {"schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
            "szse_as_of": first["metadata"]["subname"], "sse_lof_categories": sorted(k for k in parent if is_lof(k)),
            "sources": sources, "records": sorted(rows, key=lambda r: r["symbol"]),
            "listing_status_verified": False}


def render(snapshot):
    if snapshot["schema_version"] != 1 or snapshot["listing_status_verified"] is not False:
        raise ValueError("unsupported catalog schema or lifecycle claim")
    sources = {r["url"] for r in snapshot["sources"]}
    symbols = []
    for row in snapshot["records"]:
        if (row["kind"] != "LOF" or not re.fullmatch(r"\d{6}\.(SH|SZ)", row["symbol"])
                or row["source"] not in sources or not row["list_date"]):
            raise ValueError("invalid LOF catalog row")
        date.fromisoformat(row["list_date"])
        allowed = snapshot["sse_lof_categories"] if row["symbol"].endswith(".SH") else ["LOF"]
        if row["classification"] not in allowed:
            raise ValueError("non-LOF classification")
        symbols.append(row["symbol"])
    if len(set(symbols)) != len(symbols) or not all(any(s.endswith(e) for s in symbols) for e in (".SH", ".SZ")):
        raise ValueError("duplicate or incomplete LOF catalog")
    canonical = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    header = ('# -*- coding: utf-8 -*-\n'
              '"""Generated LOF identities; run tools/update_lof_catalog.py --refresh.\n'
              'Source: docs/data/lof_catalog.json; identity does not prove current listing or permission.\n'
              '"""\nLOF_CATALOG_SHA256 = "' + hashlib.sha256(canonical).hexdigest() + '"\n'
              'LOF_CATALOG_AS_OF = "' + snapshot["generated_at"][:10] + '"\nLOF_SYMBOLS = frozenset((\n')
    symbols.sort()
    return header + "".join("    " + ", ".join(repr(s) for s in symbols[n:n + 6]) + ",\n"
                            for n in range(0, len(symbols), 6)) + "))\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--refresh", action="store_true", help="Fetch official directories; never connects to QMT or a DB")
    action.add_argument("--check", action="store_true", help="Check both generated modules against the saved snapshot offline")
    args = parser.parse_args(argv)
    snapshot = refresh() if args.refresh else json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    content = render(snapshot)
    if args.check:
        return 0 if all(p.is_file() and p.read_text(encoding="utf-8") == content for p in OUTPUTS) else 1
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    if args.refresh:
        SNAPSHOT.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for path in OUTPUTS:
        path.write_text(content, encoding="utf-8")
    print("LOF catalog: {0} identities; listing status is not verified".format(len(snapshot["records"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
