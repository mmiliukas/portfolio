import logging
import xml.etree.ElementTree as ElementTree
from datetime import date, datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def as_date(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%Y-%m-%d")


def download_api(for_date: date) -> pd.DataFrame:
    logger.info(f"Downloading from arXiv for date {for_date}...")

    date_from = for_date.strftime("%Y%m%d0000")
    date_to = for_date.strftime("%Y%m%d2359")

    url = "https://export.arxiv.org/api/query"
    params = {
        "max_results": 50,
        "search_query": f"cat:q-fin.PM AND lastUpdatedDate:[{date_from} TO {date_to}]",
        "sortBy": "lastUpdatedDate",
        "sortOrder": "descending",
    }

    resp = requests.get(url, params, timeout=30)
    resp.raise_for_status()

    namespaces = {"atom": "http://www.w3.org/2005/Atom"}

    tree = ElementTree.fromstring(resp.content)
    publications = []

    for item in tree.findall("atom:entry", namespaces):
        pubDate = as_date((item.findtext("atom:published", namespaces=namespaces) or "").strip())
        title = (item.findtext("atom:title", namespaces=namespaces) or "").strip()

        pdf_link_node = item.find('atom:link[@type="application/pdf"]', namespaces)
        link = pdf_link_node.get("href") if pdf_link_node is not None else None

        publications.append(
            {
                "pubDate": pubDate,
                "title": title,
                "link": link,
            }
        )

    if publications:
        return pd.DataFrame.from_records(publications)

    return pd.DataFrame({"pubDate": [], "title": [], "link": []})
