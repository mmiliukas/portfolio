import logging
from datetime import datetime
from xml.etree import ElementTree

import pandas as pd
import requests
from config import HEADERS, RSS_URL

logger = logging.getLogger(__name__)


def as_date(value: str) -> str:
    dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")
    return dt.strftime("%Y-%m-%d")


def download_rss() -> pd.DataFrame:
    logger.info("Downloading RSS feed from arXiv...")

    resp = requests.get(RSS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    tree = ElementTree.fromstring(resp.content)

    publications = []

    for item in tree.findall(".//item"):
        pubDate = as_date((item.findtext("pubDate") or "").strip())
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip().replace("/abs/", "/pdf/")

        publications.append({"pubDate": pubDate, "title": title, "link": link})

    if publications:
        return pd.DataFrame.from_records(publications)

    return pd.DataFrame({"pubDate": [], "title": [], "link": []})
