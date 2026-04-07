import logging
from datetime import date, datetime
from xml.etree import ElementTree

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

FILE = "./publications.csv"


def as_date(value: str):
    dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")
    return dt.strftime("%Y-%m-%d")


def download() -> pd.DataFrame:
    logger.info("Downloading RSS feed from arXiv...")

    url = "https://rss.arxiv.org/rss/q-fin.PM"

    resp = requests.get(url, timeout=5)
    resp.raise_for_status()

    tree = ElementTree.fromstring(resp.content)

    publications = []

    for item in tree.findall(".//item"):
        pubDate = as_date((item.findtext("pubDate") or "").strip())
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip().replace("/abs/", "/pdf/")

        publications.append(
            {
                "pubDate": pubDate,
                "title": title,
                "link": link,
            }
        )

    df = pd.DataFrame.from_records(publications)
    df = df[df["pubDate"] == date.today().isoformat()]

    return df


def update_readme(df: pd.DataFrame) -> None:
    logger.info("Updating README.md...")

    lines = [f"## Publications {date.today().isoformat()}"]

    for index, row in df.iterrows():
        title, link = row["title"], row["link"]
        lines.append(f"- [{title}]({link})")

    with open("README.md", "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    df = download()
    update_readme(df)

    if df.empty:
        logger.info("No new publications for today")
    else:
        logger.info("Saving publications...")

        df = pd.concat([pd.read_csv(FILE), df])
        df.drop_duplicates().to_csv(FILE, index=False)
