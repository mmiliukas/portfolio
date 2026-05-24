import logging

import pandas as pd

logger = logging.getLogger(__name__)


def update_readme(df: pd.DataFrame) -> None:
    logger.info("Updating README.md...")

    lines = ["## Latest 20 publications"]

    df_to_log = df.tail(20).sort_values("pubDate", ascending=False)

    for index, row in df_to_log.iterrows():
        pubDate, title, link = row["pubDate"], row["title"], row["link"]
        lines.append(f"- {pubDate}: [{title}]({link})")

    with open("README.md", "w") as file:
        file.write("\n".join(lines))
