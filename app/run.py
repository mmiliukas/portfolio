import asyncio
import logging
import sys
from datetime import date, timedelta

import pandas as pd

# from telegram import Bot
from config import PUBLICATIONS_FILE
from download_api import download_api
from download_rss import download_rss
from update_readme import update_readme

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


async def run(token: str, chat_id: str):
    df = pd.read_csv(PUBLICATIONS_FILE)

    df_rss = download_rss()
    df_api = download_api(date.today() - timedelta(days=1))

    df_final = pd.concat([df, df_rss, df_api])
    df_final = df_final.drop_duplicates(subset=["link"], keep="first")
    df_final = df.sort_values(by="pubDate", ascending=True)

    update_readme(df_final)
    df_final.to_csv(PUBLICATIONS_FILE, index=False)

    # if df.empty:
    #     logger.info("No new publications for now")
    #     df_diff = pd.DataFrame([])
    # else:
    #     logger.info("Saving publications...")

    #     df_previous = pd.read_csv(PUBLICATIONS_FILE)
    #     df_diff = df[~df["link"].isin(df_previous["link"])]

    #     df = pd.concat([df_previous, df])
    #     df.drop_duplicates().to_csv(PUBLICATIONS_FILE, index=False)

    # telegram = Bot(token=token)
    # today = date.today().isoformat()

    # if df_diff.empty:
    #     message = "No new publications for now"
    # else:
    #     publications = []
    #     for _, row in df_diff.iterrows():
    #         title = html.escape(row["title"])
    #         link = html.escape(row["link"])
    #         publications.append(f"• <a href='{link}'>{title}</a>")
    #     message = "\n".join(publications)

    # await telegram.send_message(
    #     chat_id=chat_id,
    #     parse_mode="Html",
    #     text=(f"<b>Portfolio {today}</b>\n{message}"),
    # )


if __name__ == "__main__":
    telegram_token = sys.argv[1]
    chat_id = sys.argv[2]

    asyncio.run(run(telegram_token, chat_id))
