#!/usr/bin/env python3
"""
24/7 Breaking News -> Gold (XAU/USD) Impact Telegram Bot
Monitors Bloomberg & Investing.com breaking/market news for US, China, and Russia,
analyzes potential impact on Gold (Bullish / Bearish / Neutral), and pushes alerts to Telegram.
"""

import hashlib
import html
import logging
import os
import re
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # 5 minutes
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
DB_PATH = os.getenv("DB_PATH", "gold_news_bot.db")
REQUEST_TIMEOUT = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gold_news_bot")

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# News Sources (Breaking & Markets RSS + Google News RSS Fallbacks)
# ---------------------------------------------------------------------------
NEWS_FEEDS: List[Tuple[str, str]] = [
    # Direct Bloomberg RSS feeds
    ("Bloomberg Markets", "https://feeds.bloomberg.com/markets/news.rss"),
    ("Bloomberg Politics", "https://feeds.bloomberg.com/politics/news.rss"),
    ("Bloomberg Economics", "https://feeds.bloomberg.com/economics/news.rss"),
    # Direct Investing.com RSS feeds
    ("Investing.com Breaking/All", "https://www.investing.com/rss/news.rss"),
    ("Investing.com Most Popular", "https://www.investing.com/rss/news_285.rss"),
    ("Investing.com Commodities", "https://www.investing.com/rss/news_11.rss"),
    ("Investing.com Economy", "https://www.investing.com/rss/news_14.rss"),
    # Google News RSS fallbacks (bypasses Cloudflare/Akamai blocks on direct HTML)
    (
        "Bloomberg (via Google News)",
        "https://news.google.com/rss/search?q="
        + quote_plus("site:bloomberg.com (US OR China OR Russia OR Fed OR Gold) when:1d")
        + "&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Investing.com (via Google News)",
        "https://news.google.com/rss/search?q="
        + quote_plus("site:investing.com (US OR China OR Russia OR Fed OR Gold) when:1d")
        + "&hl=en-US&gl=US&ceid=US:en",
    ),
]

# ---------------------------------------------------------------------------
# Country / Region Filter Rules (America, China, Russia)
# ---------------------------------------------------------------------------
REGION_PATTERNS: Dict[str, List[re.Pattern]] = {
    "🇺🇸 America": [
        re.compile(r"\b(?:U\.?S\.?A?|America|American|Washington|White House)\b", re.I),
        re.compile(r"\b(?:Federal Reserve|Fed|FOMC|Jerome Powell|Powell|Yellen|Bessent)\b", re.I),
        re.compile(r"\b(?:Treasury|Pentagon|Congress|Senate|Trump|Wall Street|USD|DXY)\b", re.I),
        re.compile(r"\b(?:Nonfarm|NFP|US CPI|US PPI|US GDP)\b", re.I),
        re.compile(r"\bUS\b"),  # Case-sensitive to avoid matching "us"
    ],
    "🇨🇳 China": [
        re.compile(r"\b(?:China|Chinese|Beijing|Shanghai|Shenzhen|Sino-)\b", re.I),
        re.compile(r"\b(?:PBOC|People's Bank of China|Xi Jinping|Yuan|Renminbi|RMB|CNY|CNH)\b", re.I),
        re.compile(r"\b(?:Taiwan|South China Sea|Hong Kong)\b", re.I),
    ],
    "🇷🇺 Russia": [
        re.compile(r"\b(?:Russia|Russian|Moscow|Kremlin|Putin|Ruble|RUB)\b", re.I),
        re.compile(r"\b(?:Bank of Russia|Gazprom|Rosneft|Ukraine)\b", re.I),
    ],
}

# ---------------------------------------------------------------------------
# Gold (XAU/USD) Impact Rule Engine
# ---------------------------------------------------------------------------
BULLISH_DRIVERS: List[Tuple[re.Pattern, int, str]] = [
    (re.compile(r"\b(?:war|strike|missile|invasion|escalat\w+|attack|conflict|military)\b", re.I), 3, "Geopolitical escalation / safe-haven demand"),
    (re.compile(r"\b(?:sanction\w*|tariff\w*|trade war|embargo|retaliat\w*|ban)\b", re.I), 2, "Trade/sanctions friction boosts safe-haven flows"),
    (re.compile(r"\b(?:rate cut|cut rates|dovish|easing|qe|stimulus|liquidity)\b", re.I), 3, "Dovish monetary policy lowers opportunity cost of holding gold"),
    (re.compile(r"\b(?:dollar falls|dollar drops|weak dollar|dxy falls|dxy drops|yields fall|yields drop)\b", re.I), 2, "Weaker USD / falling yields support XAU/USD"),
    (re.compile(r"\b(?:recession|slowdown|stagflation|default|banking crisis|bank failure|shutdown)\b", re.I), 2, "Macro stress / recession risk drives defensive gold bid"),
    (re.compile(r"\b(?:central bank\w* buy\w*|gold reserves|pboc.*gold|record high|safe haven)\b", re.I), 2, "Physical / central bank accumulation supports gold"),
    (re.compile(r"\b(?:inflation rises|hot inflation|cpi surges|stagflation)\b", re.I), 1, "Inflation hedging demand (watch Fed reaction)"),
]

BEARISH_DRIVERS: List[Tuple[re.Pattern, int, str]] = [
    (re.compile(r"\b(?:rate hike|hike rates|hawkish|higher for longer|tightening)\b", re.I), 3, "Hawkish policy raises real yields and pressures gold"),
    (re.compile(r"\b(?:dollar surges|dollar rallies|strong dollar|dxy surges|dxy rises|yields surge|yields rise)\b", re.I), 2, "Stronger USD / rising yields weigh on XAU/USD"),
    (re.compile(r"\b(?:ceasefire|peace deal|peace talks|truce|de-escalat\w*|trade deal|agreement reached)\b", re.I), 3, "Geopolitical de-escalation reduces safe-haven premium"),
    (re.compile(r"\b(?:jobs beat|strong payrolls|nfp beat|hotter.*jobs|unemployment falls|gdp beats)\b", re.I), 2, "Strong US growth data reduces near-term rate-cut odds"),
    (re.compile(r"\b(?:gold falls|gold drops|gold slides|sell-off in gold)\b", re.I), 1, "Direct downside price pressure reported"),
]


@dataclass
class NewsItem:
    source: str
    title: str
    summary: str
    url: str
    published: str
    regions: List[str]

    @property
    def uid(self) -> str:
        raw = f"{self.title.strip().lower()}|{self.url.split('?')[0].strip().lower()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass
class GoldAnalysis:
    verdict: str  # BULLISH, BEARISH, NEUTRAL
    emoji: str
    label: str
    rationale: str
    method: str  # "AI" or "Macro Rules"


def clean_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    cleaned = " ".join(soup.get_text(" ").split())
    return cleaned[:400]


def match_regions(text: str) -> List[str]:
    matched = []
    for region_label, patterns in REGION_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            matched.append(region_label)
    return matched


def analyze_gold_rules(title: str, summary: str) -> GoldAnalysis:
    text = f"{title}. {summary}"
    score = 0
    reasons: List[str] = []

    for pattern, weight, reason in BULLISH_DRIVERS:
        if pattern.search(text):
            score += weight
            reasons.append(f"+ {reason}")

    for pattern, weight, reason in BEARISH_DRIVERS:
        if pattern.search(text):
            score -= weight
            reasons.append(f"- {reason}")

    if score >= 2:
        verdict, emoji, label = "BULLISH", "🟢", "PUSH UP (Bullish for Gold)"
    elif score <= -2:
        verdict, emoji, label = "BEARISH", "🔴", "PUSH DOWN (Bearish for Gold)"
    elif score == 1:
        verdict, emoji, label = "BULLISH", "🟡", "SLIGHTLY BULLISH (Mild Upside Bias)"
    elif score == -1:
        verdict, emoji, label = "BEARISH", "🟠", "SLIGHTLY BEARISH (Mild Downside Bias)"
    else:
        verdict, emoji, label = "NEUTRAL", "⚪", "NEUTRAL (No Clear XAU/USD Catalyst)"

    rationale = "; ".join(reasons[:3]) if reasons else "No direct Fed, USD/yields, or safe-haven catalyst detected."
    return GoldAnalysis(verdict=verdict, emoji=emoji, label=label, rationale=rationale, method="Macro Rules")


def analyze_gold_llm(title: str, summary: str, regions: List[str]) -> Optional[GoldAnalysis]:
    if not OPENAI_API_KEY:
        return None
    prompt = (
        "You are an institutional XAU/USD (Gold) macro analyst. "
        "Analyze the breaking news below and classify its likely immediate impact on Gold price.\n"
        "Respond in EXACTLY this 2-line format:\n"
        "VERDICT: BULLISH | BEARISH | NEUTRAL\n"
        "REASON: <1-2 concise sentences explaining transmission via USD, real yields, Fed policy, or safe-haven flows>\n\n"
        f"Regions: {', '.join(regions)}\n"
        f"Headline: {title}\n"
        f"Summary: {summary}"
    )
    try:
        resp = requests.post(
            f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 120,
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        verdict = "NEUTRAL"
        reason = content
        for line in content.splitlines():
            if line.upper().startswith("VERDICT:"):
                v = line.split(":", 1)[1].strip().upper()
                if "BULL" in v:
                    verdict = "BULLISH"
                elif "BEAR" in v:
                    verdict = "BEARISH"
                else:
                    verdict = "NEUTRAL"
            elif line.upper().startswith("REASON:"):
                reason = line.split(":", 1)[1].strip()

        mapping = {
            "BULLISH": ("🟢", "PUSH UP (Bullish for Gold)"),
            "BEARISH": ("🔴", "PUSH DOWN (Bearish for Gold)"),
            "NEUTRAL": ("⚪", "NEUTRAL"),
        }
        emoji, label = mapping[verdict]
        return GoldAnalysis(verdict=verdict, emoji=emoji, label=label, rationale=reason, method=f"AI ({OPENAI_MODEL})")
    except Exception as exc:
        logger.warning("LLM analysis failed, falling back to rules: %s", exc)
        return None


def analyze_gold_impact(item: NewsItem) -> GoldAnalysis:
    ai_result = analyze_gold_llm(item.title, item.summary, item.regions)
    if ai_result:
        return ai_result
    return analyze_gold_rules(item.title, item.summary)


# ---------------------------------------------------------------------------
# SQLite Persistence (Deduplication & Subscribers)
# ---------------------------------------------------------------------------
class BotDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_news (
                    uid TEXT PRIMARY KEY,
                    title TEXT,
                    source TEXT,
                    created_at TEXT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id TEXT PRIMARY KEY,
                    added_at TEXT
                )
                """
            )
            if DEFAULT_CHAT_ID:
                self.conn.execute(
                    "INSERT OR IGNORE INTO subscribers (chat_id, added_at) VALUES (?, ?)",
                    (DEFAULT_CHAT_ID, datetime.now(timezone.utc).isoformat()),
                )

    def is_seen(self, uid: str) -> bool:
        with self.lock:
            cur = self.conn.execute("SELECT 1 FROM seen_news WHERE uid = ?", (uid,))
            return cur.fetchone() is not None

    def mark_seen(self, item: NewsItem) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO seen_news (uid, title, source, created_at) VALUES (?, ?, ?, ?)",
                (item.uid, item.title, item.source, datetime.now(timezone.utc).isoformat()),
            )

    def count_seen(self) -> int:
        with self.lock:
            cur = self.conn.execute("SELECT COUNT(*) FROM seen_news")
            return int(cur.fetchone()[0])

    def add_subscriber(self, chat_id: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO subscribers (chat_id, added_at) VALUES (?, ?)",
                (str(chat_id), datetime.now(timezone.utc).isoformat()),
            )

    def remove_subscriber(self, chat_id: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM subscribers WHERE chat_id = ?", (str(chat_id),))

    def get_subscribers(self) -> List[str]:
        with self.lock:
            cur = self.conn.execute("SELECT chat_id FROM subscribers")
            return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Feed Fetcher
# ---------------------------------------------------------------------------
def fetch_matching_news() -> List[NewsItem]:
    items: List[NewsItem] = []
    seen_in_batch = set()

    for source_name, feed_url in NEWS_FEEDS:
        try:
            resp = requests.get(feed_url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                logger.warning("Feed %s returned HTTP %s", source_name, resp.status_code)
                continue
            parsed = feedparser.parse(resp.content)
            for entry in parsed.entries[:25]:
                title = clean_html(getattr(entry, "title", ""))
                summary = clean_html(getattr(entry, "summary", "") or getattr(entry, "description", ""))
                url = getattr(entry, "link", "").strip()
                published = getattr(entry, "published", "") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

                if not title or not url:
                    continue

                regions = match_regions(f"{title} {summary}")
                if not regions:
                    continue

                item = NewsItem(
                    source=source_name,
                    title=title,
                    summary=summary,
                    url=url,
                    published=published,
                    regions=regions,
                )
                if item.uid not in seen_in_batch:
                    seen_in_batch.add(item.uid)
                    items.append(item)
        except Exception as exc:
            logger.warning("Error fetching %s: %s", source_name, exc)

    return items


# ---------------------------------------------------------------------------
# Telegram Messaging
# ---------------------------------------------------------------------------
def send_telegram_message(chat_id: str, text: str) -> bool:
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            api_url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.error("Telegram sendMessage failed (%s): %s", chat_id, resp.text)
            return False
        return True
    except Exception as exc:
        logger.error("Telegram sendMessage exception (%s): %s", chat_id, exc)
        return False


def format_alert(item: NewsItem, analysis: GoldAnalysis) -> str:
    regions_str = " | ".join(item.regions)
    safe_title = html.escape(item.title)
    safe_source = html.escape(item.source)
    safe_summary = html.escape(item.summary[:240] + ("..." if len(item.summary) > 240 else "")) if item.summary else ""
    safe_rationale = html.escape(analysis.rationale)
    safe_url = html.escape(item.url)

    lines = [
        f"🚨 <b>BREAKING NEWS</b> | {regions_str}",
        f"📰 <b>{safe_title}</b>",
    ]
    if safe_summary and safe_summary.lower() != safe_title.lower():
        lines.append(f"<i>{safe_summary}</i>")
    lines.extend(
        [
            "",
            f"{analysis.emoji} <b>Gold (XAU/USD) Impact:</b> {html.escape(analysis.label)}",
            f"📊 <b>Analysis ({html.escape(analysis.method)}):</b> {safe_rationale}",
            f"🔗 <a href=\"{safe_url}\">Read on {safe_source}</a>",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bot Loops
# ---------------------------------------------------------------------------
stop_event = threading.Event()


def scan_and_broadcast(db: BotDB, max_initial: int = 5) -> int:
    items = fetch_matching_news()
    subscribers = db.get_subscribers()
    if not subscribers:
        logger.info("No subscribers registered yet. Send /start to the bot or set TELEGRAM_CHAT_ID.")

    is_first_run = db.count_seen() == 0
    sent_count = 0

    for item in items:
        if db.is_seen(item.uid):
            continue
        db.mark_seen(item)

        if is_first_run and sent_count >= max_initial:
            # Mark older backlog as seen without flooding on first startup
            continue

        if subscribers:
            analysis = analyze_gold_impact(item)
            msg = format_alert(item, analysis)
            for chat_id in subscribers:
                send_telegram_message(chat_id, msg)
                time.sleep(0.2)
            sent_count += 1

    return sent_count


def telegram_command_loop(db: BotDB) -> None:
    offset = 0
    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    logger.info("Telegram command listener started.")

    while not stop_event.is_set():
        try:
            resp = requests.get(
                base_url,
                params={"offset": offset, "timeout": 25},
                timeout=35,
            )
            if resp.status_code != 200:
                time.sleep(5)
                continue
            data = resp.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or update.get("channel_post")
                if not msg:
                    continue
                chat_id = str(msg["chat"]["id"])
                text = (msg.get("text") or "").strip().lower()

                if text.startswith("/start") or text.startswith("/subscribe"):
                    db.add_subscriber(chat_id)
                    send_telegram_message(
                        chat_id,
                        (
                            "✅ <b>Subscribed to Breaking Gold News Bot!</b>\n"
                            f"• Sources: Bloomberg &amp; Investing.com\n"
                            f"• Regions: 🇺🇸 US | 🇨🇳 China | 🇷🇺 Russia\n"
                            f"• Poll Interval: every {POLL_INTERVAL_SECONDS}s\n\n"
                            "Commands: /latest | /status | /unsubscribe"
                        ),
                    )
                elif text.startswith("/unsubscribe"):
                    db.remove_subscriber(chat_id)
                    send_telegram_message(chat_id, "🔕 Unsubscribed from breaking news alerts.")
                elif text.startswith("/status"):
                    send_telegram_message(
                        chat_id,
                        (
                            "🤖 <b>Bot Status: Online 24/7</b>\n"
                            f"• Subscribers: {len(db.get_subscribers())}\n"
                            f"• Tracked articles: {db.count_seen()}\n"
                            f"• Poll interval: {POLL_INTERVAL_SECONDS}s\n"
                            f"• AI Mode: {'Enabled (' + OPENAI_MODEL + ')' if OPENAI_API_KEY else 'Macro Rule Engine'}"
                        ),
                    )
                elif text.startswith("/latest"):
                    send_telegram_message(chat_id, "🔍 Scanning Bloomberg &amp; Investing.com now...")
                    items = fetch_matching_news()[:3]
                    if not items:
                        send_telegram_message(chat_id, "No matching US/China/Russia breaking news found right now.")
                    for item in items:
                        analysis = analyze_gold_impact(item)
                        send_telegram_message(chat_id, format_alert(item, analysis))
                        time.sleep(0.2)
        except Exception as exc:
            logger.warning("Command loop error: %s", exc)
            time.sleep(5)


def handle_signal(signum, frame) -> None:
    logger.info("Shutdown signal received (%s). Stopping...", signum)
    stop_event.set()


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Add it to your .env file.")
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    db = BotDB(DB_PATH)
    cmd_thread = threading.Thread(target=telegram_command_loop, args=(db,), daemon=True)
    cmd_thread.start()

    logger.info("Gold Breaking News Bot running 24/7 (interval=%ss)...", POLL_INTERVAL_SECONDS)
    while not stop_event.is_set():
        try:
            sent = scan_and_broadcast(db)
            logger.info("Scan complete. New alerts sent: %d", sent)
        except Exception as exc:
            logger.error("Scan loop error: %s", exc)

        # Sleep in 1s increments so shutdown is responsive
        for _ in range(POLL_INTERVAL_SECONDS):
            if stop_event.is_set():
                break
            time.sleep(1)


if __name__ == "__main__":
    main()