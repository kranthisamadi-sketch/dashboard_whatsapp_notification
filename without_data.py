import os
import logging
import requests
from datetime import datetime
from twilio.rest import Client
from apscheduler.schedulers.background import BackgroundScheduler
import time

# =============================================================================
# CONFIGURATION
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

METABASE_URL = os.getenv("METABASE_URL", "https://metabase.rozana.tech").rstrip("/")
METABASE_EMAIL = os.getenv("METABASE_EMAIL", "kranthisamadi@gmail.com")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "CCD88_MvddPmnc")
METABASE_DASHBOARD_ID = os.getenv("METABASE_DASHBOARD_ID", "82")

VERIFY_TLS = os.getenv("METABASE_VERIFY_TLS", "true").lower() != "false"

TABLE_NAME = os.getenv("TABLE_NAME", "Freebies Data store-wise")
TWILIO_SID = os.getenv("TWILIO_SID", "AC3e5c87780be233a0f661e8a34f7b97f7")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "755e6d50a39289a395a6ba792e8c1616")
FROM_WA = os.getenv("FROM_WA", "whatsapp:+14155238886")
TO_WA = os.getenv("TO_WA", "whatsapp:+917842507089")

# Alert interval
try:
    ALERT_INTERVAL_MINUTES = int(os.getenv("ALERT_INTERVAL_MINUTES", "1"))
except ValueError:
    ALERT_INTERVAL_MINUTES = 1

# -----------------------------------------------------------------------------
# TWILIO INIT
# -----------------------------------------------------------------------------
twilio_client = None
if TWILIO_SID and TWILIO_TOKEN:
    try:
        twilio_client = Client(TWILIO_SID, TWILIO_TOKEN)
    except Exception as e:
        logging.warning("⚠️ Could not initialize Twilio client: %s", e)

# =============================================================================
# METABASE CONNECTION
# =============================================================================
def metabase_login() -> dict:
    """Authenticate to Metabase and return session headers."""
    logging.info("🔐 Logging into Metabase at %s", METABASE_URL)
    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=15,
        verify=VERIFY_TLS,
    )
    resp.raise_for_status()
    sid = resp.json().get("id")
    if not sid:
        raise RuntimeError("No session id returned from Metabase")
    logging.info("✅ Metabase login successful.")
    return {"X-Metabase-Session": sid}

def fetch_dashboard_first_card(headers: dict):
    """Fetch first non-empty card data from dashboard."""
    resp = requests.get(
        f"{METABASE_URL}/api/dashboard/{METABASE_DASHBOARD_ID}",
        headers=headers,
        timeout=15,
        verify=VERIFY_TLS,
    )
    resp.raise_for_status()
    dash = resp.json()
    dashcards = dash.get("ordered_cards") or dash.get("dashcards") or []

    if not dashcards:
        logging.warning("⚠️ Dashboard %s has no cards", METABASE_DASHBOARD_ID)
        return [], []

    for dc in dashcards:
        card = dc.get("card") or {}
        card_id = card.get("id")
        if not card_id:
            continue
        name = card.get("name", f"Card {card_id}")
        logging.info("📡 Querying card: %s (%s)", name, card_id)
        try:
            r = requests.post(
                f"{METABASE_URL}/api/card/{card_id}/query/json",
                headers=headers,
                timeout=30,
                verify=VERIFY_TLS,
            )
            r.raise_for_status()
            data = r.json()
            if data:
                logging.info("✅ Card %s returned %d rows", card_id, len(data))
                return name, data
        except Exception as e:
            logging.warning("⚠️ Failed to query card %s: %s", card_id, e)
    logging.warning("⚠️ All dashboard cards returned no data")
    return None, []

# =============================================================================
# TABLE FORMATTER
# =============================================================================
def format_table(rows: list[dict], max_rows: int = 20) -> str:
    """Return a clean ASCII table."""
    if not rows:
        return "No data found."

    # get first row keys
    cols = list(rows[0].keys())
    rows = rows[:max_rows]

    # calculate column widths
    col_widths = [max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)) for c in cols]

    # header
    header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cols))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(cols)))

    # data rows
    data_lines = []
    for r in rows:
        line = " | ".join(str(r.get(c, "")).ljust(col_widths[i]) for i, c in enumerate(cols))
        data_lines.append(line)

    return "\n".join([header, separator] + data_lines)

# =============================================================================
# WHATSAPP MESSAGE BUILDER & SENDER
# =============================================================================
def send_whatsapp_alert():
    try:
        headers = metabase_login()
        card_name, data = fetch_dashboard_first_card(headers)
        now = datetime.now()
        formatted_date = now.strftime("%Y-%m-%d")
        formatted_time = now.strftime("%H:%M")

        if not data:
            body = f"⚠️ No data found for {"TABLE_NAME"}."
        else:
            table_txt = format_table(data, max_rows=20)
            record_count = len(data)
            body = (
                f"Table/Question: {TABLE_NAME}\n"
                f"URL: {METABASE_URL}/dashboard/{METABASE_DASHBOARD_ID}\n"
                f"Date: {formatted_date}\n"
                f"Orders: {record_count}\n"
                f"Time: {formatted_time}\n"
                f"First 20 records:\n\n{table_txt}\n\n📊 Total Records: {min(20, record_count)}"
            )

        logging.info("📨 WhatsApp preview:\n%s", body)

        if twilio_client and FROM_WA and TO_WA:
            msg = twilio_client.messages.create(from_=FROM_WA, to=TO_WA, body=body)
            logging.info("✅ WhatsApp sent. SID: %s", msg.sid)
        else:
            logging.info("ℹ️ Twilio not configured. Only preview logged.")

    except Exception as e:
        logging.exception("❌ Failed to send WhatsApp alert: %s", e)

# =============================================================================
# MAIN LOOP
# =============================================================================
if __name__ == "__main__":
    logging.info("🚀 Starting dashboard WhatsApp alert every %d minute(s)", ALERT_INTERVAL_MINUTES)
    scheduler = BackgroundScheduler()
    scheduler.add_job(send_whatsapp_alert, "interval", minutes=ALERT_INTERVAL_MINUTES)
    scheduler.start()

    # run once immediately
    send_whatsapp_alert()

    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("🛑 Alert system stopped.")

