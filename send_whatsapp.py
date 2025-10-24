import os
import time
import logging
from flask import Flask, request, jsonify
from twilio.rest import Client
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler

# -------------------------------------------------------------
# Setup Flask app and logging
# -------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------
# Twilio credentials (replace with your actual ones)
# -------------------------------------------------------------
TWILIO_SID = "AC3e5c87780be233a0f661e8a34f7b97f7"
TWILIO_TOKEN = "755e6d50a39289a395a6ba792e8c1616"
FROM_WA = "whatsapp:+14155238886"      # Twilio Sandbox number
TO_WA = "whatsapp:+917842507089"       # Your WhatsApp number

client = Client(TWILIO_SID, TWILIO_TOKEN)

# -------------------------------------------------------------
# Metabase Dashboard URL
# -------------------------------------------------------------
METABASE_DASHBOARD_URL = "http://metabase.rozana.tech/public/dashboard/321d8efc-fbb6-4319-afd2-0bf1d5c0bd33"

# -------------------------------------------------------------
# Function to format table data
# -------------------------------------------------------------
def create_pretty_table(cols, rows):
    """Create a pretty-printed, pipe-aligned table (Metabase style)."""
    if not cols or not rows:
        return "No data available"

    str_rows = [[str(val) if val is not None else "" for val in row] for row in rows]
    col_widths = [
        max(len(str(col)), *(len(row[i]) if i < len(row) else 0 for row in str_rows))
        for i, col in enumerate(cols)
    ]

    header = " | ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(cols))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(cols)))
    data_lines = []
    for row in str_rows:
        data_lines.append(" | ".join(row[i].ljust(col_widths[i]) for i in range(len(cols))))
    return "\n".join([header, separator] + data_lines)

# -------------------------------------------------------------
# Flask route — Metabase webhook receiver
# -------------------------------------------------------------
@app.route("/metabase-webhook", methods=["POST"])
def metabase_webhook():
    app.logger.info("Webhook triggered!")

    if not request.is_json:
        return jsonify({"error": "Expected JSON payload"}), 400

    data = request.get_json()
    app.logger.info("Received JSON: %s", data)

    message_lines = ["📊 *Metabase Notification*", "=" * 40, ""]

    question_name = data.get("data", {}).get("question_name")
    question_url = METABASE_DASHBOARD_URL  # Use your dashboard URL

    # Extract optional fields
    date = data.get("data", {}).get("date") or data.get("date")
    orders = data.get("data", {}).get("orders") or data.get("orders")
    time_val = data.get("data", {}).get("time") or data.get("time")

    # Add summary info
    if question_name:
        message_lines.append(f"*Table/Question:* {question_name}")
    if question_url:
        message_lines.append(f"*Dashboard:* {question_url}")
    if date:
        message_lines.append(f"*Date:* {date}")
    if orders:
        message_lines.append(f"*Orders:* {orders}")
    if time_val:
        message_lines.append(f"*Time:* {time_val}")

    message_lines.append("\nFirst 20 Records:\n")

    raw_data = data.get("data", {}).get("raw_data", {})
    cols = raw_data.get("cols", [])
    rows = raw_data.get("rows", [])

    if cols and rows:
        message_lines.append("```")
        pretty_table = create_pretty_table(cols, rows[:20])
        message_lines.append(pretty_table)
        message_lines.append("```")

        if len(rows) > 20:
            message_lines.append(f"⚠️ _Showing 20 of {len(rows)} records_")
        else:
            message_lines.append(f"📊 *Total Records:* {len(rows)}")
    else:
        message_lines.append("⚠️ No data records found.")

    message_lines.append("\n──────────────────────────────")
    message_lines.append("🤖 _Automated Metabase Alert_")

    msg_body = "\n".join(message_lines)
    app.logger.info("Message body composed:\n%s", msg_body)

    try:
        message = client.messages.create(from_=FROM_WA, to=TO_WA, body=msg_body)
        app.logger.info("✅ WhatsApp message sent! SID: %s", message.sid)
        return jsonify({"status": "sent", "sid": message.sid}), 200
    except Exception as e:
        app.logger.error("Error sending message: %s", e)
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------
# Function to send WhatsApp alert every 30 minutes
# -------------------------------------------------------------
def send_periodic_alert():
    """Send periodic dashboard link every 30 minutes."""
    try:
        msg = f"⏰ *30-Minute Metabase Update*\n\nCheck the dashboard here:\n{METABASE_DASHBOARD_URL}\n\n🤖 _Automated Alert_"
        message = client.messages.create(from_=FROM_WA, to=TO_WA, body=msg)
        app.logger.info("✅ Periodic alert sent! SID: %s", message.sid)
    except Exception as e:
        app.logger.error("❌ Error in periodic alert: %s", e)

# -------------------------------------------------------------
# Main entry — Run Flask + scheduler
# -------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting Metabase Webhook + Scheduler Server...")

    # Background scheduler to send every 30 min
    scheduler = BackgroundScheduler()
    scheduler.add_job(send_periodic_alert, "interval", minutes=30)
    scheduler.start()

    try:
        serve(app, host="0.0.0.0", port=5000)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



