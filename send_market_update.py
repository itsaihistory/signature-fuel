"""
Send Jet Fuel Market Update Email — Signature Energy

Usage:
  python send_market_update.py                    # morning update, print to console
  python send_market_update.py --send             # morning update, send email
  python send_market_update.py --time midday      # midday update
  python send_market_update.py --time afternoon   # afternoon update
  python send_market_update.py --preview          # save HTML preview to outputs/

Environment variables needed for email:
  GMAIL_ADDRESS   — sender Gmail address (e.g. parker9gordon@gmail.com)
  GMAIL_APP_PASS  — Gmail app password (NOT regular password)
  MARKET_UPDATE_TO — comma-separated recipient list (default: sender)
"""

import sys
import os
import logging
import argparse
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.reports.market_update import MarketUpdateGenerator
from config.settings import OUTPUT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def send_email(html_body: str, text_body: str, time_of_day: str):
    """Send the market update via Gmail SMTP."""
    gmail_addr = os.environ.get("GMAIL_ADDRESS", "parker9gordon@gmail.com")
    gmail_pass = os.environ.get("GMAIL_APP_PASS", "")

    if not gmail_pass:
        logger.error(
            "GMAIL_APP_PASS not set. Generate an app password at:\n"
            "  Google Account > Security > 2-Step Verification > App passwords\n"
            "Then: set GMAIL_APP_PASS=your_app_password"
        )
        return False

    recipients_str = os.environ.get("MARKET_UPDATE_TO", gmail_addr)
    recipients = [r.strip() for r in recipients_str.split(",")]

    label = {"morning": "Morning Briefing", "midday": "Midday Update",
             "afternoon": "Afternoon Close"}.get(time_of_day, "Market Update")
    today = datetime.now().strftime("%m/%d")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Signature Energy — Jet Fuel {label} ({today})"
    msg["From"] = f"Signature Fuel Intelligence <{gmail_addr}>"
    msg["To"] = ", ".join(recipients)

    # Attach plain text and HTML (email clients prefer HTML when available)
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_addr, gmail_pass)
            server.sendmail(gmail_addr, recipients, msg.as_string())
        logger.info(f"Email sent to: {', '.join(recipients)}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Signature Energy Market Update")
    parser.add_argument("--time", choices=["morning", "midday", "afternoon"],
                        default="morning", help="Time of day variant")
    parser.add_argument("--send", action="store_true",
                        help="Actually send the email (otherwise just preview)")
    parser.add_argument("--preview", action="store_true",
                        help="Save HTML preview to outputs/")
    args = parser.parse_args()

    generator = MarketUpdateGenerator()

    print(f"\nGenerating {args.time} market update...\n")

    # Generate both formats
    text_body = generator.generate(time_of_day=args.time)
    html_body = generator.generate_html(time_of_day=args.time)

    # Always print plain text to console
    try:
        print(text_body)
    except UnicodeEncodeError:
        print(text_body.encode("ascii", errors="replace").decode("ascii"))
    print()

    # Save HTML preview
    if args.preview or not args.send:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        preview_path = OUTPUT_DIR / f"market_update_{args.time}_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
        preview_path.write_text(html_body, encoding="utf-8")
        print(f"\nHTML preview saved: {preview_path}")

    # Send email
    if args.send:
        success = send_email(html_body, text_body, args.time)
        if success:
            print("\nEmail sent successfully!")
        else:
            print("\nEmail failed -- check logs above.")
            sys.exit(1)


if __name__ == "__main__":
    main()
