import os
import json
import base64
import requests
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage

import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ======================
# ENV VARIABLES
# ======================

SHEET_ID = os.getenv("SHEET_ID")
CREDS_JSON = os.getenv("GSA_CREDENTIALS")

UPDATED_SHEETS_JSON = os.getenv("UPDATED_SHEETS_JSON", "[]")

EMAIL_TO = os.getenv("EMAIL_TO")
GMAIL_SENDER = os.getenv("GMAIL_SENDER")  

OUTPUT_DIR = Path("pdf_out")
OUTPUT_DIR.mkdir(exist_ok=True)


# ======================
# AUTH
# ======================

def get_delegated_credentials(scopes):
    if not CREDS_JSON:
        raise ValueError("GSA_CREDENTIALS not set")

    creds_dict = json.loads(CREDS_JSON)

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=scopes
    )

    # Impersonate real org user
    delegated = creds.with_subject(GMAIL_SENDER)
    return delegated


# ======================
# EXPORT PDF
# ======================

def export_sheet_to_pdf(spreadsheet_id, gid, creds, out_path):
    session = requests.Session()
    creds.refresh(requests.Request())
    session.headers.update({"Authorization": f"Bearer {creds.token}"})

    params = {
        "format": "pdf",
        "gid": str(gid),
        "portrait": "true",
        "fitw": "true",
        "scale": "4",
        "sheetnames": "false",
        "gridlines": "false",
        "printtitle": "false",
        "pagenumbers": "false",
    }

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
    r = session.get(url, params=params)
    r.raise_for_status()

    out_path.write_bytes(r.content)


# ======================
# SEND EMAIL VIA GMAIL API
# ======================

def send_email_with_attachments(creds, file_paths, subject, body):
    service = build("gmail", "v1", credentials=creds)

    msg = EmailMessage()
    msg["To"] = EMAIL_TO
    msg["From"] = GMAIL_SENDER
    msg["Subject"] = subject
    msg.set_content(body)

    for path in file_paths:
        data = Path(path).read_bytes()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=Path(path).name,
        )

    raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    service.users().messages().send(
        userId="me",
        body={"raw": raw_message}
    ).execute()


# ======================
# MAIN
# ======================

def main():
    updated_sheets = json.loads(UPDATED_SHEETS_JSON)
    if not updated_sheets:
        print("No sheets to export.")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/gmail.send",
    ]

    creds = get_delegated_credentials(scopes)

    sh = gspread.authorize(creds).open_by_key(SHEET_ID)

    exported_files = []
    now_str = datetime.now().strftime("%Y-%m-%d")

    for tab_name in updated_sheets:
        ws = sh.worksheet(tab_name)
        gid = ws.id

        out_path = OUTPUT_DIR / f"{tab_name}.pdf"
        print(f"Exporting {tab_name}...")
        export_sheet_to_pdf(SHEET_ID, gid, creds, out_path)
        exported_files.append(str(out_path))

    # subject = f"Uso/Consumo - PDFs ({now_str})"
    subject = f"e-mail teste"
    # body = f"Segue em anexo os PDFs das filiais: {', '.join(updated_sheets)}."
    body = f"esse e-mail é um teste"

    print("Sending email...")
    send_email_with_attachments(creds, exported_files, subject, body)

    print("Done.")


if __name__ == "__main__":
    main()
