import os
import json
import base64
import requests
import pytz
import time  # Add this import
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from google.auth.transport.requests import Request

import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ======================
# ENV VARIABLES
# ======================

SHEET_ID = os.getenv("SHEET_ID")
CREDS_JSON = os.getenv("GSA_CREDENTIALS")

NEW_FILIAIS_JSON = os.getenv("NEW_FILIAIS_JSON", "[]")  # Receives filiais to email

EMAIL_TO = os.getenv("EMAIL_TO")
GMAIL_SENDER = os.getenv("GMAIL_SENDER")  

OUTPUT_DIR = Path("pdf_out")
OUTPUT_DIR.mkdir(exist_ok=True)

# Rate limiting - delay between PDF exports (in seconds)
PDF_EXPORT_DELAY = 2  # Adjust if needed


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

def export_sheet_to_pdf(spreadsheet_id, gid, creds, out_path, retry_count=3):
    session = requests.Session()
    creds.refresh(Request())
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
    
    for attempt in range(retry_count):
        try:
            r = session.get(url, params=params)
            r.raise_for_status()
            out_path.write_bytes(r.content)
            return  # Success, exit function
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < retry_count - 1:
                # Rate limited - wait and retry
                wait_time = (attempt + 1) * 5  # Progressive backoff: 5s, 10s, 15s
                print(f"Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{retry_count}")
                time.sleep(wait_time)
            else:
                # Other error or last retry failed
                raise


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
    new_filiais = json.loads(NEW_FILIAIS_JSON)
    if not new_filiais:
        print("No new filiais to export.")
        return

    print(f"Processing {len(new_filiais)} filiais: {new_filiais}")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/gmail.send",
    ]

    creds = get_delegated_credentials(scopes)

    sh = gspread.authorize(creds).open_by_key(SHEET_ID)

    exported_files = []
    # Date for e-mail title
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d")
    
    # Date/time for e-mail body
    local_tz = pytz.timezone('America/Sao_Paulo')  # Or 'Etc/GMT+3'
    now_local = datetime.now(local_tz)
    br_time = now_local.strftime("%d/%m/%Y %H:%M")

    for i, filial_name in enumerate(new_filiais):
        ws = sh.worksheet(filial_name)
        gid = ws.id

        # Include date in filename
        out_path = OUTPUT_DIR / f"{filial_name}_{now_str}.pdf"
        print(f"Exporting {filial_name} ({i+1}/{len(new_filiais)})...")
        
        try:
            export_sheet_to_pdf(SHEET_ID, gid, creds, out_path)
            exported_files.append(str(out_path))
            
            # Add delay between exports to avoid rate limiting (except for last item)
            if i < len(new_filiais) - 1:
                print(f"Waiting {PDF_EXPORT_DELAY}s before next export...")
                time.sleep(PDF_EXPORT_DELAY)
                
        except Exception as e:
            print(f"Failed to export {filial_name}: {e}")
            # Continue with other filiais

    if not exported_files:
        print("No PDFs were successfully exported. Skipping email.")
        return

    # Create email subject and body
    subject = f"Uso/Consumo - Novas Submissões ({now_str})"
    
    if len(new_filiais) == 1:
        body = f"Uma nova submissão foi recebida para a filial: {new_filiais[0]}.\n\nPDF em anexo.\n\nData/Hora: {br_time}"
    else:
        successful_filiais = [Path(f).stem.split('_')[0] for f in exported_files]
        body = f"Novas submissões recebidas para {len(exported_files)} filiais: {', '.join(successful_filiais)}.\n\nPDFs em anexo.\n\nData/Hora: {br_time}"

    print("Sending email with new submissions...")
    try:
        send_email_with_attachments(creds, exported_files, subject, body)
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
