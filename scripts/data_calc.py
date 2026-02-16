import os, json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from pathlib import Path

SHEET_ID = os.getenv("SHEET_ID")
CREDS_JSON = os.getenv("GSA_CREDENTIALS")  

RESPONSES_SHEET_NAME = "Respostas ao formulário 1"  

EMAIL_MAP_JSON = os.getenv("EMAIL_TO_FILIAL")

if not EMAIL_MAP_JSON:
    raise ValueError("EMAIL_TO_FILIAL secret not set")

EMAIL_TO_FILIAL = json.loads(EMAIL_MAP_JSON)

# File to track processed emails
PROCESSED_EMAILS_FILE = Path("processed_emails.json")

def connect():
    creds_dict = json.loads(CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)

def read_responses_df(sh):
    ws = sh.worksheet(RESPONSES_SHEET_NAME)
    df = pd.DataFrame(ws.get_all_records())

    # Parse timestamp
    df["Carimbo de data/hora"] = pd.to_datetime(
        df["Carimbo de data/hora"],
        errors="coerce",
        dayfirst=True
    )
    return df

def filter_current_month_latest_per_email(df, now=None):
    if now is None:
        now = datetime.now()

    df = df.dropna(subset=["Carimbo de data/hora", "Endereço de e-mail"]).copy()
    df["year"] = df["Carimbo de data/hora"].dt.year
    df["month"] = df["Carimbo de data/hora"].dt.month

    df_month = df[(df["year"] == now.year) & (df["month"] == now.month)].copy()
    if df_month.empty:
        return df_month

    # keep latest submission per email
    df_month.sort_values("Carimbo de data/hora", inplace=True)
    latest = df_month.groupby("Endereço de e-mail", as_index=False).tail(1)
    return latest

def build_label_row_map(filial_ws):
    # Read column A (labels) until it runs out
    col_a = filial_ws.col_values(1)  # A
    # Map "label text" -> row number
    label_to_row = {}
    for i, label in enumerate(col_a, start=1):
        label = (label or "").strip()
        if label:
            label_to_row[label] = i
    return label_to_row

def update_filial_tab(sh, filial_name, submission_row_dict):
    ws = sh.worksheet(filial_name)

    # Clear column B entirely (except header if needed)
    ws.batch_clear(["B1:B200"])

    label_to_row = build_label_row_map(ws)

    updates = []

    for col_name, value in submission_row_dict.items():
        label = (col_name or "").strip()
        if label in label_to_row:
            row_idx = label_to_row[label]
            updates.append((row_idx, 2, "" if pd.isna(value) else str(value)))

    if updates:
        cell_list = ws.range(min(r for r, c, v in updates), 2,
                             max(r for r, c, v in updates), 2)

        cell_map = {cell.row: cell for cell in cell_list}

        for r, c, v in updates:
            if r in cell_map:
                cell_map[r].value = v

        ws.update_cells(cell_list, value_input_option="USER_ENTERED")

def load_processed_emails():
    """Load the set of emails that have already been processed"""
    try:
        if PROCESSED_EMAILS_FILE.exists():
            with open(PROCESSED_EMAILS_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get("processed_emails", []))
    except Exception as e:
        print(f"Could not load processed emails: {e}")
    return set()

def save_processed_emails(emails):
    """Save the set of processed emails"""
    try:
        with open(PROCESSED_EMAILS_FILE, 'w') as f:
            json.dump({"processed_emails": list(emails)}, f, indent=2)
    except Exception as e:
        print(f"Could not save processed emails: {e}")

def main():
    sh = connect()
    df = read_responses_df(sh)
    latest = filter_current_month_latest_per_email(df)

    if latest.empty:
        print("No submissions for current month.")
        print("PROCESSED_EMAILS_JSON=[]")
        print("NEW_EMAILS_JSON=[]")
        return

    # Load previously processed emails
    processed_emails = load_processed_emails()
    print(f"Previously processed emails: {processed_emails}")

    # Get all emails from current submissions
    current_emails = set()
    for _, row in latest.iterrows():
        email = row["Endereço de e-mail"].strip().lower()
        filial = EMAIL_TO_FILIAL.get(email)
        if filial:  # Only track emails that have a valid mapping
            current_emails.add(email)

    # Find new emails (not in processed_emails)
    new_emails = current_emails - processed_emails
    print(f"New emails found: {new_emails}")

    # Process all submissions (update sheets)
    updated_tabs = []
    processed_this_run = set()

    for _, row in latest.iterrows():
        email = row["Endereço de e-mail"].strip().lower()
        filial = EMAIL_TO_FILIAL.get(email)

        if not filial:
            print(f"Skipping: email not mapped -> {email}")
            continue

        submission = row.to_dict()
        update_filial_tab(sh, filial, submission)
        print(f"Updated {filial} from {email} ({submission.get('Carimbo de data/hora')})")

        # Track updated sheets (for logging)
        if filial not in updated_tabs:
            updated_tabs.append(filial)
        
        # Track processed emails
        processed_this_run.add(email)

    # Save all processed emails (including previous ones)
    all_processed = processed_emails | processed_this_run
    save_processed_emails(all_processed)

    # For new emails, get their corresponding filiais
    new_filiais = []
    for email in new_emails:
        filial = EMAIL_TO_FILIAL.get(email)
        if filial and filial not in new_filiais:
            new_filiais.append(filial)

    # Output results
    print(f"UPDATED_SHEETS_JSON={json.dumps(updated_tabs)}")
    print(f"NEW_EMAILS_JSON={json.dumps(list(new_emails))}")
    print(f"NEW_FILIAIS_JSON={json.dumps(new_filiais)}")

if __name__ == "__main__":
    main()
