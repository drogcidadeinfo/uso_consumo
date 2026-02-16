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

def connect():
    creds_dict = json.loads(CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)

def read_responses_df(sh):
    ws = sh.worksheet(RESPONSES_SHEET_NAME)
    df = pd.DataFrame(ws.get_all_records())

    # Parse timestamp (Google Forms in PT-BR can vary; try robust parsing)
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

    # 1️⃣ Clear column B entirely (except header if needed)
    ws.batch_clear(["B1:B200"])  # adjust max row if needed

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

def read_previous_status():
    """Read previously updated sheets from status file"""
    try:
        status_file = Path("last_run_status.json")
        if status_file.exists():
            with open(status_file, 'r') as f:
                data = json.load(f)
                return set(data.get("updated_sheets", []))
    except Exception as e:
        print(f"Could not read previous status: {e}")
    return set()

def main():
    sh = connect()
    df = read_responses_df(sh)
    latest = filter_current_month_latest_per_email(df)

    if latest.empty:
        print("No submissions for current month.")
        print("UPDATED_SHEETS_JSON=[]")
        return

    # Read previously updated sheets
    previously_updated = read_previous_status()
    print(f"Previously updated sheets: {previously_updated}")

    updated_tabs = []
    newly_updated_tabs = []

    for _, row in latest.iterrows():
        email = row["Endereço de e-mail"].strip().lower()
        filial = EMAIL_TO_FILIAL.get(email)

        if not filial:
            print(f"Skipping: email not mapped -> {email}")
            continue

        submission = row.to_dict()
        update_filial_tab(sh, filial, submission)
        print(f"Updated {filial} from {email} ({submission.get('Carimbo de data/hora')})")

        # Track updated sheets
        if filial not in updated_tabs:
            updated_tabs.append(filial)
            
            # Check if this is newly updated
            if filial not in previously_updated:
                newly_updated_tabs.append(filial)

    # Output both lists
    print(f"UPDATED_SHEETS_JSON={json.dumps(updated_tabs)}")
    print(f"NEWLY_UPDATED_SHEETS_JSON={json.dumps(newly_updated_tabs)}")

if __name__ == "__main__":
    main()
