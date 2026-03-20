import os, json
import pandas as pd
import gspread
import time
import re
from google.oauth2.service_account import Credentials
from datetime import datetime

SHEET_ID = os.getenv("SHEET_ID")
CREDS_JSON = os.getenv("GSA_CREDENTIALS")  

RESPONSES_SHEET_NAME = "Respostas ao formulário 1"  

EMAIL_MAP_JSON = os.getenv("EMAIL_TO_FILIAL")

if not EMAIL_MAP_JSON:
    raise ValueError("EMAIL_TO_FILIAL secret not set")

EMAIL_TO_FILIAL = json.loads(EMAIL_MAP_JSON)

# Rate limiting delay (in seconds)
API_DELAY = 0.5  # Half second between API calls

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

def parse_date_from_cell(value):
    """
    Parse date from cell value that could be:
    - datetime object
    - string like "12/03/2026 11:33:45"
    - string like "12/03/2026"
    - string like "2026-03-12"
    """
    if not value:
        return None
    
    # If it's already a datetime object
    if isinstance(value, datetime):
        return value
    
    # If it's a string
    if isinstance(value, str):
        # Try different date formats
        formats = [
            "%d/%m/%Y %H:%M:%S",  # 12/03/2026 11:33:45
            "%d/%m/%Y",           # 12/03/2026
            "%Y-%m-%d",           # 2026-03-12
            "%d/%m/%y",           # 12/03/26
            "%Y-%m-%d %H:%M:%S",  # 2026-03-12 11:33:45
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        
        # Try using regex to extract date if formats fail
        # Look for pattern DD/MM/YYYY
        match = re.search(r'(\d{2})/(\d{2})/(\d{4})', value)
        if match:
            day, month, year = match.groups()
            try:
                return datetime(int(year), int(month), int(day))
            except ValueError:
                pass
    
    return None

def check_filiais_already_updated_batch(sh, filiais, current_month, current_year):
    """
    Batch check B1 for all filiais at once to minimize API calls
    Returns dict of filial_name -> bool (True if already updated)
    """
    result = {}
    
    # Process in small batches to avoid rate limits
    batch_size = 5
    for i in range(0, len(filiais), batch_size):
        batch = filiais[i:i+batch_size]
        
        for filial in batch:
            try:
                ws = sh.worksheet(filial)
                b1_value = ws.cell(1, 2).value  # Row 1, Column 2 (B1)
                
                if not b1_value:
                    print(f"DEBUG: {filial} B1 is empty")
                    result[filial] = False
                    continue
                
                # Parse the date from B1
                b1_date = parse_date_from_cell(b1_value)
                
                if b1_date:
                    already_updated = (b1_date.year == current_year and b1_date.month == current_month)
                    if already_updated:
                        print(f"DEBUG: {filial} B1 value '{b1_value}' parsed to {b1_date.strftime('%d/%m/%Y')} - ALREADY UPDATED")
                    else:
                        print(f"DEBUG: {filial} B1 value '{b1_value}' parsed to {b1_date.strftime('%d/%m/%Y')} - NEEDS UPDATE")
                    result[filial] = already_updated
                else:
                    print(f"DEBUG: {filial} Could not parse B1 value: '{b1_value}'")
                    result[filial] = False  # Assume not updated if we can't parse
                
                # Small delay to avoid rate limits
                time.sleep(API_DELAY)
                
            except Exception as e:
                print(f"Error checking {filial}: {e}")
                result[filial] = False  # Assume not updated on error
        
        # Extra delay between batches
        if i + batch_size < len(filiais):
            time.sleep(API_DELAY * 2)
    
    return result

def update_filial_tab(sh, filial_name, submission_row_dict):
    ws = sh.worksheet(filial_name)

    # Clear column B entirely (except header if needed)
    ws.batch_clear(["B1:B200"])
    time.sleep(API_DELAY)  # Delay after clear

    label_to_row = build_label_row_map(ws)
    time.sleep(API_DELAY)  # Delay after reading column A

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
        time.sleep(API_DELAY)  # Delay after update

def main():
    sh = connect()
    df = read_responses_df(sh)
    latest = filter_current_month_latest_per_email(df)

    if latest.empty:
        print("No submissions for current month.")
        print("NEW_FILIAIS_JSON=[]")
        return

    now = datetime.now()
    current_month = now.month
    current_year = now.year
    
    # Get unique filiais from submissions
    unique_filiais = set()
    for _, row in latest.iterrows():
        email = row["Endereço de e-mail"].strip().lower()
        filial = EMAIL_TO_FILIAL.get(email)
        if filial:
            unique_filiais.add(filial)
    
    print(f"Found {len(unique_filiais)} unique filiais in submissions")
    
    # Batch check all filiais at once
    updated_status = check_filiais_already_updated_batch(sh, list(unique_filiais), current_month, current_year)
    
    # Process only filiais that need updating
    new_filiais = []
    updated_tabs = []
    
    for _, row in latest.iterrows():
        email = row["Endereço de e-mail"].strip().lower()
        filial = EMAIL_TO_FILIAL.get(email)

        if not filial:
            print(f"Skipping: email not mapped -> {email}")
            continue

        # Check if this filial needs updating
        if updated_status.get(filial, False):
            print(f"SKIPPING: {filial} already updated for {current_month}/{current_year}")
            continue
        
        # Process update
        try:
            submission = row.to_dict()
            update_filial_tab(sh, filial, submission)
            
            # After updating, add current date and time to B1 (keep existing format)
            current_datetime_str = now.strftime("%d/%m/%Y %H:%M:%S")
            filial_ws = sh.worksheet(filial)
            filial_ws.update_cell(1, 2, current_datetime_str)
            time.sleep(API_DELAY)
            
            print(f"UPDATED: {filial} from {email} ({submission.get('Carimbo de data/hora')})")

            # Track updated sheets
            if filial not in updated_tabs:
                updated_tabs.append(filial)
                new_filiais.append(filial)
                
        except Exception as e:
            print(f"ERROR updating {filial}: {e}")
            continue

    # Output results
    print(f"UPDATED_SHEETS_JSON={json.dumps(updated_tabs)}")
    print(f"NEW_FILIAIS_JSON={json.dumps(new_filiais)}")
    
    if new_filiais:
        print(f"Will send emails for {len(new_filiais)} filiais: {new_filiais}")
    else:
        print("No new updates needed - all filiais already have current month data")

if __name__ == "__main__":
    main()
