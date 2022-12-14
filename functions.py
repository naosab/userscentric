from dbhpgm.dbConfig import dbConnector

# Connect to CH and get query result
def get_query_results(QUERY, project_id, page_condition, start_date, end_date, device_id):
   engineCH = dbConnector('chatamart', project_id=project_id)
   result_db = engineCH.getSQLResult(QUERY.format(project_id=project_id,
                                                  page_condition=page_condition,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  device_id=device_id))
   return result_db

import gspread
import pandas
from gspread.models import Spreadsheet
from oauth2client.service_account import ServiceAccountCredentials

# Connect to gsheet
def connect_gsheet(sheet_id):
   scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
   creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
   gc = gspread.authorize(creds)
   return gc.open_by_key(sheet_id)

# Write gsheet
def write_gsheet(GSHEET_CONN, query_result, tab_name):
   wks = GSHEET_CONN.worksheet(tab_name)  # tab name
   wks.clear()
   wks.update([query_result.columns.values.tolist()] + query_result.values.tolist())
