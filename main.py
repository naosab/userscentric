import gspread
import pandas
from dbhpgm.dbConfig import dbConnector
from gspread.models import Spreadsheet
from oauth2client.service_account import ServiceAccountCredentials

from functions import *
from input import *
from queries import *

# Query 1 connect to the gsheet - conversion rate

if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_conversionrate, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'conversionrate')


# Query 2 connect to the gsheet - nbr of sessions per visitor

if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_nbrofsessionspervisitor, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'nbrofsessionspervisitor')
   print(f'Complete, go to https://docs.google.com/spreadsheets/d/{sheet_id}/edit?usp=sharing')

# Query 3 connect to the gsheet - nbr of sessions before converting

if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_nbrofsessionsbeforeconverting, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'nbrofsessionsbeforeconverting')
   print(f'Complete, go to https://docs.google.com/spreadsheets/d/{sheet_id}/edit?usp=sharing')

# Query 4 connect to the gsheet - nextsessionreturnrate
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_nextsessionreturnrate, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'nextsessionreturnrate')
   print(f'daydiffebetweentwosessions completed')

# Query 5 connect to the gsheet - daydiffebetweentwosessions
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_daydiffebetweentwosessions, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'daydiffebetweentwosessions')
   print(f'daydiffebetweentwosessions completed')


# Query 6 connect to the gsheet - hourdiffebetweentwosessions
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_hourdiffebetweentwosessions, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'hourdiffebetweentwosessions')
   print(f'hourdiffebetweentwosessions completed')

# Query 7 connect to the gsheet - boucereturners
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_boucereturners, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'boucereturners')
   print(f'boucereturners completed')


#Page level

# Query 8 connect to the gsheet - nbr of sessions per visitors page level
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_nbrofsessionspervisitorpagelevel, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'nbrofsessionspervisitorpagelevel')
   print(f'nbrofsessionspervisitorpagelevel completed')

   # if __name__ == "__main__":
   #     result_db = get_query_results(QUERY=query_avgvisitonthepagebeforeconverting, project_id=project_id,
   #                                   page_condition=page_condition,
   #                                   start_date=start_date, end_date=end_date,
   #                                   device_id=device_id)
   #     GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   #     write_gsheet(GSHEET_CONN, result_db, 'avgvisitonthepagebeforeconverting')
   #     print(f'avgvisitonthepagebeforeconverting completed')

# Query 9 connect to the gsheet - nbr of visits on the page before making a transaction to do
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_nbrofsessionspervisitorpagelevel, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'nbrofsessionspervisitorpagelevel')
   print(f'nbrofsessionspervisitorpagelevel completed')

# Query 10 connect to the gsheet - nbr of visits on the page before making a transaction to do
   if __name__ == "__main__":
       result_db = get_query_results(QUERY=query_avgvisitonthepagebeforeconverting, project_id=project_id,
                                     page_condition=page_condition,
                                     start_date=start_date, end_date=end_date,
                                     device_id=device_id)
       GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
       write_gsheet(GSHEET_CONN, result_db, 'avgvisitonthepagebeforeconverting')
       print(f'avgvisitonthepagebeforeconverting completed')



# Query 9 connect to the gsheet - returonthesiteafterexitingonapage
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_returonthesiteafterexitingonapage, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'returonthesiteafterexitingonapage')
   print(f'returonthesiteafterexitingonapage completed')

   # Query 9 connect to the gsheet - dayreturonthesiteafterexitingonapage
   if __name__ == "__main__":
       result_db = get_query_results(QUERY=query_dayreturonthesiteafterexitingonapage, project_id=project_id,
                                     page_condition=page_condition,
                                     start_date=start_date, end_date=end_date,
                                     device_id=device_id)
       GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
       write_gsheet(GSHEET_CONN, result_db, 'dayreturonthesiteafterexitingonapage')
       print(f'dayreturonthesiteafterexitingonapage completed')



# Query 10 connect to the gsheet - nbr of sessions per visitors page level
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_boucerspagelevelcomingbackonthesite, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'boucerspagelevelcomingbackonthesite')
   print(f'boucerspagelevelcomingbackonthesite completed')


# Query 11 connect to the gsheet - nbr of sessions per visitors page level
if __name__ == "__main__":
   result_db = get_query_results(QUERY=query_boucerspagelevelcomingbackonthesameurl, project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, end_date=end_date,
                                 device_id=device_id)
   GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
   write_gsheet(GSHEET_CONN, result_db, 'boucerspagelevelcomingbackonthesameurl')
   print(f'boucerspagelevelcomingbackonthesameurl completed')




