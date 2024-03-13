import datetime
import random
import statistics
import time

import pandas as pd
import pyodbc
from pylogix import PLC
from datetime import date
from datetime import datetime
import datetime

connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                            'Server= TAPR105-PC\\SQLEXPRESS;'
                            'Trusted_Connection=yes;')

if connection:
    print("Connected Successfully")
else:
    print("Failed to connect")

cursor = connection.cursor()


def Insert_data(cursor, values):
    table_name = '[FINAL_REPORT].[dbo].[FINAL_REPORT_1]'

    columns = ['DATE', 'TIME', 'Coating_Batch', 'Slurry_Batch', 'WC_Number', 'Supplier', 'Customer', 'Customer_Part',
               'WC_loading_gp_Min_SV', 'PGM_Loading_gp_Min_SV',
               'PGM_Loading_gft3_Min_SV', 'WC_loading_gp_Max_SV', 'PGM_Loading_gp_Max_SV', 'PGM_Loading_gft3_Max_SV',
               'WC_loading_gp_Nom_SV', 'PGM_Loading_gp_Nom_SV', 'PGM_Loading_gft3_Nom_SV',
               'Substrate_VOL', 'LAYER', 'Part_No', 'WC_loading_gp_AV', 'PGM_Loading_gp_AV', 'PGM_Loading_gft3_AV',
               'Status']

    SQLCommand = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)"

    cursor.execute(SQLCommand, values)
    connection.commit()  # Commit changes to the database


def Fetch_data(Part_no):
    table_name = 'FINAL_REPORT.dbo.[FINAL_Report_1]'

    SQLCommand = "SELECT WC_loading_gp_AV FROM FINAL_REPORT.dbo.[FINAL_Report_1] WHERE Part_No = ?"

    cursor.execute(SQLCommand, Part_no)
    row = cursor.fetchone()
    if row is not None:
        Wc_loading_gp_Av = row[0]
        return Wc_loading_gp_Av
    else:
        return None  # Commit changes to the database


def Fetch_data2(Part_no):
    # table_name = 'FINAL_REPORT.dbo.[FINAL_Report_1]'

    SQLCommand = "SELECT PGM_Loading_gp_AV FROM FINAL_REPORT.dbo.[FINAL_Report_1] WHERE Part_No = ?"

    cursor.execute(SQLCommand, Part_no)
    row = cursor.fetchone()
    if row is not None:
        PGM_Loading_gp_AV = row[0]
        return PGM_Loading_gp_AV
    else:
        return None  # Commit changes to the database


def Fetch_data3(Part_no):
    table_name = 'FINAL_REPORT.dbo.[FINAL_Report_1]'

    SQLCommand = "SELECT PGM_Loading_gft3_AV FROM FINAL_REPORT.dbo.[FINAL_Report_1] WHERE Part_No = ?"

    cursor.execute(SQLCommand, Part_no)
    row = cursor.fetchone()
    if row is not None:
        PGM_Loading_gft3_AV = row[0]
        return PGM_Loading_gft3_AV
    else:
        return None  # Commit changes to the database


# def Update_data(cursor, values):
#     table_name = 'FINAL_REPORT.dbo.[FINAL_Report]'
#
#     columns = ['Date','Time','Coating_Batch', 'Slurry_Batch', 'WC_Number', 'Supplier', 'Customer', 'Customer_Part',
#                'WC_loading_gp_Min_SV', 'PGM_Loading_gp_Min_SV',
#                'PGM_Loading_gft3_Min_SV', 'WC_loading_gp_Max_SV', 'PGM_Loading_gp_Max_SV', 'PGM_Loading_gft3_Max_SV','WC_loading_gp_Nom_SV','PGM_Loading_gp_Nom_SV','PGM_Loading_gft3_Nom_SV',
#                'Substrate_VOL', 'LAYER', 'Part_No', 'WC_Loading_gp_AV', 'PGM_Loading_gp_AV', 'PGM_Loading_gft3_AV','Status']
#
#     SQLCommand = (f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? "
#                   f"?, ?,?,?,?,?,?,?)")
#
#     cursor.execute(SQLCommand, values)
#     connection.commit()
#
def update_data(cursor, a19, a20, a21, part_no):
    sql = """UPDATE FINAL_REPORT.dbo.[FINAL_REPORT_1]
                 SET WC_loading_gp_AV = ?,
                     PGM_Loading_gp_AV = ?,
                     PGM_Loading_gft3_AV = ?
                 WHERE Part_No =?"""
    cursor.execute(sql, (a19, a20, a21, part_no,))
    connection.commit()


def byte_string(value):
    if isinstance(value, bytes):
        start_index = 4
        try:
            end_index = value.index(b'\x00', start_index)
            c2 = value[start_index:end_index].decode('utf-8')
            return c2
        except ValueError:
            return ''
    elif isinstance(value, str):
        return value
    else:
        raise TypeError('Expected str or bytes, got {}'.format(type(value)))


while True:
    print("WAIT FOR CONNECTION")
    while True:
        with PLC() as comm:
            comm.IPAddress = '192.168.10.1'
            print("TRUE")
            Time = time.time()
            # print(Time.Value)
            # to read the data from PLc and write it in sql table
            FINAL_REPORT = comm.Read('Final_Report_Insert')

            if FINAL_REPORT.Value == None:
                break

            DATE = date.today()

            current_time = datetime.datetime.now()

            TIME = current_time.strftime('%H:%M:%S')

            Z1 = comm.Read('Insert_Final_Report.Coating_Batch')
            if Z1.Value == None:
                break
            Z2 = comm.Read('Insert_Final_Report.Slurry_Batch')
            if Z2.Value == None:
                break
            Z3 = comm.Read('Insert_Final_Report.WC_Number')
            if Z3.Value == None:
                break
            Z4 = comm.Read('Insert_Final_Report.Supplier')
            if Z4.Value == None:
                break
            Z5 = comm.Read('Insert_Final_Report.Customer')
            if Z5.Value == None:
                break
            Z6 = comm.Read('Insert_Final_Report.Customer_Part')
            if Z6.Value == None:
                break
            Z7 = comm.Read('Insert_Final_Report.WC_loading_gp_Min_SV')
            if Z7.Value == None:
                break
            Z8 = comm.Read('Insert_Final_Report.PGM_Loading_gp_Min_SV')
            if Z8.Value == None:
                break
            Z9 = comm.Read('Insert_Final_Report.PGM_Loading_gft3_Min_SV')
            if Z9.Value == None:
                break
            Z10 = comm.Read('Insert_Final_Report.WC_loading_gp_Max_SV')
            if Z10.Value == None:
                break
            Z11 = comm.Read('Insert_Final_Report.PGM_Loading_gp_Max_SV')
            if Z11.Value == None:
                break
            Z12 = comm.Read('Insert_Final_Report.PGM_Loading_gft3_Max_SV')
            if Z12.Value == None:
                break
            Z13 = comm.Read('Insert_Final_Report.WC_loading_gp_Nom_SV')
            if Z13.Value == None:
                break
            Z14 = comm.Read('Insert_Final_Report.PGM_Loading_gp_Nom_SV')
            if Z14.Value == None:
                break
            Z15 = comm.Read('Insert_Final_Report.PGM_Loading_gft3_Nom_SV')
            if Z15.Value == None:
                break
            Z16 = comm.Read('Insert_Final_Report.Substrate_VOL')
            if Z16.Value == None:
                break
            Z17 = comm.Read('Insert_Final_Report.LAYER')
            if Z17.Value == None:
                break
            Z18 = comm.Read('Insert_Final_Report.Part_No')
            if Z18.Value == None:
                break
            Z19 = comm.Read('Insert_Final_Report.WC_loading_gp_AV')
            if Z19.Value == None:
                break
            Z20 = comm.Read('Insert_Final_Report.PGM_Loading_gp_AV')
            if Z20.Value == None:
                break
            Z21 = comm.Read('Insert_Final_Report.PGM_Loading_gft3_AV')
            if Z21.Value == None:
                break
            Z22 = comm.Read('Insert_Final_Report.Status')
            if Z22.Value == None:
                break

            a1 = Z1.Value
            a2 = Z2.Value
            a3 = Z3.Value
            a4 = Z4.Value
            a5 = Z5.Value
            a6 = Z6.Value
            a7 = Z7.Value
            a8 = Z8.Value
            a9 = Z9.Value
            a10 = Z10.Value
            a11 = Z11.Value
            a12 = Z12.Value
            a13 = Z13.Value
            a14 = Z14.Value
            a15 = Z15.Value
            a16 = Z16.Value
            a17 = Z17.Value
            a18 = Z18.Value
            a19 = Z19.Value
            a20 = Z20.Value
            a21 = Z21.Value
            a22 = Z22.Value

            c1 = byte_string(a1)
            c2 = byte_string(a2)
            c3 = byte_string(a3)
            c4 = byte_string(a4)
            c5 = byte_string(a5)
            c6 = byte_string(a6)
            c18 = byte_string(a18)

            # print(type(c18))
            c22 = byte_string(a22)

            if FINAL_REPORT.Value == True:
                # Z1 = comm.Read('Report_To_Scada.Date')
                print('insert')
                values = (
                    DATE, TIME, c1, c2, c3, c4, c5, c6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, c18, a19,
                    a20,
                    a21, c22)

                Insert_data(cursor, values)
                comm.Write('Final_Report_Insert', False)

                print("FINAL_REPORT DATA INSERTED")

            # to fetch the data from the sql and write back to the PLC
            FOUND = comm.Read('Final_Report_Find')

            if FOUND.Value is None:
                break
            Part_No = comm.Read('Found_Report.Part_No')
            if FOUND.Value == True:
                print('Found')
                print(Part_No)
                Part_no_sql = Part_No.Value
                p_n = byte_string(Part_no_sql)
                print(p_n)
                df = Fetch_data(p_n)
                ds = Fetch_data2(p_n)
                da = Fetch_data3(p_n)

                # Extract data from the fetched results
                Wc_loading_gp = df
                print(Wc_loading_gp)
                print(type(Wc_loading_gp))
                PGM_Loading_gp_AV = ds
                print(PGM_Loading_gp_AV)
                PGM_Loading_gft3_AV = da
                print(PGM_Loading_gft3_AV)
                # Extracting the float value from the Pandas Series
                Z19 = comm.Write('Found_Report.WC_loading_gp_AV', Wc_loading_gp)
                if Z16.Value is None:
                    break
                Z20 = comm.Write('Found_Report.PGM_Loading_gp_AV', PGM_Loading_gp_AV)
                if Z17.Value is None:
                    break
                PGM_Loading_gft3_AV = PGM_Loading_gft3_AV
                Z21 = comm.Write('Found_Report.PGM_Loading_gft3_AV', PGM_Loading_gft3_AV)
                if Z18.Value is None:
                    break
                comm.Write('Final_Report_Find', False)

            # TO insert the updated data into the sql from PLC
            Update = comm.Read('Final_Report_Update')

            if Update.Value is None:
                break

            Z19 = comm.Read('Update_Report_Data.WC_loading_gp_AV')
            if Z19.Value == None:
                break
            Z20 = comm.Read('Update_Report_Data.PGM_Loading_gp_AV')
            if Z20.Value == None:
                break
            Z21 = comm.Read('Update_Report_Data.PGM_Loading_gft3_AV')
            if Z21.Value == None:
                break
            a19 = Z19.Value
            a20 = Z20.Value
            a21 = Z21.Value

            if Update.Value == True:
                print('Update')
                Part_no = Part_No.Value
                part_no = byte_string(Part_no)
                print(part_no)
                # values = (Z19.Value, Z20.Value, Z21.Value)
                update_data(cursor, a19, a20, a21, part_no)
                print("DATA_UPDATED")
                comm.Write('Final_Report_Update', False)

                print("DATA INSERTED")
