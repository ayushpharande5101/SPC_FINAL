import datetime
import random
import statistics
import time
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


def Report_ok(cursor, values):
    table_name = 'Control_chart.dbo.[REPORT_OK_1]'

    columns = ['Date','Time', 'Model_Name', 'Login_User', 'Coating_Batch_No', 'Part_Serial_No', 'Part_Scan_Data',
               'WC_Number', 'Solid_Percentage', 'Slurry_Batch_No', 'Layer', 'Face', 'Weight_Before_Coating_W1',
               'Weight_After_Coating_W2', 'Dose_Weight_W2_minus_W1', 'Result_Ok_NOk', 'PGM_Percentage',
               'PGM_gpc_Calculated_value', 'Washcoat_gl_Calculation', 'Washcoat_gpc_calculation', 'Dosing_Station']

    SQLCommand = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    cursor.execute(SQLCommand, values)
    connection.commit()  # Commit changes to the database


def Report_not_ok(cursor, values):
    table_name = 'Control_chart.dbo.[REPORT_NOT_OK_1]'

    columns = ['Date','time', 'Model_Name', 'Login_User', 'Coating_Batch_No', 'Part_Serial_No', 'Part_Scan_Data',
               'WC_Number', 'Solid_Percentage', 'Slurry_Batch_No', 'Layer', 'Face', 'Weight_Before_Coating_W1',
               'Weight_After_Coating_W2', 'Dose_Weight_W2_minus_W1', 'Result_Ok_NOk', 'PGM_Percentage',
               'PGM_gpc_Calculated_value', 'Washcoat_gl_Calculation', 'Washcoat_gpc_calculation', 'Dosing_Station']

    SQLCommand = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    cursor.execute(SQLCommand, values)
    connection.commit()  # Commit changes to the database


def Combine_data(cursor, values):
    table_name = 'Control_chart.dbo.[COMMON_REPORT_3]'

    columns = ['Date','time', 'Model_Name', 'Login_User', 'Coating_Batch_No', 'Part_Serial_No', 'Part_Scan_Data',
               'WC_Number', 'Solid_Percentage', 'Slurry_Batch_No', 'Layer', 'Face', 'Weight_Before_Coating_W1',
               'Weight_After_Coating_W2', 'Dose_Weight_W2_minus_W1', 'Result_Ok_NOk', 'PGM_Percentage',
               'PGM_gpc_Calculated_value', 'Washcoat_gl_Calculation', 'Washcoat_gpc_calculation', 'Dosing_Station','Job_Vacuum_A','Job_Vacuum','Wet_pick_up_nominal','Wet_pick_up_minimum','Wet_pick_up_maximum']

    SQLCommand = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?,?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)"

    cursor.execute(SQLCommand, values)
    connection.commit()  # Commit changes to the database


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

            REPORT_OK = comm.Read('SPC_REPORT_INSERT')

            Time1 = time.time()
            OVERALL_TIME = Time1 - Time
            print(OVERALL_TIME)

            if REPORT_OK.Value == None:
                break
            REPORT_NOT_OK = comm.Read('SPC_REPORT_INSERT_1')

            if REPORT_NOT_OK.Value == None:
                break

            COMBINE_DATA = comm.Read('SPC_COMMON_REPORT')

            if COMBINE_DATA.Value == None:
                break

            Z1 = date.today()
            current_time = datetime.datetime.now()
            # TIME = current_time.strftime('%H:%M')
            TIME = current_time.strftime('%H:%M:%S')

            print(Z1)

            print(TIME)
            Z2 = comm.Read('Report_To_Scada.Model_Name')
            if Z2.Value == None:
                break
            Z3 = comm.Read('User_Name')
            if Z3.Value == None:
                break
            Z4 = comm.Read('Report_To_Scada.Coating_Batch_No')
            if Z4.Value == None:
                break
            Z5 = comm.Read('Report_To_Scada.Part_Serial_No')
            if Z5.Value == None:
                break
            Z6 = comm.Read('Report_To_Scada.Part_Scan_Data')
            if Z6.Value == None:
                break
            Z7 = comm.Read('Report_To_Scada.WC_Number')
            if Z7.Value == None:
                break
            Z8 = comm.Read('Report_To_Scada.Solid_Percentage')
            if Z8.Value == None:
                break
            Z9 = comm.Read('Report_To_Scada.Slurry_Batch_No')
            if Z9.Value == None:
                break
            Z10 = comm.Read('Report_To_Scada.Layer')
            if Z10.Value == None:
                break
            Z11 = comm.Read('Report_To_Scada.Face')
            if Z11.Value == None:
                break
            Z12 = comm.Read('Report_To_Scada.Weight_Before_Coating_W1')
            if Z12.Value == None:
                break
            Z13 = comm.Read('Report_To_Scada.Weight_After_Coating_W2')
            if Z13.Value == None:
                break
            Z14 = comm.Read('Report_To_Scada.Dose_Weight_W2_minus_W1')
            if Z14.Value == None:
                break
            Z15 = comm.Read('Report_To_Scada.Result_Ok_NOk')
            if Z15.Value == None:
                break
            Z16 = comm.Read('Report_To_Scada.PGM_Percentage')
            if Z16.Value == None:
                break
            Z17 = comm.Read('Report_To_Scada.PGM_gpc_Calculated_value')
            if Z17.Value == None:
                break
            Z18 = comm.Read('Report_To_Scada.Washcoat_gl_Calculation')
            if Z18.Value == None:
                break
            Z19 = comm.Read('Report_To_Scada.Washcoat_gpc_calculation')
            if Z19.Value == None:
                break
            Z20 = comm.Read('Report_To_Scada.Dosing_Station')
            if Z20.Value == None:
                break
            Z21 = comm.Read('Report_To_Scada.Job_Vacuum_A')
            if Z21.Value == None:
                break
            Z22 = comm.Read('Report_To_Scada.Job_Vacuum_B')
            if Z22.Value == None:
                break
            Z23 = comm.Read('Recipe_Running.Wet_pick_up_nominal')
            if Z23.Value == None:
                break
            Z24 = comm.Read('Recipe_Running.Wet_pick_up_minimum')
            if Z24.Value == None:
                break
            Z25 = comm.Read('Recipe_Running.Wet_pick_up_maximum')
            if Z25.Value == None:
                break



            # a1 = Z1.Value
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
            a23 = Z23.Value
            a24 = Z24.Value
            a25 = Z25.Value



            c2 = byte_string(a2)
            c3 = byte_string(a3)
            c4 = byte_string(a4)
            c6 = byte_string(a6)
            c7 = byte_string(a7)
            c9 = byte_string(a9)
            c11 = byte_string(a11)
            c15 = byte_string(a15)

            if REPORT_NOT_OK.Value == True:
                values = (Z1,TIME, c2, c3, c4, a5, c6, c7, a8, c9, a10, c11, a12, a13, a14, c15, a16, a17, a18, a19, a20)

                Report_not_ok(cursor, values)

                comm.Write('SPC_REPORT_INSERT_1', False)

                print("DATA INSERTED")

            if REPORT_OK.Value == True:
                # Z1 = comm.Read('Report_To_Scada.Date')

                values = (Z1,TIME, c2, c3, c4, a5, c6, c7, a8, c9, a10, c11, a12, a13, a14, c15, a16, a17, a18, a19, a20)

                Report_ok(cursor, values)
                comm.Write('SPC_REPORT_INSERT', False)

                print("DATA INSERTED")

            if COMBINE_DATA.Value == True:
                values = (Z1,TIME, c2, c3, c4, a5, c6, c7, a8, c9, a10, c11, a12, a13, a14, c15, a16, a17, a18, a19, a20, a21, a22, a23, a24, a25)

                Combine_data(cursor, values)
                comm.Write('SPC_COMMON_REPORT', False)

                print("DATA INSERTED")

