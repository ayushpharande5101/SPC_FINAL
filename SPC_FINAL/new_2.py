import time
import numpy as np
import pandas as pd
import streamlit as st
from PIL import Image
import pyodbc
import matplotlib.pyplot as plt
from io import BytesIO
import datetime

st.set_page_config(page_title="SPC chart", layout='wide')
# inserting an image as a logo
dash_logo = "CTPL1.png"  # need changes
l_width = 200
l_height = 200
img = Image.open(dash_logo)
img.thumbnail((l_width, l_height))


# Define global variables

def connection():  # sql server connection changes needed
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server= TAPR105-PC\\SQLEXPRESS;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    return cursor


def chart(limit, ymin_limit, ymax_limit):
    global point_color
    plt.figure(figsize=(11, 5.5))
    limit_n = int(limit)
    if selected_chart_type == 'SPC Chart':
        sql1 = "SELECT SR_NO,DATETIME,BATCH,FINAL_WEIGHT,UCL,LCL,CL,USL,LSL FROM Control_chart.dbo.SOLO_CHART "
        curs1 = connection().execute(sql1)
        rows1 = curs1.fetchall()
        columns1 = [column[0] for column in curs1.description]
        ds = pd.DataFrame.from_records(rows1, columns=columns1)
        ds = ds.sort_values(by='SR_NO')
        curs1.close()
        UCL = ds['UCL'].iloc[-1]
        LCL = ds['LCL'].iloc[-1]
        USL = ds['USL'].iloc[-1]
        LSL = ds['LSL'].iloc[-1]
        CL = ds['CL'].iloc[-1]
        plot_data = []
        for i in range(limit_n):
            last_row = ds.iloc[-(i + 1)]
            if float(ymin_limit) < last_row['FINAL_WEIGHT'] < float(ymax_limit):
                plt.text(last_row['SR_NO'], last_row['FINAL_WEIGHT'],
                         f'{last_row["FINAL_WEIGHT"]:.2f}', fontsize=8, ha='center', va='bottom')
                point_color = 'red' if (last_row['FINAL_WEIGHT'] > UCL or last_row['FINAL_WEIGHT'] < LCL) else '#1476b8'
                plt.scatter(last_row['SR_NO'], last_row['FINAL_WEIGHT'], color=point_color, zorder=17)
                plot_data.append((last_row['SR_NO'], last_row['FINAL_WEIGHT']))
            plt.axhline(y=CL, color='g', linestyle='-', label='CL')
            plt.axhline(y=UCL, color='r', linestyle='--', label='UCL')
            plt.axhline(y=LCL, color='r', linestyle='--', label='LCL')
            plt.axhline(y=USL, color='y', linestyle='--', label='USL')
            plt.axhline(y=LSL, color='y', linestyle='--', label='LSL')
            for label, value in [('UCL', UCL), ('LCL', LCL), ('CL', CL), ('USL', USL), ('LSL', LSL)]:
                plt.annotate(label, xy=(ds['SR_NO'].iloc[-1], value), xytext=(ds['SR_NO'].iloc[-1] + 1, value),
                             color='black', fontsize=10, ha='left', va='center')
            plot_data.append((last_row['SR_NO'], last_row['FINAL_WEIGHT']))
        # Plot all points at once
        plt.plot(*zip(*plot_data), marker='o', linestyle='-')
        plt.title('SPC Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.ylim(float(ymin_limit), float(ymax_limit))
    elif selected_chart_type == 'X Chart':
        sql1 = ("SELECT SR_NO,DATETIME,BATCH,1,2,3,4,5,X_MAX,X_MIN,AVERAGE,RANGE,OVERALL_AVERAGE,OVERALL_RANGE,UCL_X,"
                " LCL_X FROM Control_chart.dbo.X_CHART")
        curs1 = connection().execute(sql1)
        rows1 = curs1.fetchall()
        columns1 = [column[0] for column in curs1.description]
        ds = pd.DataFrame.from_records(rows1, columns=columns1)
        ds = ds.sort_values(by='SR_NO')
        curs1.close()
        plot_data = []
        UCL_X = ds['UCL_X'].iloc[-1]
        LCL_X = ds['LCL_X'].iloc[-1]
        CL = 454.03155
        for i in range(limit_n):
            last_row = ds.iloc[-(i + 1)]
            if float(ymin_limit) < last_row['AVERAGE'] < float(ymax_limit):
                plt.text(last_row['SR_NO'], last_row['AVERAGE'],
                         f'{last_row["AVERAGE"]:.2f}', fontsize=8, ha='center', va='bottom')
                point_color = 'red' if (last_row['AVERAGE'] > UCL_X or last_row['AVERAGE'] < LCL_X) else '#1476b8'
                plt.scatter(last_row['SR_NO'], last_row['AVERAGE'], color=point_color, zorder=5)
                plot_data.append((last_row['SR_NO'], last_row['AVERAGE']))
            plt.axhline(y=CL, color='g', linestyle='-', label='CL')
            plt.axhline(y=UCL_X, color='r', linestyle='--', label='UCL')
            plt.axhline(y=LCL_X, color='r', linestyle='--', label='LCL')
            for label, value in [('UCL', UCL_X), ('LCL', LCL_X), ('CL', CL)]:
                plt.annotate(label, xy=(ds['SR_NO'].iloc[-1], value), xytext=(ds['SR_NO'].iloc[-1] + 1, value),
                             color='black', fontsize=10, ha='left', va='center')
            plot_data.append((last_row['SR_NO'], last_row['AVERAGE']))
            # Plot all points at once

        plt.plot(*zip(*plot_data), marker='o', linestyle='-')
        plt.title('SPC-X Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.ylim(float(ymin_limit), float(ymax_limit))

    elif selected_chart_type == 'R Chart':
        sql1 = ("SELECT SR_NO,DATETIME,BATCH,1,2,3,4,5,X_MAX,X_MIN,RANGE,OVERALL_RANGE,UCL_R,LCL_R"
                " FROM Control_chart.dbo.R_CHART ")

        curs1 = connection().execute(sql1)
        rows1 = curs1.fetchall()
        columns1 = [column[0] for column in curs1.description]
        ds1 = pd.DataFrame.from_records(rows1, columns=columns1)
        ds1 = ds1.sort_values(by='SR_NO')
        curs1.close()
        plot_data = []
        UCL_R = ds1['UCL_R'].iloc[-1]
        LCL_R = ds1['LCL_R'].iloc[-1]
        CL = 454.03155
        for i in range(limit_n):
            last_row = ds1.iloc[-(i + 1)]
            if float(ymin_limit) < last_row['RANGE'] < float(ymax_limit):
                plt.text(last_row['SR_NO'], last_row['RANGE'],
                         f'{last_row["RANGE"]:.2f}', fontsize=8, ha='center', va='bottom')
                point_color = 'red' if (last_row['RANGE'] > UCL_R or last_row['RANGE'] < LCL_R) else '#1476b8'
                plt.scatter(last_row['SR_NO'], last_row['RANGE'], color=point_color, zorder=0)
                plot_data.append((last_row['SR_NO'], last_row['RANGE']))
            plt.axhline(y=CL, color='g', linestyle='-', label='CL')
            plt.axhline(y=UCL_R, color='r', linestyle='--', label='UCL')
            plt.axhline(y=LCL_R, color='r', linestyle='--', label='LCL')
            for label, value in [('UCL', UCL_R), ('LCL', LCL_R), ('CL', CL)]:
                plt.annotate(label, xy=(ds1['SR_NO'].iloc[-1], value), xytext=(ds1['SR_NO'].iloc[-1] + 1, value),
                             color='black', fontsize=10, ha='left', va='center')
            plot_data.append((last_row['SR_NO'], last_row['RANGE']))
        # Plot all points at once
        plt.plot(*zip(*plot_data), marker='o', linestyle='-')
        plt.title('SPC-R Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.ylim(float(ymin_limit), float(ymax_limit))


def plot_histo1():
    plt.figure(figsize=(10, 4.5))
    if selected_chart_type == 'Histogram':
        sql = ("SELECT SR_NO, DATETIME, INTERVAL_1, INTERVAL_2, FREQUENCY, CUM_FREQUENCY FROM "
               "Control_chart.dbo.Histogram")
        curs = connection().execute(sql)
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        curs.close()

        val1 = ds['INTERVAL_2'].iloc[0]
        val2 = ds['INTERVAL_2'].iloc[1]
        bin_width = val2 - val1

        bins = np.arange(ds['INTERVAL_2'].min() - bin_width / 2, ds['INTERVAL_2'].max() + bin_width / 2 + bin_width,
                         bin_width)

        frequencies = ds['FREQUENCY']

        plt.hist(ds['INTERVAL_2'], bins=bins, edgecolor='black', weights=frequencies, align='mid')

        # Annotate frequencies in the middle of each bar
        for i, freq in enumerate(frequencies):
            plt.text((bins[i] + bins[i + 1]) / 2, freq, str(int(freq)), ha='center', va='bottom')

        # Add labels and title
        plt.xlabel('Interval')  # Update X-axis label to Interval
        plt.ylabel('Frequency')
        plt.title('Histogram')
        plt.xticks(np.arange(ds['INTERVAL_2'].min(), ds['INTERVAL_2'].max() + 1, bin_width), rotation=45)




def show_chart(f_date, batch, ymin_limit, ymax_limit, disp):
    plt.figure(figsize=(11, 5.5))  # Ensure a fixed figure size
    if selected_chart_type == 'SPC Chart':
        sql1 = ("SELECT SR_NO,DATETIME,BATCH,FINAL_WEIGHT,UCL,LCL,CL,USL,LSL FROM Control_chart.dbo.SOLO_CHART_1 WHERE "
                "CONVERT(DATE, DATETIME) =  ? AND BATCH = ?")
        curs1 = connection().execute(sql1, (f_date, batch))
        rows1 = curs1.fetchall()
        columns1 = [column[0] for column in curs1.description]
        ds1 = pd.DataFrame.from_records(rows1, columns=columns1)
        ds1 = ds1.sort_values(by='SR_NO')
        curs1.close()
        sr_no_len = len(ds1['SR_NO'])
        start_val = ds1['SR_NO'].iloc[0] - 1
        points_per_page = int(disp)
        if sr_no_len == points_per_page:
            points_per_page = +1
        
        # Use the slider to control the starting point of the visible range
        start_x = st.slider('Select start of X-axis range', min_value=0.0, max_value=float(sr_no_len - points_per_page),
                            value=float(start_val), step=1.0)

        # Calculate the end point based on the selected range
        end_x = start_x + points_per_page
        USL = ds1['USL'].iloc[-1]
        LSL = ds1['LSL'].iloc[-1]
        UCL = ds1['UCL'].iloc[-1]
        LCL = ds1['LCL'].iloc[-1]
        CL = ds1['CL'].iloc[-1]
        for index, row in ds1.iterrows():
            if start_x <= row['SR_NO'] <= end_x and float(ymin_limit) <= row['FINAL_WEIGHT'] <= float(ymax_limit):
                plt.text(row['SR_NO'], row['FINAL_WEIGHT'], f'{row["FINAL_WEIGHT"]:.2f}', fontsize=8, ha='center',
                         va='bottom')
                if selected_chart_type == 'SPC Chart' and (row['FINAL_WEIGHT'] > UCL or row['FINAL_WEIGHT'] < LCL):
                    plt.scatter(row['SR_NO'], row['FINAL_WEIGHT'], color='red', zorder=5)
        plt.axhline(y=CL, color='g', linestyle='-', label='CL', zorder=0)
        plt.axhline(y=UCL, color='r', linestyle='--', label='UCL', zorder=0)
        plt.axhline(y=LCL, color='r', linestyle='--', label='LCL', zorder=0)
        plt.axhline(y=USL, color='y', linestyle='--', label='USL', zorder=0)
        plt.axhline(y=LSL, color='y', linestyle='--', label='LSL', zorder=0)
        for label, value in [('UCL', UCL), ('LCL', LCL), ('CL', CL), ('USL', USL), ('LSL', LSL)]:
            plt.annotate(label, xy=(end_x, value), xytext=(end_x + 0.2, value), color='black', fontsize=10, ha='left',
                         va='center')
        plt.plot(ds1['SR_NO'], ds1['FINAL_WEIGHT'], marker='o', linestyle='-')
        plt.title(f'SPC Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.ylim(float(ymin_limit), float(ymax_limit))
        # Set x-axis limits based on the slider value
        plt.xlim(start_x, end_x)
    elif selected_chart_type == 'X Chart':
        sql = ("SELECT SR_NO, DATETIME, BATCH, AVERAGE, UCL_X, LCL_X"
               " FROM Control_chart.dbo.X_CHART_1 WHERE CONVERT(DATE, DATETIME) = ? AND BATCH = ?")
        curs = connection().execute(sql, (f_date, batch))
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        ds = ds.sort_values(by='SR_NO')
        curs.close()
        if ds.empty:
            st.warning("No data found for the selected criteria.")
            return
        sr_no_len = ds['SR_NO'].iloc[-1]
        start_val = ds['SR_NO'].iloc[0] - 1
        start_x = st.slider('Select start of X-axis range', min_value=0.0, max_value=float(sr_no_len),
                            value=float(start_val),
                            step=0.1)
        UCL_X = ds['UCL_X'].iloc[-1]
        LCL_X = ds['LCL_X'].iloc[-1] - 1
        CL = 454.03155
        plt.figure(figsize=(10, 4.5))
        for index, row in ds.iterrows():
            if start_x <= row['SR_NO'] <= start_x + 10 and float(ymin_limit) <= row['AVERAGE'] <= float(ymax_limit):
                plt.text(row['SR_NO'], row['AVERAGE'], f'{row["AVERAGE"]:.2f}', fontsize=8, ha='center', va='bottom')
                if selected_chart_type == 'X Chart' and (row['AVERAGE'] > UCL_X or row['AVERAGE'] < LCL_X):
                    plt.scatter(row['SR_NO'], row['AVERAGE'], color='red', zorder=5)
        plt.axhline(y=CL, color='g', linestyle='-', label='CL', zorder=0)
        plt.axhline(y=UCL_X, color='r', linestyle='--', label='UCL', zorder=0)
        plt.axhline(y=LCL_X, color='r', linestyle='--', label='LCL', zorder=0)
        for label, value in [('UCL', UCL_X), ('LCL', LCL_X), ('CL', CL)]:
            plt.annotate(label, xy=(start_x + 10, value), xytext=(start_x + 10 + 0.2, value),
                         color='black', fontsize=10, ha='left', va='center')
        plt.plot(ds['SR_NO'], ds['AVERAGE'], marker='o', linestyle='-')
        plt.title(f'SPC-X Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.xlim(start_x, start_x + 10)
        plt.ylim(float(ymin_limit), float(ymax_limit))
    elif selected_chart_type == 'R Chart':
        sql_query = ("SELECT SR_NO, DATETIME, BATCH, RANGE, UCL_R, LCL_R"
                     " FROM Control_chart.dbo.R_CHART_1 WHERE CONVERT(DATE, DATETIME) = ? AND BATCH = ?")
        curs = connection().execute(sql_query, (f_date, batch))
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        ds = ds.sort_values(by='SR_NO')
        curs.close()
        if ds.empty:
            st.warning("No data found for the selected criteria.")
            return
        sr_no_len = ds['SR_NO'].iloc[-1]
        start_val = ds['SR_NO'].iloc[0] - 1
        points_per_page = int(disp)
        start_x = st.slider('Select start of X-axis range', min_value=0.0, max_value=float(sr_no_len),
                            value=float(start_val), step=0.1)
        UCL_R = ds['UCL_R'].iloc[-1]
        LCL_R = ds['LCL_R'].iloc[-1]
        CL = 117
        for index, row in ds.iterrows():
            if start_x <= row['SR_NO'] <= start_x + 10 and float(ymin_limit) <= row['RANGE'] <= float(ymax_limit):
                plt.text(row['SR_NO'], row['RANGE'], f'{row["RANGE"]:.2f}', fontsize=8, ha='center', va='bottom')
                if selected_chart_type == 'R Chart' and (row['RANGE'] > UCL_R or row['RANGE'] < LCL_R):
                    plt.scatter(row['SR_NO'], row['RANGE'], color='red', zorder=0)
        plt.axhline(y=CL, color='g', linestyle='-', label='CL', zorder=0)
        plt.axhline(y=UCL_R, color='r', linestyle='--', label='UCL', zorder=0)
        plt.axhline(y=LCL_R, color='r', linestyle='--', label='LCL', zorder=0)
        for label, value in [('UCL', UCL_R), ('LCL', LCL_R), ('CL', CL)]:
            plt.annotate(label, xy=(start_x + 10, value), xytext=(start_x + 10 + 0.2, value),
                         color='black', fontsize=10, ha='left', va='center')
        plt.plot(ds['SR_NO'], ds['RANGE'], marker='o', linestyle='-')
        plt.title(f'SPC-R Chart')
        plt.xlabel('Part Id In Integer')
        plt.ylabel('Coating Difference In Real Format')
        plt.ylim(float(ymin_limit), float(ymax_limit))
        plt.xlim(start_x, start_x + 10)


connect = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                         'Server=TAPR105-PC\\SQLEXPRESS;'
                         'Trusted_Connection=yes;')

if connection:
    print("Connected Successfully")
    cursor = connect.cursor()
else:
    print("Failed to connect")


def insert_data_histogram(cursor, data):
    table_name = '[Control_chart].[dbo].[Histogram]'

    columns = ['SR_NO', 'DATETIME', 'BATCH', 'INTERVAL_1', 'INTERVAL_2', 'FREQUENCY', 'CUM_FREQUENCY']
    SQLCommand = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (?, ?, ?, ?, ?, ?, ?)"
    cursor.executemany(SQLCommand, data)
    cursor.connection.commit()  # Commit changes to the database


def delete_all_data_histogram(cursor):
    table_name = '[Control_chart].[dbo].[Histogram]'
    SQLCommand = f"DELETE FROM {table_name}"
    cursor.execute(SQLCommand)
    cursor.connection.commit()  # Commit changes to the database


def delete_all_data_histogram_hist(cursor):
    table_name = '[Control_chart].[dbo].[Histogram]'
    SQLCommand = f"DELETE FROM {table_name}"
    cursor.execute(SQLCommand)
    cursor.connection.commit()  # Commit changes to the database


def find_max(lst):
    if not lst:  # If the list is empty, return None
        return None
    maximum = lst[0]  # Initialize maximum with the first element of the list
    for item in lst:
        if item > maximum:
            maximum = item
    return maximum


def find_min(lst):
    if not lst:  # If the list is empty, return None
        return None
    minimum = lst[0]  # Initialize maximum with the first element of the list
    for item in lst:
        if item < minimum:
            minimum = item
    return minimum


def plot_histo(batch):
    cursor = connect.cursor()
    # Make sure to fetch the column names
    sql = "SELECT FINAL_WEIGHT FROM [Control_chart].[dbo].[SOLO_CHART_1] WHERE BATCH = ?"
    cursor.execute(sql, batch)
    df = []
    # Check if connection().description is not None before iterating
    if cursor.description is not None:
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        df['FINAL_WEIGHT'] = df['FINAL_WEIGHT']
        # Proceed with your logic using columns
    else:
        # Handle the case when connection().description is None
        print("No description available")
    # Fetch all rows
    # Create DataFrame with proper column name
    # Now you can access the columns
    # variable2 = s_option
    #variable3 = f_date
    # variable4 = df['FINAL_WEIGHT']

    # Filter the DataFrame to select only rows with the desired batch name
    batch_df = batch

    # Now you can access the datetime and final weight of the desired batch
    datetime_values = datetime.datetime.now()
    final_weight = df['FINAL_WEIGHT'].tolist()

    print(datetime_values)
    print(final_weight)
    print(type(final_weight))

    # Perform your calculations here using datetime_values and final_weight...

    # Convert the DataFrame column to a Python list

    ALL_MAXIMUM = find_max(final_weight)
    ALL_MINIMUM = find_min(final_weight)

    print(f"X_MAX", ALL_MAXIMUM)
    print(f"X_MIN", ALL_MINIMUM)
    INTERVAL_2 = (ALL_MAXIMUM - ALL_MINIMUM) / 5

    THIRD_VALUE = ALL_MINIMUM
    FIRST_VALUE = THIRD_VALUE - (2 * INTERVAL_2)
    SECOND_VALUE = THIRD_VALUE - INTERVAL_2
    FOURTH_VALUE = THIRD_VALUE + INTERVAL_2
    FIFTH_VALUE = FOURTH_VALUE + INTERVAL_2
    SIXTH_VALUE = FIFTH_VALUE + INTERVAL_2
    SEVENTH_VALUE = SIXTH_VALUE + INTERVAL_2
    EIGHTH_VALUE = SEVENTH_VALUE + INTERVAL_2
    NINTH_VALUE = EIGHTH_VALUE + INTERVAL_2
    TENTH_VALUE = NINTH_VALUE + INTERVAL_2
    ELEVENTH_VALUE = TENTH_VALUE + INTERVAL_2
    TWELFTH_VALUE = ELEVENTH_VALUE + INTERVAL_2

    bins = [FIRST_VALUE, SECOND_VALUE, THIRD_VALUE, FOURTH_VALUE, FIFTH_VALUE, SIXTH_VALUE, SEVENTH_VALUE,
            EIGHTH_VALUE, NINTH_VALUE, TENTH_VALUE, ELEVENTH_VALUE, TWELFTH_VALUE]

    hist, bin_edges = np.histogram(final_weight, bins=bins)
    cumulative_freq = np.cumsum(hist)

    print(cumulative_freq)

    # ALL_BATCH = []

    # BATCH = variable2.iloc[-1]
    DATETIME = datetime.datetime.now()

    data_to_insert = [
        (1, DATETIME, batch, SECOND_VALUE, FIRST_VALUE, int(hist[0]), int(cumulative_freq[0])),
        (2, DATETIME, batch, THIRD_VALUE, SECOND_VALUE, int(hist[1]), int(cumulative_freq[1])),
        (3, DATETIME, batch, FOURTH_VALUE, THIRD_VALUE, int(hist[2]), int(cumulative_freq[2])),
        (4, DATETIME, batch, FIFTH_VALUE, FOURTH_VALUE, int(hist[3]), int(cumulative_freq[3])),
        (5, DATETIME, batch, SIXTH_VALUE, FIFTH_VALUE, int(hist[4]), int(cumulative_freq[4])),
        (6, DATETIME, batch, SEVENTH_VALUE, SIXTH_VALUE, int(hist[5]), int(cumulative_freq[5])),
        (7, DATETIME, batch, EIGHTH_VALUE, SEVENTH_VALUE, int(hist[6]), int(cumulative_freq[6])),
        (8, DATETIME, batch, NINTH_VALUE, EIGHTH_VALUE, int(hist[7]), int(cumulative_freq[7])),
        (9, DATETIME, batch, TENTH_VALUE, NINTH_VALUE, int(hist[8]), int(cumulative_freq[8])),
        (10, DATETIME, batch, ELEVENTH_VALUE, TENTH_VALUE, int(hist[9]), int(cumulative_freq[9])),
    ]

    print(data_to_insert)

    # You should check if data_to_insert is not empty before proceeding with insertions
    if data_to_insert:
        delete_all_data_histogram(cursor)
        insert_data_histogram(cursor, data_to_insert)
    print("DATA_INSERTED")

    if st.button('Generate Chart', help='To generate Histogram chart'):
        plot_histo1()


def get_batch():
    if selected_chart_type == 'SPC Chart':
        sql_query = "SELECT TOP 1 BATCH FROM Control_chart.dbo.SOLO_CHART ORDER BY BATCH DESC"
        cursor = connection().execute(sql_query)
        row = cursor.fetchone()
        cursor.close()
        return [str(row.BATCH)] if row else []
    elif selected_chart_type == 'X Chart':
        sql_query = "SELECT TOP 1 BATCH FROM Control_chart.dbo.X_CHART ORDER BY BATCH DESC"
        cursor = connection().execute(sql_query)
        row = cursor.fetchone()
        cursor.close()
        return [str(row.BATCH)] if row else []
    elif selected_chart_type == 'R Chart':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.R_CHART ORDER BY BATCH DESC"
        cursor = connection().execute(sql_query)
        row = cursor.fetchone()
        cursor.close()
        return [str(row.BATCH)] if row else []
    elif selected_chart_type == 'Histogram':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.SOLO_CHART_1"
        cursor = connection().execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return list(set([str(row.BATCH) for row in rows]))


def get_batch1():
    if selected_chart_type == 'SPC Chart':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.SOLO_CHART_1"
        cursor = connection().execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return list(set([str(row.BATCH) for row in rows]))
    elif selected_chart_type == 'X Chart':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.X_CHART_1"
        cursor = connection().execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return list(set([str(row.BATCH) for row in rows]))
    elif selected_chart_type == 'R Chart':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.R_CHART_1"
        cursor = connection().execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return list(set([str(row.BATCH) for row in rows]))
    elif selected_chart_type == 'Histogram':
        sql_query = "SELECT DISTINCT BATCH FROM Control_chart.dbo.SOLO_CHART_1"
        cursor = connection().execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return list(set([str(row.BATCH) for row in rows]))


def get_date():
    if selected_chart_type == 'SPC Chart':
        sql_query = "SELECT MAX(DATETIME) FROM Control_chart.dbo.SOLO_CHART_1"
        cursor = connection().execute(sql_query)
        latest_datetime = cursor.fetchone()[0]
        cursor.close()
        return latest_datetime
    elif selected_chart_type == 'X Chart':
        sql_query = "SELECT MAX(DATETIME) FROM Control_chart.dbo.X_CHART_1"
        cursor = connection().execute(sql_query)
        latest_datetime = cursor.fetchone()[0]
        cursor.close()
        return latest_datetime
    elif selected_chart_type == 'Histogram':
        sql_query = "SELECT MAX(DATETIME) FROM Control_chart.dbo.SOLO_CHART_1"
        cursor = connection().execute(sql_query)
        latest_datetime = cursor.fetchone()[0]
        cursor.close()
        return latest_datetime
    elif selected_chart_type == 'R Chart':
        sql_query = "SELECT MAX(DATETIME) FROM Control_chart.dbo.R_CHART_1"
        cursor = connection().execute(sql_query)
        latest_datetime = cursor.fetchone()[0]
        cursor.close()
        return latest_datetime


l_w = 1
dd_w = 2
m_w = 1
op_w = 1
min = 1
max = 1
displ = 1
# creating columns to create image, chart selection,clear button
l_side, dd, m_side, option, y_limit, limit, disp1 = st.columns([l_w, dd_w, m_w, op_w, min, max, displ])
with l_side:
    st.image(img, use_column_width=False)
with dd:
    left, right = st.columns(2)
    with left:
        chart_types = ['SPC Chart', 'R Chart', 'X Chart', 'Histogram']
        selected_chart_type = st.selectbox('Select Chart Type:', chart_types)
    with right:
        select = ["current", "Old data"]
        select_mode = st.selectbox('Mode: ', select)
with m_side:
    if select_mode == 'current':
        if selected_chart_type == 'R Chart' or selected_chart_type == 'X Chart' or selected_chart_type == 'SPC Chart':
            present_datetime = pd.to_datetime('today').date()
            From_date = st.date_input('Date :', value=present_datetime)
            f_date = str(From_date)
    elif select_mode == 'Old data':
        if selected_chart_type == 'Histogram':
            present_datetime = pd.to_datetime('today').date()
            default_datetime = pd.to_datetime(get_date()).date()
            col1, col2 = st.columns(2)
            with col1:
                From_date = st.date_input('From:', value=present_datetime)
            with col2:
                To_date = st.date_input('To:', value=default_datetime)
            f_date = str(From_date)
            t_date = str(To_date)
        else:
            default_datetime = pd.to_datetime(get_date()).date()
            From_date = st.date_input('Date :', value=default_datetime)
            f_date = str(From_date)

with option:
    if select_mode == 'current':
        batch_option = get_batch()
        s_option = st.selectbox('Batch:', batch_option)
        batch = s_option
    elif select_mode == 'Old data':
        batch_option = get_batch1()
        s_option = st.selectbox('Batch:', batch_option)
        batch = s_option

with y_limit:
    if selected_chart_type in ['R Chart', 'X Chart', 'SPC Chart']:
        ymin_limit, ymax_limit = st.columns(2)
        with ymin_limit:
            ymin_limit = st.text_input('Y-min limit:', value=10)
        with ymax_limit:
            ymax_limit = st.text_input('Y-max limit:', value=40)
with disp1:
    if selected_chart_type in ['R Chart', 'X Chart', 'SPC Chart']:
        disp = st.text_input("Y-axis Limit:", value=10)
with limit:
    if select_mode == "current":
        if selected_chart_type in ['R Chart', 'X Chart', 'SPC Chart']:
            limit = st.number_input("NO of Data:", min_value=10, max_value=100, value=10)
        if selected_chart_type in ['R Chart', 'X Chart', 'SPC Chart']:
            chart(limit, ymin_limit, ymax_limit)
        else:
            plot_histo(batch)
    elif select_mode == "Old data":
        if selected_chart_type == 'R Chart' or selected_chart_type == 'X Chart' or selected_chart_type == 'SPC Chart':
            show_chart(f_date, batch, ymin_limit, ymax_limit, disp)
        else:
            plot_histo(batch)


def to_excel(df):
    writer = pd.ExcelWriter('df_test.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.book.close()
    df_xlsx = open('df_test.xlsx', 'rb').read()
    return df_xlsx


def for_excel_data():
    if selected_chart_type == 'SPC Chart':
        sql = ("SELECT SR_NO,DATETIME,BATCH,FINAL_WEIGHT,UCL,LCL,CL,USL,LSL FROM Control_chart.dbo.SOLO_CHART_1 WHERE "
               "CONVERT(DATE, DATETIME) = ? AND BATCH = ?")
        curs = connection().execute(sql, (f_date, batch))
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        df = pd.DataFrame.from_records(rows, columns=columns)
        curs.close()
        return df
    elif selected_chart_type == 'X Chart':
        sql = ("SELECT SR_NO, DATETIME, BATCH, AVERAGE, UCL_X, LCL_X FROM Control_chart.dbo.X_CHART_1 WHERE CONVERT("
               "DATE, DATETIME) = ? AND BATCH = ?")
        curs = connection().execute(sql, (f_date, batch))
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        curs.close()
        return ds
    elif selected_chart_type == 'R Chart':
        sql_query = ("SELECT SR_NO, DATETIME, BATCH, RANGE, UCL_R, LCL_R"
                     " FROM Control_chart.dbo.R_CHART_1 WHERE CONVERT(DATE, DATETIME) = ? AND BATCH = ?")
        curs = connection().execute(sql_query, (f_date, batch))
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        curs.close()
        return ds
    elif selected_chart_type == 'Histogram':
        sql = "SELECT SR_NO,DATETIME,INTERVAL_1,INTERVAL_2,FREQUENCY,CUM_FREQUENCY FROM Control_chart.dbo.Histogram"
        curs = connection().execute(sql)
        rows = curs.fetchall()
        columns = [column[0] for column in curs.description]
        ds = pd.DataFrame.from_records(rows, columns=columns)
        curs.close()
        return ds


left_w = 8.75
right_w = 1.35
left_s, right_s = st.columns([left_w, right_w])
with left_s:
    with st.container(border=True):
        st.pyplot(plt)
with right_s:
    if select_mode == 'current':
        if selected_chart_type == 'R Chart':
            sql = ("SELECT SR_NO, DATETIME, BATCH, RANGE, UCL_R, LCL_R,CpK,Cp"
                   " FROM Control_chart.dbo.R_chart")
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['CpK'] = ds['CpK']
            ds['Cp'] = ds['Cp']
            ds['UCL_R'] = ds['UCL_R']
            ds['LCL_R'] = ds['LCL_R']
            cpk_cp_table = pd.DataFrame({'Parameter': ['CpK', 'Cp', 'UCL', 'LCL'],
                                         'Value': [ds['CpK'].iloc[-1], ds['Cp'].iloc[-1], ds['UCL_R'].iloc[-1],
                                                   ds['LCL_R'].iloc[-1]]})
            st.table(cpk_cp_table)
        elif selected_chart_type == 'X Chart':
            sql = ("SELECT SR_NO, DATETIME, BATCH, AVERAGE, UCL_X, LCL_X,CpK,Cp"
                   " FROM Control_chart.dbo.X_chart")
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['CpK'] = ds['CpK']
            ds['Cp'] = ds['Cp']
            ds['UCL_X'] = ds['UCL_X']
            ds['LCL_X'] = ds['LCL_X']
            cpk_cp_table = pd.DataFrame({'Parameter': ['CpK', 'Cp', 'UCL', 'LCL'],
                                         'Value': [ds['CpK'].iloc[-1], ds['Cp'].iloc[-1], ds['UCL_X'].iloc[-1],
                                                   ds['LCL_X'].iloc[-1]]})
            st.table(cpk_cp_table)
        elif selected_chart_type == 'SPC Chart':
            sql = "SELECT SR_NO,DATETIME,BATCH,FINAL_WEIGHT,UCL,LCL,CL,USL,LSL,CL FROM Control_chart.dbo.SOLO_CHART"
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['UCL'] = ds['UCL']
            ds['LCL'] = ds['LCL']
            ds['USL'] = ds['USL']
            ds['LSL'] = ds['LSL']
            cpk_cp_table = pd.DataFrame({'Parameter': ['USL', 'UCL', 'CL', 'LCL', 'LSL'],
                                         'Value': [ds['USL'].iloc[-1], ds['UCL'].iloc[-1], ds['CL'].iloc[-1],
                                                   ds['LCL'].iloc[-1],
                                                   ds['LSL'].iloc[-1]]})
            st.table(cpk_cp_table)
    elif select_mode == 'Old data':
        if selected_chart_type == 'R Chart':
            sql = ("SELECT SR_NO, DATETIME, BATCH, RANGE, UCL_R, LCL_R,CpK,Cp"
                   " FROM Control_chart.dbo.R_chart_1")
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['CpK'] = ds['CpK']
            ds['Cp'] = ds['Cp']
            ds['UCL_R'] = ds['UCL_R']
            ds['LCL_R'] = ds['LCL_R']
            cpk_cp_table = pd.DataFrame({'Parameter': ['CpK', 'Cp', 'UCL', 'LCL'],
                                         'Value': [ds['CpK'].iloc[-1], ds['Cp'].iloc[-1], ds['UCL_R'].iloc[-1],
                                                   ds['LCL_R'].iloc[-1]]})
            st.table(cpk_cp_table)
        elif selected_chart_type == 'X Chart':
            sql = ("SELECT SR_NO, DATETIME, BATCH, AVERAGE, UCL_X, LCL_X,CpK,Cp"
                   " FROM Control_chart.dbo.X_chart_1")
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['CpK'] = ds['CpK']
            ds['Cp'] = ds['Cp']
            ds['UCL_X'] = ds['UCL_X']
            ds['LCL_X'] = ds['LCL_X']
            cpk_cp_table = pd.DataFrame({'Parameter': ['CpK', 'Cp', 'UCL', 'LCL'],
                                         'Value': [ds['CpK'].iloc[-1], ds['Cp'].iloc[-1], ds['UCL_X'].iloc[-1],
                                                   ds['LCL_X'].iloc[-1]]})
            st.table(cpk_cp_table)
        elif selected_chart_type == 'SPC Chart':
            sql = "SELECT SR_NO,DATETIME,BATCH,FINAL_WEIGHT,UCL,LCL,CL,USL,LSL FROM Control_chart.dbo.SOLO_CHART_1"
            curs = connection().execute(sql)
            # Fetch all rows and create a DataFrame
            rows = curs.fetchall()
            columns = [column[0] for column in curs.description]
            ds = pd.DataFrame.from_records(rows, columns=columns)
            # Close cursor and connection
            curs.close()
            ds['UCL'] = ds['UCL']
            ds['LCL'] = ds['LCL']
            ds['USL'] = ds['USL']
            ds['LSL'] = ds['LSL']
            cpk_cp_table = pd.DataFrame({'Parameter': ['USL', 'UCL', 'CL', 'LCL', 'LSL'],
                                         'Value': [ds['USL'].iloc[-1], ds['UCL'].iloc[-1], ds['CL'].iloc[-1],
                                                   ds['LCL'].iloc[-1],
                                                   ds['LSL'].iloc[-1]]})
            st.table(cpk_cp_table)
        data = for_excel_data()
        df_xlsx = to_excel(data)
        st.download_button(label='📥 Download Current Result',
                           data=df_xlsx,
                           file_name='SPC_data.xlsx')

if __name__ == '__main__':
    chart_placeholder = st.empty()
    time.sleep(5)
    if selected_chart_type == 'R Chart' or selected_chart_type == 'X Chart' or selected_chart_type == ('SPC '
                                                                                                       'Chart'):
        st.experimental_rerun()
    # Run the chart in a loop
    while True:
        batch = str(s_option)
        f_date = str(From_date)
        if select_mode == "current":
            if selected_chart_type == 'R Chart' or selected_chart_type == 'X Chart' or selected_chart_type == ('SPC '
                                                                                                               'Chart'):
                chart(limit, ymin_limit, ymax_limit)
            else:
                plot_histo1()
        elif select_mode == "Old data":
            if selected_chart_type == 'R Chart' or selected_chart_type == 'X Chart' or selected_chart_type == ('SPC '
                                                                                                               'Chart'):
                show_chart(f_date, batch, ymin_limit, ymax_limit, disp)
            else:
                plot_histo(batch)
            # Add a delay to control the update speed
        time.sleep(5)
