# .py file dependencies

import os
import pandas as pd
import numpy as np

from skimpy import clean_columns

from env import user, password, host, get_connection


'''Function to retrieve Codeup Curriculum Logs and cache as .csv file'''
def get_curriculum_logs():

    # creating the operating system filename for referencing
    filename = "curriculum_logs.csv"
    if os.path.isfile(filename):
        
        df = pd.read_csv(filename)

        # let's print the shape
        print(f'df shape: {df.shape}')

        return df

    else: 

        # creating the corriculum logs url for to retrieve from MySQL
        url = get_connection(user, password, host, "curriculum_logs")

        # creating the MySQL query
        query = '''
                SELECT 
                    lg.date,
                    lg.time,
                    lg.user_id,
                    lg.cohort_id,
                    ct.name AS cohort_name,
                    lg.ip AS source_ip,
                    lg.path AS endpoint
                FROM
                    logs lg,
                    cohorts ct
                WHERE
                    lg.cohort_id = ct.id'''

        # creating the df
        df = pd.read_sql(query, url)

        # setting user/cohort id to proper data type
        df[["user_id", "cohort_id"]] = df[["user_id", "cohort_id"]].astype(object)

        # combining date and time & dropping previous columns
        df["datetime"] = df["date"] + " " + df["time"]
        df = df.drop(columns = ["date", "time", "cohort_id"])

        # converting datetime column to proper pd.datetime 
        df["datetime"] = pd.to_datetime(df["datetime"])

        # setting the date column to index
        df = df.set_index("datetime").rename_axis(None).sort_index()
        
        # creating a day column 
        df["day"] = df.index.strftime("%A")

        # creating a month column 
        df["month"] = df.index.strftime("%B")

        # cleaning remaining column names
        df = clean_columns(df)

    # print the new merged df shape
    print(f'df shape: {df.shape}')

    # returning the codeup "curriculum" dataframe
    return df


    