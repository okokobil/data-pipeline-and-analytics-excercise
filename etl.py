#Time too short
#Apologies coudn't finish this excercise in designated time. My knowledge of python and programming concepts are awesome. But I have created a ETL pipeline in python before. But I understand the concepts of ETL and its architecture very well.
#My Plan below
#Extraction:
#•	Files: Read data from CSV, JSON, Excel, or other file formats using pandas (e.g., pd.read_csv(), pd.read_json()). 
#Transformation:
#•	Data Cleaning: Handle missing values (df.fillna(), df.dropna()), remove duplicates (df.drop_duplicates()), correct data types (df.astype()).
#•	Data Manipulation: Filter rows (df[df['column'] > value]), select/rename columns, aggregate data (df.groupby().agg()), merge/join dataframes (pd.merge()).
#•	Create the star schema
#Loading:
#•	Files: Save data to various file formats using pandas (e.g., df.to_csv(), df.to_json()).
import pandas as pd


raw_data = {
    "channel_code":'./data/channel_code.csv',
    "plan":'./data/plan.csv',
    "plan_payment_frequency":'./data/plan_payment_frequency.csv',
    "payment_frequency":'./data/payment_frequency.csv',
    "status_code":'./data/status_code.csv',
    "play_session_channel":'./data/play_session_channel.csv',
    "play_session_status_code":'./data/play_session_status_code.csv',
    "user":'./data/user.csv',
    "user_payment_detail":'./data/user_payment_detail.csv',
    "user_plan":'./data/user_plan.csv',
    "user_play_session":'./data/user_play_session.csv',
    "user_registration":'./data/user_registration.csv',
}

    
table_names = [key for key in raw_data.keys()]

for table_name in table_names:
    try:
        source = raw_data[table_name]
        df = pd.read_csv(source)
        #Clean data
        globals()[table_name] = df
        globals()[table_name].dropna(inplace=True)
        globals()[table_name].drop_duplicates(inplace=True)
        print(globals()[table_name])
    except FileNotFoundError:
        print("Error: table not found. Please create it with some data.")
        exit()


user_payment_detail_key = 'user_play_session_id'
user_key = 'user_id'
user_registration_key = 'user_registration_id'
user_plan_key = 'user_plan_id'
user_payment_detail_key = 'user_payment_detail_id'
plan_key = 'plan_id'
payment_frequency_key = 'payment_frequency_code'
user_plan_key = 'user_plan_id'
play_session_status_code_key = 'play_session_status_code'


