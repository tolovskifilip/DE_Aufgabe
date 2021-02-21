import psycopg2
import pandas as pd
import numpy as np
import os
import sqlalchemy

def get_relevant_files():
    path_to_dir = os.getcwd() #get current directory
    filenames = os.listdir(path_to_dir) #files in directory
    
    ##array of dictionaries with columns and their max. length for each .csv table, assuming difference in different files
    #dictionaries_with_maximum_values={}
    filenames_relevant=[]
    
    for file in filenames:
        if 'Historical-Report-GUSFacebook' in file: #check if it is a relevant file
            #print('Current file: ', file)
            #table = pd.read_csv(file) #read in pandas
            #dictionary_maximum_length = {}
            #for column in table.columns:
            #    if pd.api.types.is_object_dtype(table[column].dtype): #if it is an object, which in this case is a string
            #        dictionary_maximum_length[column] = table[column].str.len().max()
            #dictionaries_with_maximum_values[file]=dictionary_maximum_length #corresponding max. lengths for each file
            filenames_relevant.append(file)
            
            
    return filenames_relevant #return appropriate file names in a list

def add_data_to_table(filenames_relevant):
    engine = sqlalchemy.create_engine("postgresql://postgres:passpostgres@localhost:5432/postgres") 
    conn = engine.connect() #connect to database

    for file in filenames_relevant:
        df=pd.read_csv(file)
        
        #renaming columns
        df=df.rename(columns={'Page Name': 'Page_Name','User Name': 'User_Name','Facebook Id': 'Facebook_Id',
                       'Likes at Posting': 'Likes_at_Posting','Followers at Posting': 'Followers_at_Posting',
                       'Video Share Status': 'Video_Share_Status','Post Views': 'Post_Views',
                       'Total Views': 'Total_Views','Total Views For All Crossposts': 'Total_Views_For_All_Crossposts',
                       'Video Share Status': 'Video_Share_Status','Final Link': 'Final_Link',
                       'Image Text': 'Image_Text','Link Text': 'Link_Text',
                       'Sponsor Id': 'Sponsor_Id','Sponsor Name': 'Sponsor_Name',
                       'Total Interactions': 'Total_Interactions',
                       'Overperforming Score (weighted  —  Likes 1x Shares 1x Comments 1x Love 1x Wow 1x Haha 1x Sad 1x Angry 1x Care 1x )': 'Overperforming_Score'})
        
        
        #inserting data into the table, if the table existst, data is appended)
        df.to_sql('HISTORICAL_REPORT_GUSFACEBOOK_table',conn,index=False,if_exists='append')
            
        print('Data is added succesfully')
            
    conn.close()#close connection
        
        
        
        
        
#------------------------unsuccessful attempt with psycopg2, due to copy method and issues with delimititer --------------------------------



#    connection = psycopg2.connect(user="postgres",
#                                  password="passpostgres",
#                                  host="127.0.0.1",
#                                  port="5432",
#                                  database="postgres")
#    
#    cursor = connection.cursor()
#    
#    create_table_query = 'CREATE TABLE HISTORICAL_REPORT_GUSFACEBOOK (PAGE_NAME VARCHAR(45),USER_NAME VARCHAR(12),FACEBOOK_ID BIGINT PRIMARY KEY  NOT NULL, LIKES_AT_POSTING BIGINT,FOLLOWERS_AT_POSTING BIGINT, CREATED VARCHAR(23),TYPE VARCHAR(20), LIKES BIGINT,COMMENTS BIGINT,SHARES BIGINT, LOVE BIGINT, WOW BIGINT, HAHA BIGINT, SAD BIGINT, ANGRY BIGINT, CARE BIGINT,VIDEO_SHARE_STATUS VARCHAR(9),POST_VIEWS BIGINT, TOTAL_VIEWS BIGINT,TOTAL_VIEWS_FOR_ALL_CROSSPOSTS BIGINT, VIDEO_LENGTH VARCHAR(8),URL VARCHAR(61),MESSAGE VARCHAR(4162),LINK VARCHAR(239),FINAL_LINK VARCHAR(1287),IMAGE_TEXT VARCHAR(755),LINK_TEXT VARCHAR(136),DESCRIPTION VARCHAR(2807),SPONSOR_ID DOUBLE PRECISION,SPONSOR_NAME VARCHAR(44),TOTAL_INTERACTIONS VARCHAR(6),OVERPERFORMING_SCORE DOUBLE PRECISION);'# Execute a command: this creates a new table
#    cursor.execute(create_table_query)
#    connection.commit()
#    print("Table created successfully in PostgreSQL ")
#    cursor.close()
#    connection.close()
#    print("PostgreSQL connection is closed")
#     
#        
#        cursor = connection.cursor()
#    
#    for file in filenames:
#        if 'Historical-Report-GUSFacebook' in file: #check if it is a relevant file
#            with open(file, 'r',encoding="utf8") as file_to_inserted:
#            # Notice that we don't need the `csv` module.
#                next(file_to_inserted) # Skip the header row.
#                cursor.copy_from(file_to_inserted, 'HISTORICAL_REPORT_GUSFACEBOOK', sep=',')
#    
#            connection.commit()
#    
#    cursor.close()
#    connection.close()