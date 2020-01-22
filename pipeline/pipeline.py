#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')


# In[2]:


import pip


# In[3]:


import psycopg2


# In[13]:


if __name__ == "__main__":

    #Задаём входные параметры
    unixOptions = "sdt:edt"
    gnuOptions = ["start_dt=", "end_dt="]
    
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:] #excluding script name
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:
        print (str(err))
        sys.exit(2)
    
    start_dt = ''
    end_dt = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-sdt", "--start_dt"):
            start_dt = currentValue
        elif currentArgument in ("-edt", "--end_dt"):
            end_dt = currentValue
    
  


# In[14]:


#Порт 5433 на компьютере (ИЗМЕНИТЬ!)

    db_config = {'user': 'my_user',         
                 'pwd': 'my_user_password', 
                 'host': 'localhost',       
                 'port': 5433,              
                 'db': 'zen'}             
    
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                     db_config['pwd'],
                                 db_config['host'],
                                 db_config['port'],
                                 db_config['db'])
    
    engine = create_engine(connection_string)
    
    


# In[15]:


# Теперь выберем из таблицы только те строки,
    # которые были выпущены между start_dt и end_dt
    # причем сделаем запрос явно, прописав все интересующие поля
    
    query = '''
              SELECT event_id,
                     age_segment,
                     event,
                     item_id,
                     item_type,
                     item_topic,
                     source_id,
                     source_type,
                     source_topic,
                     ts,
                     TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' AS dt,
                     user_id
              FROM log_raw
              WHERE TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
          '''.format(start_dt, end_dt)
    
              
    
    log_raw = pd.io.sql.read_sql(query, con = engine)
   
    columns_datetime = ['dt']
    for column in columns_datetime: log_raw[column] = pd.to_datetime(log_raw[column]).dt.round('min')
    #print(log_raw.head())


# In[16]:


dash_visits = log_raw.groupby(['item_topic', 'source_topic','age_segment','dt']).agg({'user_id': 'count'}).reset_index()
dash_engagement = log_raw.groupby(['dt','item_topic','event','age_segment']).agg({'user_id' : 'nunique'}).reset_index()
dash_visits = dash_visits.rename(columns = {'user_id': 'visits'})
dash_engagement = dash_engagement.rename(columns = {'user_id': 'unique_users'})
   
tables = {'dash_visits':dash_visits, 'dash_engagement':dash_engagement}
for table_name, table_data in tables.items():
    query = '''
            DELETE FROM {} WHERE dt BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
          '''.format(table_name, start_dt, end_dt)
    engine.execute(query)
    table_data.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
   
print(dash_visits.head())
print()
print(dash_engagement.head())


# In[ ]:




