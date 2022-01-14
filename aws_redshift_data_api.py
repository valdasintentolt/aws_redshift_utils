
import operator
import boto3
from botocore.exceptions import WaiterError, ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client

import pandas as pd
import numpy as np

from IPython.core.magic import register_cell_magic

#---------------------
# Change values before execution
#---------------------

MY_PROFILE="aws_profile"

cluster_id='redshift-cluster'
db = Database= 'redshift-db'
db_user='redadmin'

#---------------------

waiter_name = 'DataAPIExecution'
delay=2
max_attempts=120


waiter_config = {
  'version': 2,
  'waiters': {
    'DataAPIExecution': {
      'operation': 'DescribeStatement',
      'delay': delay,
      'maxAttempts': max_attempts,
      'acceptors': [
        {
          "matcher": "path",
          "expected": "FINISHED",
          "argument": "Status",
          "state": "success"
        },
        {
          "matcher": "pathAny",
          "expected": ["PICKED","STARTED","SUBMITTED"],
          "argument": "Status",
          "state": "retry"
        },
        {
          "matcher": "pathAny",
          "expected": ["FAILED","ABORTED"],
          "argument": "Status",
          "state": "failure"
        }
      ],
    },
  },

}


# Setup the session
boto3_session=boto3.Session(profile_name=MY_PROFILE)

# Setup the client
client_redshift = boto3_session.client("redshift-data")
waiter_model = WaiterModel(waiter_config)
custom_waiter = create_waiter_with_client(waiter_name, waiter_model, client_redshift)

# Helper functions
def parse_data_api_values(a):
    colname=list(a.keys())[0]
    if colname == "isNull": 
        return None
    else: 
        return a[colname]

def redshift_select(sql=None, client=None):
    
    res = client.execute_statement(Database= db, Sql= sql, ClusterIdentifier=cluster_id, DbUser=db_user)

    id = res["Id"]
    # Waiter in try block and wait for DATA API to return
    try:
        custom_waiter.wait(Id=id)
        #print("Done waiting to finish Data API.")
    except WaiterError as e:
        print (e)
        desc=client.describe_statement(Id=id)
        print("Status: " + desc["Status"] + ". Excution time: %d miliseconds" %float(desc["Duration"]/pow(10,6)))
        
    try:
        output=client.get_statement_result(Id=id)
    except WaiterError as e:
        desc=client.describe_statement(Id=id)
        print("Status: " + desc["Status"] + ". Excution time: %d miliseconds" %float(desc["Duration"]/pow(10,6)))
        
    nrows=output["TotalNumRows"]
    #print("Number of rows: %d" %nrows)
    ncols=len(output["ColumnMetadata"])
    #print("Number of columns: %d" %ncols)
    resultrows=output["Records"]
    
    if  nrows == 0:
        return None

    col_labels=[]
    for i in range(ncols): col_labels.append(output["ColumnMetadata"][i]['label'])
    records=[]
    for i in range(nrows): records.append(resultrows[i])
    
    df = pd.DataFrame(np.array(resultrows), columns=col_labels)
    for col_i in list(df.columns):
        df[col_i]=df[col_i].apply(lambda a:parse_data_api_values(a))
  
    return df

@register_cell_magic
def redshift_sql(line, cell):
    return redshift_select(sql=cell, client=client_redshift)


# Usage samples

# --- Basic helper with Pandas DataFrame output ---
sql = """
select * from public.d_dt limit 5
"""
pdf=redshift_select(sql=sql, client=client_redshift)
pdf
# ---

# --- Jupyter cell with Cell Magic ---
# %%redshift_sql
#
# select * from public.d_dt limit 10
# --- 
