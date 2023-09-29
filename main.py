from flask import Flask, jsonify, session, request
import pandas as pd
from dotenv import load_dotenv
import os
from flask_session import Session

load_dotenv(dotenv_path='db_config/.env')
DB_TYPE = os.getenv("DB_TYPE")
def check_table(object_name):
    match DB_TYPE:
        case 'SQLITE3':
            import sqlite3
            conn = sqlite3.connect(os.getenv('SQLITE_DB_PATH'))
        case 'SNOWFLAKE':
            import snowflake.connector
            conn = snowflake.connector.connect(
                user=os.getenv('SNOW_USER'),
                password=os.getenv('SNOW_PASSWORD'),
                account=os.getenv('SNOW_ACCOUNT')
            )
        case default:
            return
    cur = conn.cursor()
    
    if os.path.isfile(f'db_config/{object_name}.sql'):
        sql_file= f'db_config/{object_name}.sql'
        # Open the SQL file
        with open(sql_file) as f:
            sql_query = f.read()
        try:
            df = pd.read_sql_query(sql_query,conn)
            return df
        except:
            return 
    else:
        return 

app = Flask(__name__)
app.secret_key = 'any random string'
SESSION_TYPE = "filesystem"
SESSION_PERMANENT = False
app.config.from_object(__name__)
Session(app)

@app.route("/<object_name>")
def data(object_name):

    # initialization of pagination of api
    start_iloc=0
    page_size = int(os.getenv('PAGE_SIZE'))
    if request.args['page']:
        start_iloc=int(request.args['page'])*page_size-1

    # if data available in session, render from session 
    if 'session' in session and session.get('session')==request.headers['Cookie']:
        if 'obj' in session and session.get('obj')==object_name:
            df = session.get('data')
        else:
            df= check_table(object_name)
    else:
        df= check_table(object_name)

    # get the requested data and update the session data
    session['data'] = df
    session['obj'] = object_name
    session['session']=request.headers['Cookie']

    # render the data
    if df is not None:
        if not df.empty:
            # slice the data as par the page request
            json_index = session.get('data').iloc[start_iloc:start_iloc+page_size].to_json(orient ='records')
            session['page'] = json_index
            return session['page']
        else :
            return "No data found !!"
    else:
        return "not a valid request !"


if __name__=='__main__':
    app.run()