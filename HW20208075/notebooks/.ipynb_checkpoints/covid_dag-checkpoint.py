import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def Get_DF_i(Day):
        import pandas as pd
        DF_i=None
        try: 
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            DF_day['Day']=Day
            cond=(DF_day.Country_Region=='Jordan')
            Selec_columns=['Day','Country_Region', 'Last_Update',
                  'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                  'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
            print(f'{Day} is not available!')
            pass
        return DF_i

def Create_data():
        import pandas as pd
        List_of_Days=[]
        import datetime
        for i in range(1,145):
                Previous_Date = datetime.datetime.today() - datetime.timedelta(days=145-i)
                if (Previous_Date.day >9):
                    if (Previous_Date.month >9):
                          List_of_Days.append(f'{Previous_Date.month}-{Previous_Date.day}-{Previous_Date.year}')
                    else:
                          List_of_Days.append(f'0{Previous_Date.month}-{Previous_Date.day}-{Previous_Date.year}')
                else:
                    if (Previous_Date.month >9):
                          List_of_Days.append(f'{Previous_Date.month}-0{Previous_Date.day}-{Previous_Date.year}')
                    else:
                          List_of_Days.append(f'0{Previous_Date.month}-0{Previous_Date.day}-{Previous_Date.year}')
            
        DF_all=[]
        for Day in List_of_Days:
            DF_all.append(Get_DF_i(Day))
            
        DF_Jordan=pd.concat(DF_all).reset_index(drop=True)
        DF_Jordan['Last_Update']=pd.to_datetime(DF_Jordan.Last_Update, infer_datetime_format=True)  
        DF_Jordan['Day']=pd.to_datetime(DF_Jordan.Day, infer_datetime_format=True)  
        DF_Jordan['Case_Fatality_Ratio']=DF_Jordan['Case_Fatality_Ratio'].astype(float)
        DF_Jordan.set_index('Day', inplace=True)
        DF_Jordan.to_csv('/opt/airflow/data/DF_Jordan.csv')

def MinMaxScaler():
        import pandas as pd
        from sklearn.preprocessing import MinMaxScaler
        DF_Jordan = pd.read_csv('/opt/airflow/data/DF_Jordan.csv', parse_dates=['Last_Update'])
        DF_Jordan.set_index('Day', inplace=True)
        min_max_scaler = MinMaxScaler()
        DF_Jordan_u=DF_Jordan.copy()
        Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active','Case_Fatality_Ratio']
        DF_Jordan_u_2=DF_Jordan_u[Select_Columns]
        DF_Jordan_u_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_Jordan_u_2[Select_Columns]),columns=Select_Columns)
        DF_Jordan_u_3['Day']=DF_Jordan_u.Day
        DF_Jordan_u_3.set_index('Day', inplace=True)
        DF_Jordan_u_3.to_csv('/opt/airflow/data/DF_Jordan_Scaled.csv')

def Plotting():
        import pandas as pd 
        import matplotlib.pyplot as plt
        DF_Jordan_u_3 = pd.read_csv('/opt/airflow/data/DF_Jordan_Scaled.csv', parse_dates=['Last_Update'])
        DF_Jordan_u_3.set_index('Day', inplace=True) 
        #Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active','Case_Fatality_Ratio']
        #DF_Jordan_u_3[Select_Columns].plot(figsize=(30,20))
        #plt.savefig('/opt/airflow/output/Jordan_scoring_report.png')
    

def CSV_to_Postgres():
        import psycopg2
        from sqlalchemy import create_engine
        import pandas as pd

        DF_Jordan_Scaled = pd.read_csv('/opt/airflow/data/DF_Jordan_Scaled.csv', parse_dates=['Last_Update'])
        DF_Jordan_Scaled.set_index('Day', inplace=True)

        host="postgres"
        database="airflow"
        user="airflow"
        password="airflow"
        port='5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        DF_Jordan_Scaled.to_sql('Covid19_Scaled', engine,if_exists='replace',index=False)    

 
 
default_args = {
    'owner': 'admin',
    'start_date': dt.datetime(2021, 5, 24),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}
 
with DAG('covid_dag_jordan',
         default_args=default_args,
         schedule_interval=timedelta(days=1),  
         catchup=False,     
         ) as dag:
    
    Extract_Data = PythonOperator(task_id='GetCovidDataForJordan', python_callable=Create_data)
    Scaled_Data = PythonOperator(task_id='ScaledDataForJordan', python_callable=MinMaxScaler)
    Ploting_Data = PythonOperator(task_id='PlotingScaledDataForJordan', python_callable=Plotting)
    toPostgres = PythonOperator(task_id='to_Postgres', python_callable=CSV_to_Postgres)

    #Install_dependecies = BashOperator(task_id='installing',bash_command='pip install pymongo dnspython')
 
 
 

 Extract_Data >> Scaled_Data >> Ploting_Data >> toPostgres