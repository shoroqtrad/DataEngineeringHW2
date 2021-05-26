Shrouq Al-fuqahaa 20208075 

The steps below explain what you have to do to run the app  : 

first of all we have to setup the environment and make there directories :
`mkdir ./dags ./logs ./plugins ./data ./output`

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

then we need to init the air flow 
`docker-compose up airflow-init`

after that let's run the docker compose and if you want to run in the background just leave -d after the docker-compose up 
`docker-compose up -d`

then open localhost:8080 to open the airflow dag , username : airflow , password : airflow
then select Kuwait_DAG , click on trigger to start running .
first all library will be installed like sklearn , matplotlib , 
after that the data will be extracted depends on  Country_Region  : kuwait  and save it as DF_Kuwait.csv in the data folder
then we have to scale the data using min max scaler and save it as DF_Kuwait_Scaled.csv in the data folder . 
then the png will created from the matplotlib and save it as Kuwait_scoring_report .png in the output folder 
finally the DF_Kuwait_Scaled.csv into porstgres  to bulid table named  Covid19_Scaled


to open pgdamin localhost:8000 to see the table email : Shrouq@admin.com password : 30101997 
to take a look on  the notebook : localhost:8888


if you want to terminate the docker compose just write : 
`docker-compose down`
