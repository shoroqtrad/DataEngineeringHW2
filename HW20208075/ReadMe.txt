Shrouq Al-fuqahaa 20208075 

The steps below explain what you have to do to run the app  : 

first of all we have to setup the environment and make there directories :
`mkdir ./dags ./logs ./plugins ./data ./output`

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

then we need to init the air flow 
`docker-compose up airflow-init`

after that let's run the docker compose and if you want to run in the background just leave -d after the docker-compose up 
`docker-compose up -d`

if you want to terminate the docker compose just write : 
`docker-compose down`