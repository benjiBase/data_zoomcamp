
# start with 
# FROM base Docker image that we will build on 
# image_name:version
#docker run -it python:3.9.1 bash (run the container with bash terminal)

#to run this use
#docker build -t test:homework . (name_1:name_2) (build the image)
#docker run -it test:pandas(name of image) [everything here is the args]

#install the dependencies for ingest-data.py with this docker build and run
FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

#send to this directory and files contained in this directory when docker build is run
WORKDIR /app 

#Copy sourcefile destinationfile
COPY ingest-data.py ingest-data.py

#the terminal that container starts
#docker run -it test:pandas(name of image) [everything here is the args] 
ENTRYPOINT [ "python", "ingest-data.py" ]


#docker build -t taxi_data_ingest:homework .
# docker run -it \
#     --network=homework_default \
#     taxi_data_ingest:homework \
#     --user=root \
#     --password=root \
#     --host=    \
#     --port=5432 \
#     --db=ny_taxi \
#     --table_name1=trips \
#     --table_name2=zones
