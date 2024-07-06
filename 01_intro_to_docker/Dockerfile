
# start with 
# FROM base Docker image that we will build on 
# image_name:version
#docker run -it python:3.9.1 bash (run the container with bash terminal)
FROM python:3.9.1

RUN pip install pandas

#send to this directory and files contained in this directory when docker build is run
WORKDIR /app

#Copy sourcefile destinationfile
COPY pipeline.py pipeline.py

#the terminal that container starts
#docker run -it test:pandas(name of image) [everything here is the args] 
ENTRYPOINT [ "python", "pipeline.py" ]