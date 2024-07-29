
## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL 
 
[Original source for homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/01-docker-terraform/homework.md)

## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete` 
- `--rc`
- `--rmc`
- `--rm`(Answer)


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

Ans: 0.43.0

![alt text](image.png)


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

1. Run the script docker-compose up 
2. set up the dockerfile
3. docker build -t taxi_data_ingest:homework .
4. docker network ls to find the network name we are connecting to. Will be the one we have build on in step 3
5. docker run -it \
    --network=homework_default \
    taxi_data_ingest:homework \
    --user=root \
    --password=root \
    --host=homework-pgdatabase-1 \
    --port=5432 \
    --db=ny_taxi \
    --table_name1=trips \
    --table_name2=zones
6. create instance of pgadmin and ensure host is homework-pgdatabase-1 or host = (name)

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
**- 15612 (Answer)**
- 15859
- 89009

```sql
SELECT COUNT(1)
FROM public.trips
WHERE DATE(lpep_pickup_datetime) ='2019-09-18' 
    AND DATE(lpep_dropoff_datetime) ='2019-09-18'
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance. 

- 2019-09-18
- 2019-09-16
**- 2019-09-26 (Answer)**
- 2019-09-21
```sql
SELECT DATE(lpep_pickup_datetime),
DATE(lpep_dropoff_datetime),
MAX(trip_distance) as trip_distance
FROM public.trips
GROUP BY 1,2
ORDER BY trip_distance DESC
```

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
**- "Brooklyn" "Manhattan" "Queens" (ANSWER)**  
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"
```sql
SELECT 
	COUNT(1),
	"Borough"
FROM trips 
LEFT JOIN zones
ON trips."PULocationID" = zones."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-09-18' 
GROUP BY 2
ORDER BY 1 DESC
LIMIT 3
```

## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza
**MY ANSWER is Woodside, Solution put JFK Airport not sure why**

![alt text](image-1.png)

```sql
SELECT 
z1."Zone" as pickup,
z2."Zone" as dropoff,
tip_amount
FROM trips 
INNER JOIN zones z1
	ON trips."PULocationID" = z1."LocationID"
INNER JOIN zones z2
	ON trips."DOLocationID" = z2."LocationID"
WHERE to_char(lpep_dropoff_datetime, 'YYYY-MM') = '2019-09' 
	AND z1."Zone" = 'Astoria'
ORDER BY 5 DESC
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```
