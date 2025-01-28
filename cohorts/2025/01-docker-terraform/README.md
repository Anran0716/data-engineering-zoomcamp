# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1

```bash
docker run -it --entrypoint bash python:3.12.8
pip --version
```

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- db:5432

If there are more than one answers, select only one of them

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  198,924;  109,603;  27,678;  35,189


Note: I got totally different number when I queried at Postgre. So I picked the answer closest to my outcome. 

```bash
#outcome: 104830, 198995, 109642, 27686, 35201
SELECT 
	COUNT(*) FILTER(WHERE trip_distance<=1) AS up_to_1mi,
	COUNT(*) FILTER(WHERE trip_distance>1 AND trip_distance<=3) AS between_1_and_3_mi,
	COUNT(*) FILTER(WHERE trip_distance>3 AND trip_distance<=7) AS between_3_and_7_mi,
  COUNT(*) FILTER(WHERE trip_distance>7 AND trip_distance<=10) AS between_7_and_10_mi,
	COUNT(*) FILTER(WHERE trip_distance>10) AS over_10_mi 
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'

```


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-31

```bash

SELECT 
    lpep_pickup_datetime::date AS pickup_date,
    MAX(trip_distance) AS max_trip_distance
FROM 
    green_taxi_trips
WHERE 
    lpep_pickup_datetime::date IN ('2019-10-11', '2019-10-24', '2019-10-26', '2019-10-31')
GROUP BY 
    lpep_pickup_datetime::date
ORDER BY 
    max_trip_distance DESC
LIMIT 1;

```


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights

```bash

SELECT 
    sum(total_amount) AS sum_amount,
	CONCAT(z."Zone") AS "pickup_loc"
FROM 
    green_taxi_trips g,
	taxi_zones z
WHERE 
    g."PULocationID" = z."LocationID"
 AND 
    g.lpep_pickup_datetime::date = '2019-10-18'
GROUP BY pickup_loc
HAVING sum(total_amount)>13000
ORDER BY sum_amount DESC
LIMIT 3;

```

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- JFK Airport

```bash

SELECT 
    MAX(g.tip_amount) AS largest_tip,
    CONCAT(zdo."Zone") AS "dropff_loc"
FROM 
    green_taxi_trips g
JOIN 
    taxi_zones zpu ON g."PULocationID" = zpu."LocationID"
JOIN
    taxi_zones zdo ON g."DOLocationID" = zdo."LocationID"
WHERE
    g.lpep_pickup_datetime::date >= '2019-10-01'
    AND g.lpep_pickup_datetime::date < '2019-11-01'
    AND zpu."Zone" = 'East Harlem North'
GROUP BY zdo."Zone"
ORDER BY largest_tip DESC
LIMIT 1;

```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform init, terraform apply -auto-approve, terraform destroy



## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1
