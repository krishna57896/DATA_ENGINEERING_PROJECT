#!/bin/bash

# pull the postgres image
#  if it is up to date, then the image is not changed
#  if out of date, then the image is updated
docker pull postgres:11.4

# make postgres directory (to persist postgres between runs)
#  if it doesn't already exist
if [ ! -d postgres ]; then
	mkdir ./postgres 
fi

# run the docker image, specifying 
#  (rm) to remove the container and its filesystem on exit,
#  (name) to identify the container,
#  (e) expose POSTGRES_USER as an environment variable
#  (e) expose POSTGRES_PASSWORD as an environment variable
#  (d) launch the container in the background
#  (p) bind localhost:5432 to port 5432 on the container
#  (v) mount the local volume postgres to the volume path 
#      in the container
docker run --rm \
	--name pg-docker \
	-e POSTGRES_USER=postgres \
	-e POSTGRES_PASSWORD=WildFlea123! \
	-d \
	-p 5432:5432 \
	-v postgres:/var/lib/postgresql/data \
	postgres:11.4

