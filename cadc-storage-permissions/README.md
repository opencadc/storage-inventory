# storage-inventory
Storage Inventory Permissions API 

building

gradle clean build
docker build -t TBD -f Dockerfile .

checking it

docker run --rm -it TBD:latest /bin/bash

running it on container port 8080

docker run --rm -d -v /:/conf:ro -v /:/logs:rw TBD:latest

running it on localhost port 80

docker run --rm -d -v /:/conf:ro -v /:/logs:rw -p 80:8080 TBD:latest
