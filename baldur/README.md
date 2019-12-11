# baldur
baldur

building

gradle clean build
docker build -t baldur -f Dockerfile .

checking it

docker run --rm -it baldur:latest /bin/bash

running it on container port 8080

docker run --rm -d -v /:/conf:ro -v /:/logs:rw baldur:latest

running it on localhost port 80

docker run --rm -d -v /path/to/local/conf:/conf:ro -v /path/to/local/logs:/logs:rw -p 80:8080 baldur:latest
