# Artifact storage and management service (minoc)

### building

1. gradle clean build
2. docker build -t minoc -f Dockerfile .

### checking it
docker run --rm -it minoc:latest /bin/bash

### running it on container port 8080
docker run --rm -d minoc:latest

### running it on localhost port 80
docker run --rm -d -p 80:8080 minoc:latest
