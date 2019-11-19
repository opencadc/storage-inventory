# Artifact storage and management service (minoc)

### building

1. gradle build
2. docker build -t minoc -f Dockerfile .

### checking it
docker run -it minoc:latest /bin/bash

### running it on container port 8080
docker run -d minoc:latest

### running it on localhost port 80
docker run -d -p 80:8080 minoc:latest
