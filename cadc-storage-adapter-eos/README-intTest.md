## building it
```
docker build -t eos-int-test -f Dockerfile-intTest .
```

## running it
```
docker run --rm -it --user  pdowler:pdowler \
    -v $HOME/.m2:/home/pdowler/.m2:rw \
    -v $(find /home/pdowler/.gradle/wrapper/ -type d -name gradle-8.14):/home/pdowler/gradle8:ro \
    -v $(dirname $(pwd)):/home/pdowler/storage-inventory.git:rw \
    -v $HOME/config:/home/pdowler/config:ro \
     --name eos-int-test eos-int-test:latest /bin/bash
```
Once the session is started:
```
cd ~/storage-inventory.git
alias gradle=$(find /home/pdowler/gradle8/ -type f -name gradle)
...
```
