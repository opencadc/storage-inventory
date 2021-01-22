FROM cadc-java:1

COPY build/distributions/tantar.tar /

RUN tar xvf tantar.tar \
    && rm -rf tantar.tar

CMD ["/tantar/bin/tantar"]
