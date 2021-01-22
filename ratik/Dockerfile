FROM cadc-java:1

COPY build/distributions/ratik.tar /

RUN tar xvf ratik.tar \
    && rm -rf ratik.tar

CMD ["/ratik/bin/ratik"]
