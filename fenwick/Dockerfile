FROM cadc-java:1

COPY build/distributions/fenwick.tar /

RUN tar xvf fenwick.tar \
    && rm -rf fenwick.tar

CMD ["/fenwick/bin/fenwick"]
