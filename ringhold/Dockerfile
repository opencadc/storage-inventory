FROM cadc-java:1

COPY build/distributions/ringhold.tar /

RUN tar xvf ringhold.tar \
    && rm -rf ringhold.tar

CMD ["/ringhold/bin/ringhold"]
