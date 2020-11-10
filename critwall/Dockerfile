FROM cadc-java:1

COPY build/distributions/critwall.tar /

RUN tar xvf critwall.tar \
    && rm -f critwall.tar

CMD ["/critwall/bin/critwall"]
