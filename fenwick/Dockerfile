FROM cadc-java

ARG APP_VERSION=0.1

COPY build/distributions/fenwick-${APP_VERSION}.tar /

RUN tar xvf fenwick-${APP_VERSION}.tar \
    && cp -a fenwick-${APP_VERSION}/* /usr/local/ \
    && rm -rf fenwick-${APP_VERSION}*

CMD ["/usr/local/bin/fenwick"]
