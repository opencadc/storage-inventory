FROM cadc-java

ARG APP_VERSION=0.1

COPY build/distributions/tantar-${APP_VERSION}.tar /

RUN tar xvf tantar-${APP_VERSION}.tar \
    && cp -a tantar-${APP_VERSION}/* /usr/local/ \
    && rm -rf tantar-${APP_VERSION}*

CMD ["/usr/local/bin/tantar"]
