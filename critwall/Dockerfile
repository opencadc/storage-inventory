FROM cadc-java

ARG APP_VERSION=0.1

COPY build/distributions/critwall-${APP_VERSION}.tar /

RUN tar xvf critwall-${APP_VERSION}.tar \
    && cp -a critwall-${APP_VERSION}/* /usr/local/ \
    && rm -rf critwall-${APP_VERSION}*

CMD ["/usr/local/bin/critwall"]
