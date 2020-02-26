FROM cadc-java

ARG APP_VERSION=0.1

COPY build/distributions/cadc-file-validate-${APP_VERSION}.tar /

RUN tar xvf cadc-file-validate-${APP_VERSION}.tar \
    && cp -a cadc-file-validate-${APP_VERSION}/* /usr/local/ \
    && rm -rf cadc-file-validate-${APP_VERSION}*

CMD ["/usr/local/bin/cadc-file-validate"]
