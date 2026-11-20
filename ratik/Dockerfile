FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="1.1.0"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/ratik.tar /

CMD ["/ratik/bin/ratik"]
