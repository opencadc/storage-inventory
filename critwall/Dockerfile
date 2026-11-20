FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="1.1.1"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/critwall.tar /

CMD ["/critwall/bin/critwall"]
