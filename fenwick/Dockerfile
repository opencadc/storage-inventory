FROM images.opencadc.org/library/cadc-java:1

ENV IMAGE_VERSION="1.1.2"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

ADD build/distributions/fenwick.tar /

CMD ["/fenwick/bin/fenwick"]
