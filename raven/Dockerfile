FROM images.opencadc.org/library/cadc-tomcat:1

ENV IMAGE_VERSION="1.1.1"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

COPY build/libs/raven.war /usr/share/tomcat/webapps
