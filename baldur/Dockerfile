FROM images.opencadc.org/library/cadc-tomcat:1

ENV IMAGE_VERSION="0.3.9"
LABEL org.opencontainers.image.version="${IMAGE_VERSION}"

COPY build/libs/baldur.war /usr/share/tomcat/webapps
