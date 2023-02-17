FROM images.opencadc.org/library/cadc-tomcat:1

RUN rm -rf /usr/share/tomcat/webapps/*
COPY build/libs/baldur.war /usr/share/tomcat/webapps
