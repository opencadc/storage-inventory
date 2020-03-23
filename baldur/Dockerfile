# No versioning in cadc-tomcat yet so just use 'latest' for now
#FROM cadc-tomcat:0.1

FROM cadc-tomcat:latest

RUN ln -s /config /usr/share/tomcat/config

RUN rm -rf /usr/share/tomcat/webapps/*
COPY build/libs/baldur.war /usr/share/tomcat/webapps
