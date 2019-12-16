# No versioning in cadc-tomcat yet so just use 'latest' for now
#FROM cadc-tomcat:0.1

FROM cadc-tomcat:latest

RUN ln -s /conf /root/config

RUN rm -rf /usr/share/tomcat/webapps/*
COPY build/libs/minoc.war /usr/share/tomcat/webapps

EXPOSE 8080
