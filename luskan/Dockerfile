FROM cadc-tomcat:latest

RUN mkdir /root/config
COPY src/main/resources/LocalAuthority.properties /root/config/
COPY build/libs/*.war /usr/share/tomcat/webapps
