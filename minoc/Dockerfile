FROM images.opencadc.org/library/cadc-tomcat:1

RUN dnf install -y attr && dnf clean all

COPY build/libs/minoc.war /usr/share/tomcat/webapps/

