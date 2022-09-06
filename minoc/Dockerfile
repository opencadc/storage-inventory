FROM cadc-tomcat

RUN dnf install -y wcslib && dnf clean all

COPY build/libs/minoc.war /usr/share/tomcat/webapps/

