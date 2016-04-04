FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

RUN yum install -y wget && yum install -y tar
RUN yum -y install vim
RUN yum -y --enablerepo=epel,remi,remi-php56 install gcc
RUN yum -y --enablerepo=epel,remi,remi-php56 install make
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-pear

RUN yum -y --enablerepo=epel,remi,remi-php56 install php-odbc

# Oracle
ADD oracle/oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm /tmp/oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm
ADD oracle/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm /tmp/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm
ADD oracle/oracle.sh /tmp/oracle.sh
RUN yum -y --nogpgcheck install /tmp/oracle-instantclient*
RUN export LD_LIBRARY_PATH=/usr/lib/oracle/12.1/client64/lib/
RUN export ORACLE_HOME=/usr/lib/oracle/12.1/client64/lib/
RUN echo 'instantclient,/usr/lib/oracle/12.1/client64/lib/' | pecl install -f oci8-1.4.10
RUN echo "extension=oci8.so" > /etc/php.d/30-oci8.ini

WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

RUN curl --location --silent --show-error --fail \
        https://github.com/Barzahlen/waitforservices/releases/download/v0.3/waitforservices \
        > /usr/local/bin/waitforservices && \
    chmod +x /usr/local/bin/waitforservices

ENTRYPOINT php ./src/run.php --data=/data