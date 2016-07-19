#VERSION 1.0.0
FROM keboola/base-php56
MAINTAINER Marc Raiser <marc@keboola.com>

# Install Dependencies
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-devel
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-pgsql

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction

ENTRYPOINT php ./run.php --data=/data
