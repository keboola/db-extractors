FROM php:7.1

ENV COMPOSER_ALLOW_SUPERUSER=1

WORKDIR /code

RUN apt-get update -q && apt-get install -y unzip ssh libmcrypt-dev libaio1 --no-install-recommends

# Oracle instantclient
ADD oracle/instantclient-basiclite-linux.x64-12.2.0.1.0.zip /tmp/
ADD oracle/instantclient-sdk-linux.x64-12.2.0.1.0.zip /tmp/
ADD oracle/instantclient-sqlplus-linux.x64-12.2.0.1.0.zip /tmp/

RUN unzip /tmp/instantclient-basiclite-linux.x64-12.2.0.1.0.zip -d /usr/local/
RUN unzip /tmp/instantclient-sdk-linux.x64-12.2.0.1.0.zip -d /usr/local/
RUN unzip /tmp/instantclient-sqlplus-linux.x64-12.2.0.1.0.zip -d /usr/local/

RUN ln -s /usr/local/instantclient_12_2 /usr/local/instantclient
RUN ln -s /usr/local/instantclient/libclntsh.so.12.1 /usr/local/instantclient/libclntsh.so
RUN ln -s /usr/local/instantclient/sqlplus /usr/bin/sqlplus

RUN export LD_LIBRARY_PATH=/usr/local/instantclient
RUN export PATH=/usr/local/instantclient:$PATH
RUN echo 'instantclient,/usr/local/instantclient' | pecl install oci8
RUN echo "extension=oci8.so" > /usr/local/etc/php/conf.d/php-oci8.ini

WORKDIR /root
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

# Initialize
COPY . /code
RUN composer selfupdate && composer install --no-interaction

CMD php ./src/run.php --data=/data