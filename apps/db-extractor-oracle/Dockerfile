FROM db-ex-oracle-sshproxy AS sshproxy
FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-jdk8/jdk-8u211-linux-x64.tar.gz /tmp/oracle-jdk.tar.gz

FROM php:7.3-cli-stretch

ENV COMPOSER_ALLOW_SUPERUSER=1
ENV TZ=UTC

WORKDIR /code

RUN apt-get update -q \
  && apt-get install -y unzip ssh libmcrypt-dev libaio1 wget gnupg git dirmngr --no-install-recommends

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
RUN echo /usr/local/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf
RUN ldconfig

RUN mkdir -p /usr/share/man/man1

COPY --from=1 /tmp/oracle-jdk.tar.gz /tmp/oracle-jdk.tar.gz

RUN mkdir /usr/lib/jvm/ && tar zxvf /tmp/oracle-jdk.tar.gz -C /usr/lib/jvm/ \
 && update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.8.0_211/bin/java 4 \
 &&  update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/jdk1.8.0_211/bin/javac 4 \
 && update-alternatives --config java \
 && rm /tmp/oracle-jdk.tar.gz

ENV JAVA_HOME /usr/lib/jvm/jdk1.8.0_211

WORKDIR /root
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

# Initialize
COPY . /code
RUN composer selfupdate && composer install --no-interaction

COPY --from=sshproxy /root/.ssh /root/.ssh

CMD php ./src/run.php --data=/data
