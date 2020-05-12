FROM keboola/db-component-ssh-proxy:latest AS sshproxy
FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
ARG TABLE_EXPORTER_VERSION=2.0.2

RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-jdk8/jdk-8u231-linux-x64.tar.gz /tmp/oracle-jdk.tar.gz
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-basiclite-linux.x64-12.2.0.1.0.zip /tmp/instantclient-basiclite.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-sdk-linux.x64-12.2.0.1.0.zip /tmp/instantclient-sdk.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-sqlplus-linux.x64-12.2.0.1.0.zip /tmp/instantclient-sqlplus.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-table-exporter/TableExporter-$TABLE_EXPORTER_VERSION-jar-with-dependencies.jar /tmp/table-exporter.jar

FROM php:7.3-cli-stretch

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV TZ=UTC

WORKDIR /code

RUN apt-get update -q \
  && apt-get install -y unzip ssh libmcrypt-dev libaio1 wget gnupg git dirmngr --no-install-recommends

# Table exporter
COPY --from=1 /tmp/table-exporter.jar /opt/table-exporter.jar

# Oracle instantclient
COPY --from=1 /tmp/instantclient-basiclite.zip /tmp/instantclient-basiclite.zip
COPY --from=1 /tmp/instantclient-sdk.zip /tmp/instantclient-sdk.zip
COPY --from=1 /tmp/instantclient-sqlplus.zip /tmp/instantclient-sqlplus.zip

RUN unzip /tmp/instantclient-basiclite.zip -d /usr/local/
RUN unzip /tmp/instantclient-sdk.zip -d /usr/local/
RUN unzip /tmp/instantclient-sqlplus.zip -d /usr/local/

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
 && update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.8.0_231/bin/java 4 \
 &&  update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/jdk1.8.0_231/bin/javac 4 \
 && update-alternatives --config java \
 && rm /tmp/oracle-jdk.tar.gz

ENV JAVA_HOME /usr/lib/jvm/jdk1.8.0_231

WORKDIR /root
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

COPY --from=sshproxy /root/.ssh /root/.ssh

CMD php ./src/run.php --data=/data
