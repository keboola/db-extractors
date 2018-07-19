FROM php:7.1

ENV COMPOSER_ALLOW_SUPERUSER=1
ENV TZ=UTC

WORKDIR /code

RUN apt-get update -q \
  && apt-get install -y unzip ssh libmcrypt-dev libaio1 wget gnupg dirmngr --no-install-recommends

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

# auto validate license
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

# update repos
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list
RUN echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN apt-get update

RUN mkdir -p /usr/share/man/man1

# install java -- this will fail, but it will at least create the req'd files for the below sed commands
RUN apt-get install -y --no-install-recommends oracle-java8-installer || true

WORKDIR /var/lib/dpkg/info
RUN sed -i 's|JAVA_VERSION=8u171|JAVA_VERSION=8u181|' oracle-java8-installer.*
RUN sed -i 's|PARTNER_URL=http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/|PARTNER_URL=http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/|' oracle-java8-installer.*
RUN sed -i 's|SHA256SUM_TGZ="b6dd2837efaaec4109b36cfbb94a774db100029f98b0d78be68c27bec0275982"|SHA256SUM_TGZ="1845567095bfbfebd42ed0d09397939796d05456290fb20a83c476ba09f991d3"|' oracle-java8-installer.*
RUN sed -i 's|J_DIR=jdk1.8.0_171|J_DIR=jdk1.8.0_181|' oracle-java8-installer.*

# install java (again)
RUN apt-get install -y --no-install-recommends oracle-java8-installer oracle-java8-set-default

RUN apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

ADD oracle/sqlcl /code/

WORKDIR /root
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

# Initialize
COPY . /code
RUN composer selfupdate && composer install --no-interaction

CMD php ./src/run.php --data=/data
