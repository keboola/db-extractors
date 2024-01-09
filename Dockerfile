ARG PHP_VERSION=8.2
ARG PGSQL_VERSION=13
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

FROM php:${PHP_VERSION}-cli-buster AS base-buster
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

FROM base-buster AS lib-db-extractor-common
ENV APP_NAME=db-extractor-common
ENV APP_HOME=/code/libs/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR ${APP_HOME}/

COPY libs/${APP_NAME}/patches ${APP_HOME}/patches
COPY libs/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    wget \
    curl \
    unzip \
    libzip-dev \
    libicu-dev \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# INTL
RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

# PDO mysql
RUN docker-php-ext-install pdo_mysql

# Add debugger
RUN pecl channel-update pecl.php.net \
    && pecl config-set php_ini /usr/local/etc/php.ini \
    && pecl install xdebug \
    && docker-php-ext-enable xdebug

## Composer - deps always cached unless changed
# First copy only composer files
COPY libs/${APP_NAME}/composer.* ${APP_HOME}/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
COPY libs/db-extractor-adapter /code/libs/db-extractor-adapter
COPY libs/db-extractor-config /code/libs/db-extractor-config
COPY libs/db-extractor-ssh-tunnel /code/libs/db-extractor-ssh-tunnel
COPY libs/db-extractor-table-format /code/libs/db-extractor-table-format

RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY libs/${APP_NAME}/. ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

FROM base-buster AS lib-db-extractor-adapter
ENV APP_NAME=db-extractor-adapter
ENV APP_HOME=/code/libs/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR ${APP_HOME}

COPY libs/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh
COPY libs/${APP_NAME}/docker/MariaDB_odbc_driver_template.ini /etc/MariaDB_odbc_driver_template.ini

# MariaDB ODBC driver package is in backports
RUN printf "deb http://httpredir.debian.org/debian buster-backports main non-free" \
    > /etc/apt/sources.list.d/backports.list

RUN apt-get update && apt-get install -y --no-install-recommends \
        ssh \
        git \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
        # MariaDB ODBC driver
        odbc-mariadb \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh \
	&& odbcinst -i -d -f /etc/MariaDB_odbc_driver_template.ini

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO mysql
RUN docker-php-ext-install pdo_mysql

# PHP ODBC
# https://github.com/docker-library/php/issues/103#issuecomment-353674490
RUN set -ex; \
    docker-php-source extract; \
    { \
        echo '# https://github.com/docker-library/php/issues/103#issuecomment-353674490'; \
        echo 'AC_DEFUN([PHP_ALWAYS_SHARED],[])dnl'; \
        echo; \
        cat /usr/src/php/ext/odbc/config.m4; \
    } > temp.m4; \
    mv temp.m4 /usr/src/php/ext/odbc/config.m4; \
    docker-php-ext-configure odbc --with-unixODBC=shared,/usr; \
    docker-php-ext-install odbc; \
    docker-php-source delete

## Composer - deps always cached unless changed
# First copy only composer files
COPY libs/${APP_NAME}/composer.* ${APP_HOME}/

COPY libs/db-extractor-config /code/libs/db-extractor-config
COPY libs/db-extractor-table-format /code/libs/db-extractor-table-format

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY libs/${APP_NAME}/. ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["composer", "ci"]

FROM base-buster AS lib-db-extractor-config
ENV APP_NAME=db-extractor-config
ENV APP_HOME=/code/libs/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        ssh \
        git \
        locales \
        unzip \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

WORKDIR ${APP_HOME}

## Composer - deps always cached unless changed
# First copy only composer files
COPY libs/${APP_NAME}/composer.* ${APP_HOME}/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY libs/${APP_NAME}/. ${APP_HOME}/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["composer", "ci"]

FROM base-buster AS lib-db-extractor-ssh-tunnel
ENV APP_NAME=db-extractor-ssh-tunnel
ENV APP_HOME=/code/libs/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -q \
  && apt-get install git unzip ssh -y --no-install-recommends

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR ${APP_HOME}

## Composer - deps always cached unless changed
# First copy only composer files
COPY libs/${APP_NAME}/composer.* ${APP_HOME}/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# copy rest of the app
COPY libs/${APP_NAME}/. ${APP_HOME}/

# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["composer", "ci"]

FROM base-buster AS lib-db-extractor-table-format
ENV APP_NAME=db-extractor-table-format
ENV APP_HOME=/code/libs/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR ${APP_HOME}

COPY libs/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        ssh \
        git \
        locales \
        unzip \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

## Composer - deps always cached unless changed
# First copy only composer files
COPY libs/${APP_NAME}/composer.* ${APP_HOME}/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY libs/${APP_NAME}/. ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["composer", "ci"]

FROM base-buster AS app-db-extractor-mssql
ENV APP_NAME=db-extractor-mssql
ENV APP_HOME=/code/apps/${APP_NAME}
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR ${APP_HOME}

COPY apps/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    apt-transport-https \
    wget \
    libxml2-dev \
    gnupg2 \
    unixodbc-dev \
    libgss3 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql17=17.7.1.1-1 \
    mssql-tools=17.7.1.1-1 \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO mssql
RUN pecl install pdo_sqlsrv-5.10.0 sqlsrv-5.10.0 \
    && docker-php-ext-enable sqlsrv pdo_sqlsrv \
    && docker-php-ext-install xml

# Set path
ENV PATH $PATH:/opt/mssql-tools/bin

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

## Composer - deps always cached unless changed
# First copy only composer files
COPY apps/${APP_NAME}/composer.* ${APP_HOME}/
COPY libs/ /code/libs/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY apps/${APP_NAME}/ ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "./src/run.php"]

FROM base-buster AS app-db-extractor-mysql
ENV APP_NAME=db-extractor-mysql
ENV APP_HOME=/code/apps/${APP_NAME}

ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR ${APP_HOME}

COPY apps/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    libicu-dev \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO mysql
RUN docker-php-ext-install pdo_mysql

## Composer - deps always cached unless changed
# First copy only composer files
COPY apps/${APP_NAME}/composer.* ${APP_HOME}/
COPY libs/ /code/libs/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY apps/${APP_NAME}/. ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]

FROM base-buster AS app-db-extractor-pgsql
ENV APP_NAME=db-extractor-pgsql
ENV APP_HOME=/code/apps/${APP_NAME}

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR ${APP_HOME}

COPY apps/${APP_NAME}/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    locales \
    unzip \
    ssh \
    zip \
    libpq-dev \
    postgresql \
    postgresql-contrib \
    libicu-dev \
    && rm -r /var/lib/apt/lists/* \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# PDO pgsql
RUN docker-php-ext-install pdo pdo_pgsql pgsql

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

# Disable PgSQL server side debugging mesages.
#
# The database server can generate debug messages and send them to the client.
# It cannot be configured in PHP, the message goes from libpg -> pgsql extension -> PDO -> PHP STDOUT.
# Azure Managed PgSQL instances generate "LOG" level messages through this channel.
# The "LOG" messages then breaks STDOUT of the testConnection synchronous action, it is no more valid JSON.
#
# This behavior can be modified via "client_min_messages" setting, default value is "NOTICE" level.
# The setting can be set via SQL, using "SET LOCAL ...", but that's too late, some messages are logged when creating a new connection.
# Fortunately, libpq can be configured via "PGOPTIONS" environment variable.
#
# Read more:
# - https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-SHELL "20.1.4. Parameter Interaction via the Shell"
# - https://www.postgresql.org/docs/16/runtime-config-client.html "20.11. Client Connection Defaults / client_min_messages"
ENV PGOPTIONS="-c client_min_messages=ERROR"

## Composer - deps always cached unless changed
# First copy only composer files
COPY apps/db-extractor-pgsql/composer.* ${APP_HOME}/
COPY libs/ /code/libs/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY apps/db-extractor-pgsql ${APP_HOME}/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "./src/run.php"]

FROM quay.io/keboola/aws-cli AS aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
ARG TABLE_EXPORTER_VERSION=3.6.0

RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-jdk8/jdk-8u231-linux-x64.tar.gz /tmp/oracle-jdk.tar.gz
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-basiclite-linux.x64-12.2.0.1.0.zip /tmp/instantclient-basiclite.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-sdk-linux.x64-12.2.0.1.0.zip /tmp/instantclient-sdk.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-instantclient/instantclient-sqlplus-linux.x64-12.2.0.1.0.zip /tmp/instantclient-sqlplus.zip
RUN /usr/bin/aws s3 cp s3://keboola-drivers/oracle-table-exporter/TableExporter-$TABLE_EXPORTER_VERSION-jar-with-dependencies.jar /tmp/table-exporter.jar

FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql

USER root

RUN /opt/mssql/bin/mssql-conf set sqlagent.enabled true

FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql-ssl

USER root

COPY docker/databases/mssql/ssl/mssql.crt /etc/ssl/certs/mssql.crt
COPY docker/databases/mssql/ssl/mssql.key /etc/ssl/private/mssql.key

RUN chmod 600 /etc/ssl/certs/mssql.crt /etc/ssl/private/mssql.key

RUN /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.crt \
      && /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key \
      && /opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2 \
      && /opt/mssql/bin/mssql-conf set network.forceencryption 1

FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql-ssl-invalid-cn

USER root

COPY docker/databases/mssql/ssl/mssql-invalidCn.crt /etc/ssl/certs/mssql.crt
COPY docker/databases/mssql/ssl/mssql-invalidCn.key /etc/ssl/private/mssql.key

RUN chmod 600 /etc/ssl/certs/mssql.crt /etc/ssl/private/mssql.key

RUN /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.crt \
      && /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key \
      && /opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2 \
      && /opt/mssql/bin/mssql-conf set network.forceencryption 1

FROM postgres:${PGSQL_VERSION} AS pgsql

# Use non-standard encoding for tests
RUN localedef -i cs_CZ -c -f ISO-8859-2 -A /usr/share/locale/locale.alias cs_CZ.ISO-8859-2
ENV LANG cs_CZ.ISO-8859-2

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

# Copy certificates, SSL must by enabled by args: "-c ssl=on -c ssl_ca_file=/ssl-cert/ca-cert.pem -c ssl_cert_file=/ssl-cert/server-cert.pem -c ssl_key_file=/ssl-cert/server-key.pem"
COPY docker/databases/pgsql/ssl /ssl-cert
RUN chmod 700 -R /ssl-cert/
RUN chown postgres:postgres -R /ssl-cert/

# Force SSL mode, if used arg "-c hba_file=/etc/postgresql/pg_hba_ssl.conf"
COPY docker/databases/pgsql/conf/pg_hba_ssl.conf /etc/postgresql/pg_hba_ssl.conf
