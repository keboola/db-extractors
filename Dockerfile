ARG PHP_VERSION=8.2
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





FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql

USER root

RUN /opt/mssql/bin/mssql-conf set sqlagent.enabled true

FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql-ssl

USER root

COPY certs/mssql/mssql.crt /etc/ssl/certs/mssql.crt
COPY certs/mssql/mssql.key /etc/ssl/private/mssql.key

RUN chmod 600 /etc/ssl/certs/mssql.crt /etc/ssl/private/mssql.key

RUN /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.crt \
      && /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key \
      && /opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2 \
      && /opt/mssql/bin/mssql-conf set network.forceencryption 1

FROM mcr.microsoft.com/mssql/server:2019-latest AS mssql-ssl-invalid-cn

USER root

COPY certs/mssql/mssql-invalidCn.crt /etc/ssl/certs/mssql.crt
COPY certs/mssql/mssql-invalidCn.key /etc/ssl/private/mssql.key

RUN chmod 600 /etc/ssl/certs/mssql.crt /etc/ssl/private/mssql.key

RUN /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.crt \
      && /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key \
      && /opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2 \
      && /opt/mssql/bin/mssql-conf set network.forceencryption 1
