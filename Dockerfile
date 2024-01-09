ARG PHP_VERSION=8.2
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive

FROM php:${PHP_VERSION}-cli-buster AS base-buster
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

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
