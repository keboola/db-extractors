FROM php:7.2

ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER=1

# Deps
RUN apt-get update
RUN apt-get install -y --no-install-recommends wget curl make git zip unzip time ssh libzip-dev openssl libpq-dev \
  && docker-php-ext-install pdo pdo_pgsql

# php settings
RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/php.ini

WORKDIR /root

# Composer
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

ADD . /code
RUN composer install --no-interaction

CMD php ./run.php --data=/data
