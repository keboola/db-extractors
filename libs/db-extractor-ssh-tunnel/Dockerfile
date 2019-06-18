FROM php:7.2

ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

RUN apt-get update -q \
  && apt-get install git unzip -y --no-install-recommends

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

COPY . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN composer install --no-interaction

CMD php ./vendor/bin/phpunit
