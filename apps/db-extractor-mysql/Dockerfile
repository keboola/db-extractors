FROM php:7.3-cli-stretch

ENV COMPOSER_ALLOW_SUPERUSER=1

RUN apt-get update -q \
  && apt-get install unzip ssh -y --no-install-recommends

RUN docker-php-ext-install pdo pdo_mysql

RUN echo "date.timezone=UTC" >> /usr/local/etc/php/php.ini \
  && echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /root

RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

WORKDIR /code

COPY . /code

RUN composer install --no-interaction

CMD php ./src/run.php --data=/data
