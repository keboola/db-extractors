FROM centos:7
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

WORKDIR /code

# Install required tools
RUN rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
	rpm -Uvh http://rpms.famillecollet.com/enterprise/remi-release-7.rpm && \
	yum -y --enablerepo=epel,remi,remi-php56 upgrade && \
	yum -y --enablerepo=epel,remi,remi-php56 install \
		git \
		php \
		openssl \
		php-cli \
		php-common \
		php-mbstring \
		php-pdo \
		php-xml \
		php-devel \
		php-pear \
		php-mysql \
		&& \
	yum clean all && \
	echo "date.timezone=UTC" >> /etc/php.ini && \
	echo "memory_limit = -1" >> /etc/php.ini && \
	curl -sS https://getcomposer.org/installer | php && \
	mv composer.phar /usr/local/bin/composer

# Initialize
COPY . /code/
RUN composer install --no-interaction

RUN curl --location --silent --show-error --fail \
        https://github.com/Barzahlen/waitforservices/releases/download/v0.3/waitforservices \
        > /usr/local/bin/waitforservices && \
    chmod +x /usr/local/bin/waitforservices

ENTRYPOINT php ./src/run.php --data=/data