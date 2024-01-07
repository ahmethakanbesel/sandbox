# Use the official PHP 8.3 image with FPM based on Alpine
FROM php:8.3-fpm-alpine

# Install system dependencies
RUN apk add --no-cache \
    libpng-dev \
    oniguruma-dev \
    libxml2-dev \
    libzip-dev \
    nginx \
    zip \
    unzip \
    git

# Install PHP extensions
RUN docker-php-ext-install pdo_mysql mbstring exif pcntl bcmath gd zip

# Enable PHP OPcache extension
RUN docker-php-ext-enable opcache

# OPcache configuration (you can adjust these values as needed)
RUN { \
    echo 'opcache.memory_consumption=128'; \
    echo 'opcache.interned_strings_buffer=8'; \
    echo 'opcache.max_accelerated_files=4000'; \
    echo 'opcache.revalidate_freq=2'; \
    echo 'opcache.fast_shutdown=1'; \
    echo 'opcache.enable_cli=1'; \
    } > /usr/local/etc/php/conf.d/opcache-recommended.ini

# Set working directory
WORKDIR /var/www

# Copy a new configuration file
COPY start.sh /start.sh

RUN chmod +x /start.sh

# Expose port 80
EXPOSE 80

# Start PHP-FPM and Nginx server
CMD ["/start.sh"]