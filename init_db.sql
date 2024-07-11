SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

DROP TABLE IF EXISTS `agents`;
CREATE TABLE `agents` (
    `agent_id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `email` varchar(255) NOT NULL,
    `name` varchar(255),
    `phone` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `operations`;
CREATE TABLE `operations` (
    `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `operation_type` int NOT NULL,
    `product_id` int NOT NULL,
    `agent_id` int NOT NULL,
    `place_id_from` int NOT NULL,
    `place_id_to` int NOT NULL,
    `at` varchar(255) NOT NULL,
    `count` int NOT NULL,
    `price_actual` int NOT NULL
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `places`;
CREATE TABLE `places` (
    `place_id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` varchar(255) NOT NULL,
    `is_supplier` int NOT NULL,
    `is_storage` int NOT NULL,
    `is_retail` int NOT NULL,
    `address` varchar(255),
    `phone` varchar(255),
    `description` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `products`;
CREATE TABLE `products` (
    `product_id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `place_id_of_supplier` int NOT NULL,
    `price` int NOT NULL,
    `name` varchar(255) NOT NULL,
    `vendor_code` varchar(255),
    `author` varchar(255)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;