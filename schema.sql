/* =========================================================
SCHEMA: Coffee Market Expansion Analytics
AUTHOR: Shivam Kumar
DIALECT: MySQL 8.0+
PURPOSE: Production-style schema for analytical loading and
         expansion analysis in MySQL.
========================================================= */

CREATE DATABASE IF NOT EXISTS coffee_expansion;
USE coffee_expansion;

DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS city;

CREATE TABLE city (
    city_id INT NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    population INT NOT NULL,
    estimated_rent DECIMAL(12, 2) NOT NULL,
    city_rank INT NOT NULL,
    CONSTRAINT pk_city PRIMARY KEY (city_id),
    CONSTRAINT uq_city_name UNIQUE (city_name),
    CONSTRAINT uq_city_rank UNIQUE (city_rank),
    CONSTRAINT chk_city_population CHECK (population > 0),
    CONSTRAINT chk_city_rent CHECK (estimated_rent >= 0)
) ENGINE = InnoDB;

CREATE TABLE customers (
    customer_id INT NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    city_id INT NOT NULL,
    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT fk_customers_city
        FOREIGN KEY (city_id) REFERENCES city (city_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    INDEX idx_customers_city (city_id),
    INDEX idx_customers_name_city (customer_name, city_id)
) ENGINE = InnoDB;

CREATE TABLE products (
    product_id INT NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT uq_product_name UNIQUE (product_name),
    CONSTRAINT chk_products_price CHECK (price > 0)
) ENGINE = InnoDB;

CREATE TABLE sales (
    sale_id INT NOT NULL,
    sale_date DATE NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    rating DECIMAL(3, 2) NOT NULL,
    CONSTRAINT pk_sales PRIMARY KEY (sale_id),
    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT chk_sales_total CHECK (total > 0),
    CONSTRAINT chk_sales_rating CHECK (rating BETWEEN 1 AND 5),
    INDEX idx_sales_date (sale_date),
    INDEX idx_sales_customer_date (customer_id, sale_date),
    INDEX idx_sales_product_date (product_id, sale_date),
    INDEX idx_sales_customer_product_date (customer_id, product_id, sale_date)
) ENGINE = InnoDB;

/* =========================================================
INDEXING NOTES
- city_name and product_name are unique natural dimensions used in
  presentation and join validation.
- customer_name is intentionally not unique because the source data
  contains repeated names within the same city.
- sales.total is stored separately from products.price so the model
  remains compatible with future discounts, bundles, or price overrides.
========================================================= */
