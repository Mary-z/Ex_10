DROP TABLE IF EXISTS sales_temp;
            CREATE TABLE sales_temp (
                sale_id INT PRIMARY KEY,
                customer_id INT,
                product_id INT,
                quantity INT,
                sale_date DATE,
                sale_amount INT,
                region VARCHAR(5)
            );

DROP TABLE IF EXISTS sales;
            CREATE table sales (
               sale_id INT PRIMARY KEY,
               customer_id INT,
               product_id INT,
               quantity INT,
               sale_date DATE,
               sale_amount INT,
               region VARCHAR(5)
            );
