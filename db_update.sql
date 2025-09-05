-- Ensure binary logging is enabled (for future scalability)
SET GLOBAL log_bin = ON;
SET GLOBAL binlog_format = 'ROW';

CREATE DATABASE realtime_orders;
USE realtime_orders;

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    status ENUM('pending', 'shipped', 'delivered') NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create notification table for triggers
CREATE TABLE order_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    action VARCHAR(10),
    old_data JSON,
    new_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER //

CREATE TRIGGER order_after_insert
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_notifications (order_id, action, new_data)
    VALUES (NEW.id, 'INSERT', JSON_OBJECT(
        'id', NEW.id,
        'customer_name', NEW.customer_name,
        'product_name', NEW.product_name,
        'status', NEW.status,
        'updated_at', NEW.updated_at
    ));
END//

CREATE TRIGGER order_after_update
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_notifications (order_id, action, old_data, new_data)
    VALUES (NEW.id, 'UPDATE', 
        JSON_OBJECT(
            'id', OLD.id,
            'customer_name', OLD.customer_name,
            'product_name', OLD.product_name,
            'status', OLD.status,
            'updated_at', OLD.updated_at
        ),
        JSON_OBJECT(
            'id', NEW.id,
            'customer_name', NEW.customer_name,
            'product_name', NEW.product_name,
            'status', NEW.status,
            'updated_at', NEW.updated_at
        )
    );
END//

CREATE TRIGGER order_after_delete
AFTER DELETE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_notifications (order_id, action, old_data)
    VALUES (OLD.id, 'DELETE', JSON_OBJECT(
        'id', OLD.id,
        'customer_name', OLD.customer_name,
        'product_name', OLD.product_name,
        'status', OLD.status,
        'updated_at', OLD.updated_at
    ));
END//

DELIMITER ;

