CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL CHECK (position('@' IN email) > 1),
    phone VARCHAR(50),
    city VARCHAR(100),
    loyalty_tier VARCHAR(20) NOT NULL CHECK (loyalty_tier IN ('Bronze', 'Silver', 'Gold')),
    join_date DATE NOT NULL,
    active_flag BOOLEAN NOT NULL
);

CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    store_id VARCHAR(20) NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL CHECK (updated_at >= order_ts),
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'cancelled', 'returned')),
    channel VARCHAR(20) NOT NULL CHECK (channel IN ('store', 'online')),
    promo_id VARCHAR(20),
    subtotal NUMERIC(14, 2) NOT NULL CHECK (subtotal >= 0),
    discount_amount NUMERIC(14, 2) NOT NULL CHECK (discount_amount >= 0),
    tax_amount NUMERIC(14, 2) NOT NULL CHECK (tax_amount >= 0),
    total_amount NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
    currency_code CHAR(3) NOT NULL CHECK (currency_code = 'PKR')
);

CREATE TABLE order_items (
    order_item_id VARCHAR(30) PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(14, 2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(14, 2) NOT NULL CHECK (line_total >= 0),
    currency_code CHAR(3) NOT NULL CHECK (currency_code = 'PKR')
);

CREATE TABLE payments (
    payment_id VARCHAR(30) PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    payment_ts TIMESTAMP NOT NULL,
    payment_method VARCHAR(30) NOT NULL CHECK (
        payment_method IN ('cash', 'card', 'wallet', 'bank_transfer')
    ),
    payment_status VARCHAR(20) NOT NULL CHECK (
        payment_status IN ('paid', 'refunded', 'cancelled')
    ),
    amount NUMERIC(14, 2) NOT NULL CHECK (amount >= 0),
    currency_code CHAR(3) NOT NULL CHECK (currency_code = 'PKR')
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);

