\copy customers FROM '/source_data/customers.csv' WITH (FORMAT csv, HEADER true)
\copy orders FROM '/source_data/orders.csv' WITH (FORMAT csv, HEADER true)
\copy order_items FROM '/source_data/order_items.csv' WITH (FORMAT csv, HEADER true)
\copy payments FROM '/source_data/payments.csv' WITH (FORMAT csv, HEADER true)

