-- ShopVibe: schema de origem (OLTP)
-- Execução: docker exec -i postgres psql -U postgres -d postgres < sql/01_schema.sql

CREATE SCHEMA IF NOT EXISTS shopvibe;

CREATE TABLE IF NOT EXISTS shopvibe.customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(120) NOT NULL,
    email        VARCHAR(160) NOT NULL UNIQUE,
    city         VARCHAR(80),
    state        CHAR(2),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shopvibe.products (
    product_id   SERIAL PRIMARY KEY,
    sku          VARCHAR(30) NOT NULL UNIQUE,
    name         VARCHAR(120) NOT NULL,
    category     VARCHAR(60) NOT NULL,
    price        NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    stock_qty    INTEGER NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shopvibe.orders (
    order_id        BIGSERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES shopvibe.customers(customer_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'CREATED',
    payment_method  VARCHAR(30) NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shopvibe.order_items (
    order_item_id  BIGSERIAL PRIMARY KEY,
    order_id       BIGINT NOT NULL REFERENCES shopvibe.orders(order_id),
    product_id     INTEGER NOT NULL REFERENCES shopvibe.products(product_id),
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);

-- Eventos de UPDATE/DELETE trazem a linha completa em "before"
ALTER TABLE shopvibe.customers   REPLICA IDENTITY FULL;
ALTER TABLE shopvibe.products    REPLICA IDENTITY FULL;
ALTER TABLE shopvibe.orders      REPLICA IDENTITY FULL;
ALTER TABLE shopvibe.order_items REPLICA IDENTITY FULL;

-- Publication usada pelo Debezium (pgoutput): só as tabelas de pedido
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'shopvibe_pub') THEN
        CREATE PUBLICATION shopvibe_pub FOR TABLE shopvibe.orders, shopvibe.order_items;
    END IF;
END $$;
