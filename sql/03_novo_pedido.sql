-- ShopVibe: cria UM pedido novo (usado no vídeo)
-- Execução: docker exec -i postgres psql -U postgres -d postgres < sql/03_novo_pedido.sql
-- Pedido + itens na mesma transação: o Debezium publica os dois só após o COMMIT.

BEGIN;

WITH novo AS (
    INSERT INTO shopvibe.orders (customer_id, status, payment_method, total_amount)
    VALUES (
        (SELECT customer_id FROM shopvibe.customers ORDER BY random() LIMIT 1),
        'CREATED',
        (ARRAY['credit_card','pix','boleto'])[1 + floor(random() * 3)::int],
        0
    )
    RETURNING order_id
),
itens AS (
    INSERT INTO shopvibe.order_items (order_id, product_id, quantity, unit_price)
    SELECT novo.order_id, p.product_id, 1 + floor(random() * 2)::int, p.price
    FROM novo, (SELECT product_id, price FROM shopvibe.products ORDER BY random() LIMIT 2) p
    RETURNING order_id, quantity, unit_price
)
UPDATE shopvibe.orders o
SET total_amount = (SELECT sum(quantity * unit_price) FROM itens),
    updated_at   = now()
WHERE o.order_id = (SELECT order_id FROM novo);

COMMIT;

SELECT order_id, customer_id, payment_method, total_amount, created_at
FROM shopvibe.orders ORDER BY order_id DESC LIMIT 1;
