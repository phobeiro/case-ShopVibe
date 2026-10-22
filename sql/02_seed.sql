-- ShopVibe: dados iniciais para o demo
-- Execução: docker exec -i postgres psql -U postgres -d postgres < sql/02_seed.sql

INSERT INTO shopvibe.customers (name, email, city, state) VALUES
    ('Ana Souza',     'ana.souza@example.com',     'São Paulo',      'SP'),
    ('Bruno Lima',    'bruno.lima@example.com',    'Rio de Janeiro', 'RJ'),
    ('Carla Mendes',  'carla.mendes@example.com',  'Belo Horizonte', 'MG'),
    ('Diego Alves',   'diego.alves@example.com',   'Curitiba',       'PR'),
    ('Elisa Rocha',   'elisa.rocha@example.com',   'Porto Alegre',   'RS')
ON CONFLICT (email) DO NOTHING;

INSERT INTO shopvibe.products (sku, name, category, price, stock_qty) VALUES
    ('CAM-001', 'Camiseta Básica Preta',  'Camisetas',  59.90, 200),
    ('JEA-001', 'Calça Jeans Slim',       'Calças',    189.90, 120),
    ('TEN-001', 'Tênis Urbano Branco',    'Calçados',  299.90,  80),
    ('BOL-001', 'Bolsa Tiracolo Couro',   'Acessórios',249.90,  40),
    ('REL-001', 'Relógio Minimalista',    'Acessórios',399.90,  25)
ON CONFLICT (sku) DO NOTHING;
