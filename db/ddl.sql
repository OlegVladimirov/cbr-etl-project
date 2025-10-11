DROP TABLE IF EXISTS exchange_rates;
CREATE TABLE exchange_rates (
    date DATE,
    currency_code VARCHAR(10),
    rate DECIMAL(10, 4)
);

DROP TABLE IF EXISTS currency_reference;
CREATE TABLE currency_reference (
    id VARCHAR(10),
    name TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    eng_name TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    nominal INT,
    iso_char_code CHAR(3)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;