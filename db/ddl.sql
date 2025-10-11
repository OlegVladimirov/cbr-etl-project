drop table if exists exchange_rates;
CREATE TABLE exchange_rates (
    date DATE,
    currency_code VARCHAR(10),
    rate DECIMAL(10, 4)
);

drop table if exists currency_reference;
CREATE TABLE currency_reference (
    id VARCHAR(10),
    name TEXT,
    eng_name TEXT,
    nominal INTEGER,
    iso_char_code CHAR(3)
);