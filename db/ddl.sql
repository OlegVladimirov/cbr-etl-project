drop table if exists public.exchange_rates;
CREATE TABLE public.exchange_rates (
    date DATE,
    currency_code VARCHAR(10),
    rate NUMERIC(10, 4)
);

drop table if exists public.currency_reference;
CREATE TABLE public.currency_reference (
    id VARCHAR(10),
    name TEXT,
    eng_name TEXT,
    nominal INTEGER,
    iso_char_code CHAR(3)
);