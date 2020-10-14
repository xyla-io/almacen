--
-- table
--
CREATE TABLE {SCHEMA}fetch_currency_exchange_rates (
    base character varying(9) NOT NULL,
    "target" character varying(9) NOT NULL,
    "date" date NOT NULL,
    rate double precision NOT NULL,
    crystallized bool not null default true,
    fetch_date datetime,
    PRIMARY KEY (base, "target", "date")
)
distkey ("date")
sortkey ("date", base, "target");