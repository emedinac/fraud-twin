CREATE SCHEMA IF NOT EXISTS fraudtwin;

CREATE TABLE IF NOT EXISTS fraudtwin.simulation_runs (
    run_id TEXT PRIMARY KEY,
    generator_version TEXT NOT NULL,
    configuration_hash TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('COMPLETED')),
    started_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fraudtwin.institutions (
    run_id TEXT NOT NULL REFERENCES fraudtwin.simulation_runs(run_id),
    id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.customers (
    run_id TEXT NOT NULL REFERENCES fraudtwin.simulation_runs(run_id),
    id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.merchants (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    acquirer_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, acquirer_id) REFERENCES fraudtwin.institutions(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.devices (
    run_id TEXT NOT NULL REFERENCES fraudtwin.simulation_runs(run_id),
    id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.accounts (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    institution_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, institution_id) REFERENCES fraudtwin.institutions(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.cards (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.pix_keys (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    institution_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, institution_id) REFERENCES fraudtwin.institutions(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.payments (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    payer_account_id TEXT NOT NULL,
    payee_account_id TEXT,
    merchant_id TEXT,
    card_id TEXT,
    payer_institution_id TEXT,
    payee_institution_id TEXT,
    payer_pix_key_id TEXT,
    payee_pix_key_id TEXT,
    amount NUMERIC(38, 9) NOT NULL CHECK (amount > 0),
    initiated_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, payer_account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, payee_account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, merchant_id) REFERENCES fraudtwin.merchants(run_id, id),
    FOREIGN KEY (run_id, card_id) REFERENCES fraudtwin.cards(run_id, id),
    FOREIGN KEY (run_id, payer_institution_id) REFERENCES fraudtwin.institutions(run_id, id),
    FOREIGN KEY (run_id, payee_institution_id) REFERENCES fraudtwin.institutions(run_id, id),
    FOREIGN KEY (run_id, payer_pix_key_id) REFERENCES fraudtwin.pix_keys(run_id, id),
    FOREIGN KEY (run_id, payee_pix_key_id) REFERENCES fraudtwin.pix_keys(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.payment_events (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    payment_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    merchant_id TEXT,
    card_id TEXT,
    device_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, payment_id) REFERENCES fraudtwin.payments(run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, merchant_id) REFERENCES fraudtwin.merchants(run_id, id),
    FOREIGN KEY (run_id, card_id) REFERENCES fraudtwin.cards(run_id, id),
    FOREIGN KEY (run_id, device_id) REFERENCES fraudtwin.devices(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.fraud_records (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    account_id TEXT,
    card_id TEXT,
    device_id TEXT,
    merchant_id TEXT,
    payment_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    amount NUMERIC(38, 9) NOT NULL CHECK (amount > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, card_id) REFERENCES fraudtwin.cards(run_id, id),
    FOREIGN KEY (run_id, device_id) REFERENCES fraudtwin.devices(run_id, id),
    FOREIGN KEY (run_id, merchant_id) REFERENCES fraudtwin.merchants(run_id, id),
    FOREIGN KEY (run_id, payment_id) REFERENCES fraudtwin.payments(run_id, id),
    FOREIGN KEY (run_id, event_id) REFERENCES fraudtwin.payment_events(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.fraud_alerts (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    account_id TEXT,
    card_id TEXT,
    device_id TEXT,
    merchant_id TEXT,
    payment_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    fraud_record_id TEXT NOT NULL,
    amount NUMERIC(38, 9) NOT NULL CHECK (amount > 0),
    alert_created_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, card_id) REFERENCES fraudtwin.cards(run_id, id),
    FOREIGN KEY (run_id, device_id) REFERENCES fraudtwin.devices(run_id, id),
    FOREIGN KEY (run_id, merchant_id) REFERENCES fraudtwin.merchants(run_id, id),
    FOREIGN KEY (run_id, payment_id) REFERENCES fraudtwin.payments(run_id, id),
    FOREIGN KEY (run_id, event_id) REFERENCES fraudtwin.payment_events(run_id, id),
    FOREIGN KEY (run_id, fraud_record_id) REFERENCES fraudtwin.fraud_records(run_id, id)
);

CREATE TABLE IF NOT EXISTS fraudtwin.fraud_cases (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    fraud_alert_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    account_id TEXT,
    card_id TEXT,
    device_id TEXT,
    merchant_id TEXT,
    payment_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    fraud_record_id TEXT NOT NULL,
    amount NUMERIC(38, 9) NOT NULL CHECK (amount > 0),
    case_opened_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, fraud_alert_id) REFERENCES fraudtwin.fraud_alerts(run_id, id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id),
    FOREIGN KEY (run_id, account_id) REFERENCES fraudtwin.accounts(run_id, id),
    FOREIGN KEY (run_id, card_id) REFERENCES fraudtwin.cards(run_id, id),
    FOREIGN KEY (run_id, device_id) REFERENCES fraudtwin.devices(run_id, id),
    FOREIGN KEY (run_id, merchant_id) REFERENCES fraudtwin.merchants(run_id, id),
    FOREIGN KEY (run_id, payment_id) REFERENCES fraudtwin.payments(run_id, id),
    FOREIGN KEY (run_id, event_id) REFERENCES fraudtwin.payment_events(run_id, id),
    FOREIGN KEY (run_id, fraud_record_id) REFERENCES fraudtwin.fraud_records(run_id, id)
);

-- Reserved for the customer-interaction workflow introduced by a later
-- release. Keeping the table in the operational schema makes the initial
-- PostgreSQL layout compatible with the documented minimum without inventing
-- synthetic interactions that do not yet exist in the domain model.
CREATE TABLE IF NOT EXISTS fraudtwin.customer_interactions (
    run_id TEXT NOT NULL REFERENCES fraudtwin.simulation_runs(run_id),
    interaction_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    interaction_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, interaction_id),
    FOREIGN KEY (run_id, customer_id) REFERENCES fraudtwin.customers(run_id, id)
);
