CREATE TABLE `Classifieds` (
    id                  VARCHAR(255)  PRIMARY KEY,
    customer_id         VARCHAR(255), 
    created_at          VARCHAR(255),
    text                TEXT,
    ad_type             VARCHAR(255),
    price               DECIMAL(10,2),
    currency            VARCHAR(255),
    payment_type        VARCHAR(255),
    payment_cost        DECIMAL(10,2),
    created_dt          DATETIME(6),
    created_ns          SMALLINT
)