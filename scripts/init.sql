CREATE DATABASE email_sender;

\c email_sender;

CREATE TABLE emails(
    id SERIAL NOT NULL,
    subject VARCHAR(100) NOT NULL,
    message VARCHAR(250) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);