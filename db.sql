CREATE DATABASE wikipedia;
USE wikipedia;
CREATE TABLE wikipedia (
 parent VARCHAR(255) NOT NULL,
 child VARCHAR(255) NOT NULL
);
ALTER TABLE wikipedia ADD CONSTRAINT pk_wikipedia PRIMARY KEY (parent,child);