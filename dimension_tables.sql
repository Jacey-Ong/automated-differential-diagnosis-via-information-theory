-- parent dimension tables

CREATE TABLE IF NOT EXISTS pathologies (
pathology_name VARCHAR(255),
icd10 VARCHAR(255),
severity INT,
PRIMARY KEY (pathology_name)
);

CREATE TABLE IF NOT EXISTS evidences (
evidence_code VARCHAR(255),
root_evidence VARCHAR(255),
question TEXT,
is_antecedent BOOLEAN,
evidence_type ENUM('binary','categorical','multichoice'),
PRIMARY KEY (evidence_code)
);

-- relationship dimension tables - these have foreign keys referencing the parent tables

CREATE TABLE IF NOT EXISTS pathology_evidences (
pathology_name VARCHAR(255), -- foreign key
evidence_code VARCHAR(255),  -- foreign key
PRIMARY KEY (pathology, evidence_code)
);
-- ALTER TABLE pathology_evidences_tbl ADD FOREIGN KEY (evidence) REFERENCES evidences(evidence_code);
-- ALTER TABLE pathology_evidences_tbl ADD FOREIGN KEY (pathology) REFERENCES pathologies(pathology_name);

CREATE TABLE IF NOT EXISTS evidence_values_bool (
evidence_code VARCHAR(255), -- foreign key
PRIMARY KEY (evidence_code)
);
-- ALTER TABLE evidence_values_bool ADD FOREIGN KEY (evidence_code) REFERENCES evidences(evidence_code);

CREATE TABLE IF NOT EXISTS evidence_values_num (
evidence_code VARCHAR(255), -- foreign key
possible_values_num INT,
is_default BOOLEAN,
PRIMARY KEY (evidence_code, possible_values_num)
);
-- ALTER TABLE evidence_values_num ADD FOREIGN KEY (evidence_code) REFERENCES evidences(evidence_code);

CREATE TABLE IF NOT EXISTS evidence_values_str (
evidence_code VARCHAR(255), -- foreign key
possible_values_str VARCHAR(255), -- foreign key
is_default BOOLEAN,
PRIMARY KEY (evidence_code, possible_values_str)
);
-- ALTER TABLE evidence_values_str ADD FOREIGN KEY (evidence_code) REFERENCES evidences(evidence_code);
-- ALTER TABLE evidence_values_str ADD FOREIGN KEY (possible_values_str) REFERENCES value_str_meaning(value_str);

CREATE TABLE IF NOT EXISTS value_str_meaning (
value_str VARCHAR(255),
value_meaning VARCHAR(255),
PRIMARY KEY (value_str)
);