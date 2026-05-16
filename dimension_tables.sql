CREATE TABLE IF NOT EXISTS pathology_tbl (
pathology VARCHAR(255),
icd10_id VARCHAR(255),
severity INT,
PRIMARY KEY (pathology)
);

CREATE TABLE IF NOT EXISTS pathology_evidences_tbl (
pathology VARCHAR(255), -- foreign key
evidence VARCHAR(255),  -- foreign key
PRIMARY KEY (pathology, evidence)
);

-- ALTER TABLE pathology_evidences_tbl
-- ADD FOREIGN KEY (evidence) REFERENCES evidences_tbl(evidence_code);
-- ALTER TABLE pathology_evidences_tbl
-- ADD FOREIGN KEY (pathology) REFERENCES pathology_tbl(pathology);

CREATE TABLE IF NOT EXISTS evidences_tbl (
evidence_code VARCHAR(255) PRIMARY KEY,
root_evidence VARCHAR(255),
question TEXT,
is_antecedent BOOLEAN,
evidence_type VARCHAR(255),
default_value_bool BOOLEAN,
default_value_num INT,
default_value_str VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS evidence_values_tbl (
evidence_code VARCHAR(255),
evidence_type VARCHAR(255),
possible_values_bool BOOLEAN,
possible_values_num INT,
possible_values_str VARCHAR(255),
evidence_value VARCHAR(255),
PRIMARY key (evidence_value)
);

-- ALTER TABLE evidence_values_tbl
-- ADD FOREIGN KEY (evidence_code) REFERENCES evidences_tbl(evidence_code);

CREATE TABLE IF NOT EXISTS string_values_tbl (
value_str VARCHAR(255),
value_meaning VARCHAR(255),
associated_evidence VARCHAR(255),
PRIMARY KEY (value_str, associated_evidence)
);

-- ALTER TABLE string_values_tbl
-- ADD FOREIGN KEY (associated_evidence) REFERENCES evidences_tbl(evidence_code);