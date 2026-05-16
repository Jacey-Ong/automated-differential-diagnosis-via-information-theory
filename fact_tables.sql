CREATE TABLE IF NOT EXISTS patient_info_tbl (
patient_id INT PRIMARY KEY,
age INT,
sex VARCHAR(255),
initial_evidence VARCHAR(255), -- foreign key
ground_pathology VARCHAR(255), -- foreign key
dataset VARCHAR(255) 
);

-- ALTER TABLE patient_info_tbl 
-- ADD FOREIGN KEY (initial_evidence) REFERENCES evidences_tbl(evidence_code);

-- ALTER TABLE patient_info_tbl
-- ADD FOREIGN KEY (ground_pathology) REFERENCES pathology_tbl(pathology);

CREATE TABLE IF NOT EXISTS patient_evidences_tbl (
patient_id INT,
evidence VARCHAR(255), -- foreign key
value_bool BOOLEAN,
value_num INT,
value_str VARCHAR(255),
PRIMARY KEY (patient_id, evidence)
);

-- ALTER TABLE patient_evidences_tbl
-- ADD FOREIGN KEY (evidence) REFERENCES evidences_tbl(evidence_code);

-- ALTER TABLE patient_evidences_tbl
-- ADD FOREIGN KEY (patient_id) REFERENCES patient_info_tbl(patient_id);

CREATE TABLE IF NOT EXISTS patient_ddx_tbl (
patient_id INT PRIMARY KEY,
pathology VARCHAR(255), -- foreign key
ddx_prob FLOAT 
);

-- ALTER TABLE patient_ddx_tbl 
-- ADD FOREIGN KEY (pathology) REFERENCES pathology_tbl(pathology);

-- ALTER TABLE patient_ddx_tbl 
-- ADD FOREIGN KEY (patient_id) REFERENCES patient_info_tbl(patient_id);
