CREATE TABLE IF NOT EXISTS patient_info (
patient_id INT PRIMARY KEY,
age INT,
sex VARCHAR(255),
initial_evidence VARCHAR(255), -- foreign key
ground_pathology VARCHAR(255), -- foreign key
dataset VARCHAR(255) 
);
-- ALTER TABLE patient_info ADD FOREIGN KEY (initial_evidence) REFERENCES evidences(evidence_code);
-- ALTER TABLE patient_info ADD FOREIGN KEY (ground_pathology) REFERENCES pathologies(pathology_name);

CREATE TABLE IF NOT EXISTS patient_evidences_bool (
patient_id INT, -- foreign key
evidence_code VARCHAR(255), -- foreign key
PRIMARY KEY (patient_id, evidence)
);
-- ALTER TABLE patient_evidences_bool ADD FOREIGN KEY (patient_id) REFERENCES patient_info(patient_id);
-- ALTER TABLE patient_evidences_bool ADD FOREIGN KEY (evidence_code) REFERENCES evidence_values_bool(evidence_code);

CREATE TABLE IF NOT EXISTS patient_evidences_num (
patient_id INT, -- foreign key 
evidence_code VARCHAR(255), -- foreign key
value_num INT,
PRIMARY KEY (patient_id, evidence_code)
-- foreign key composite (evidence_code, value_num)
);
-- ALTER TABLE patient_evidences_num ADD FOREIGN KEY (patient_id) REFERENCES patient_info(patient_id);
-- ALTER TABLE patient_evidences_num ADD FOREIGN KEY (evidence_code, value_num) REFERENCES evidence_values_num(evidence_code, possible_values_num);

CREATE TABLE IF NOT EXISTS patient_evidences_str (
patient_id INT, -- foreign key
evidence_code VARCHAR(255),  -- foreign key
value_str VARCHAR(255),
PRIMARY KEY (patient_id, evidence_code, value_str)
-- foreign key composite (evidence_code, value_str)
);
-- ALTER TABLE patient_evidences_str ADD FOREIGN KEY (patient_id) REFERENCES patient_info(patient_id);
-- ALTER TABLE patient_evidences_str ADD FOREIGN KEY (evidence_code, value_str) REFERENCES evidence_values_str(evidence_code, possible_values_str);

CREATE TABLE IF NOT EXISTS patient_ddx (
patient_id INT PRIMARY KEY, -- foreign key
pathology_name VARCHAR(255), -- foreign key
ddx_prob FLOAT 
);
-- ALTER TABLE patient_ddx ADD FOREIGN KEY (patient_id) REFERENCES patient_info(patient_id);
-- ALTER TABLE patient_ddx ADD FOREIGN KEY (pathology_name) REFERENCES pathologies(pathology_name);