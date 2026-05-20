import json 
import time
import polars as pl
from sqlalchemy import create_engine
from credentials import pathologies_path, evidences_path, config

if __name__ == "__main__":
    begin = time.perf_counter()

    with open(pathologies_path) as f:
        raw_path = json.load(f)

    path_json =[]

    for _, val in raw_path.items():
        path_json.append({
            'pathology_name': val.get('condition_name'),
            'icd10': val.get('icd10-id'),
            'symptoms': val.get('symptoms'),
            'antecedents': val.get('antecedents'),
            'severity': val.get('severity')
        })

    for pathology in path_json:
        symptoms = []
        antecedents = []
        for s, val in pathology['symptoms'].items():
            symptoms.append(s)
        for a, val in pathology['antecedents'].items():
            antecedents.append(a)

        pathology['symptoms'] = symptoms
        pathology['antecedents'] = antecedents

    pathologies_raw = pl.DataFrame(path_json)
    pathologies_df = pathologies_raw.select(['pathology_name','icd10','severity'])
    path_end = time.perf_counter()
    print(f'Created pathologies table in {path_end-begin} seconds.')

    with open(evidences_path) as f:
        raw_ev = json.load(f)

    evidences_json = []

    for _, val in raw_ev.items():
        evidences_json.append(
            {
                'evidence_code': val.get('name'),
                'root_evidence': val.get('code_question'),
                'question': val.get('question_en'),
                'is_antecedent': val.get('is_antecedent'),
                'evidence_type': val.get('data_type'),
                'default_value': val.get('default_value'),
                'possible_values': val.get('possible-values'),
                'value_meaning': val.get('value_meaning')
            }
        )

    evidences_raw = pl.DataFrame(evidences_json)
    evidences_raw = evidences_raw.with_columns(evidence_type = pl.col('evidence_type').replace({'B':'binary','C':'categorical','M':'multichoice'}))
    evidences_raw = evidences_raw.select(pl.exclude('value_meaning'))

    evidences_df = evidences_raw.select(pl.exclude(['possible_values','value_meaning','default_value']))
    ev_end = time.perf_counter()
    print(f'Created evidences table in {ev_end - path_end} seconds.')

    pathology_evidences_df = pathologies_raw.select(['pathology_name','symptoms','antecedents'])
    pathology_evidences_df = pathology_evidences_df.with_columns(pl.concat_list('symptoms','antecedents').alias('evidence_code')).drop('symptoms','antecedents')
    pathology_evidences_df = pathology_evidences_df.explode('evidence_code')
    path_ev_end = time.perf_counter()
    print(f'Created pathology_evidences table in {path_ev_end - ev_end} seconds.')

    ev_val_df = evidences_raw.select(['evidence_code','evidence_type','default_value','possible_values'])
    ev_val_df = ev_val_df.with_columns(possible_values = pl.when(evidence_type = 'binary').then(pl.lit([0,1]))\
                                    .otherwise(pl.col('possible_values')))
    ev_val_df = ev_val_df.explode('possible_values')
    ev_val_df = ev_val_df.with_columns(is_default = (pl.col('default_value')==pl.col('possible_values')))
    ev_val_df = ev_val_df.select(pl.exclude('default_value'))

    ev_val_bool = ev_val_df.filter(evidence_type = 'binary').select('evidence_code').unique()
    ev_val_bool_end = time.perf_counter()
    print(f'Created evidences_value_bool table in {ev_val_bool_end - path_ev_end} seconds.')

    ev_val_num = ev_val_df.filter(pl.col('evidence_type')=='categorical',~pl.col('possible_values').str.contains('V_'))\
                .cast({'possible_values':pl.Int8}).rename({'possible_values':'possible_values_num'}).select(pl.exclude('evidence_type'))
    ev_val_num_end = time.perf_counter()
    print(f'Created evidences_value_num table in {ev_val_num_end - ev_val_bool_end} seconds.')

    ev_val_str = ev_val_df.filter(pl.col('possible_values').str.contains('V_'))\
                .rename({'possible_values':'possible_values_str'}).select(pl.exclude('evidence_type'))
    ev_val_str_end = time.perf_counter()
    print(f'Created evidences_value_num table in {ev_val_str_end - ev_val_num_end} seconds.')

    value_meaning_dict ={
        'value_str':[],
        'value_meaning':[],
    }

    for ev in evidences_json:
        if ev['value_meaning'] == {}:
            continue

        for k, v in ev['value_meaning'].items():
            value_meaning_dict['value_str'].append(k)
            value_meaning_dict['value_meaning'].append(v['en'])

    value_meaning_df = pl.DataFrame(value_meaning_dict)
    value_meaning_df = value_meaning_df.unique(maintain_order=True)
    value_meaning_end = time.perf_counter()
    print(f'Created evidences_value_num table in {value_meaning_end - ev_val_str_end} seconds.')

    user = config["user"]
    password = config["password"]
    host = config["host"]
    port = config["port"]
    database = config["database"]

    url = f"mysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)

    pathologies_df.write_database(
        table_name='pathologies',
        connection = engine,
        if_table_exists='append',
        engine = 'sqlalchemy'
    )

    evidences_df.write_database(
        table_name='evidences',
        connection = engine,
        if_table_exists='append',
        engine = 'sqlalchemy'
    )

    value_meaning_df.write_database(
        'value_str_meaning',
        if_table_exists='append',
        connection=engine,
        engine='sqlalchemy'
    )

    pathology_evidences_df.write_database(
        table_name='pathology_evidences',
        connection = engine,
        if_table_exists='append',
        engine = 'sqlalchemy'
    )

    ev_val_bool.write_database(
        'evidence_values_bool',
        if_table_exists='append',
        connection=engine,
        engine='sqlalchemy'
    )

    ev_val_num.write_database(
        'evidence_values_num',
        if_table_exists='append',
        connection=engine,
        engine='sqlalchemy'
    )

    ev_val_str.write_database(
        'evidence_values_str',
        if_table_exists='append',
        connection=engine,
        engine='sqlalchemy'
    )

    db_load_end = time.perf_counter()
    print(f'Loaded all the tables in MySQL database in {db_load_end - value_meaning_end} seconds.')

