import time
import torch
from pathlib import Path
# from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, MultiLabelBinarizer
from mysql.connector import connect
from credentials import config
import pandas as pd
import polars as pl
import numpy as np

def extract_patient_info(config: dict, dataset: str) -> pl.DataFrame:   
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""select patient_id, age, sex, initial_evidence from patient_info 
                    where dataset = '{dataset}' """
            cur.execute(query)
            raw = cur.fetchall()

    df = pl.DataFrame(raw, schema=['patient_id', 'age', 'sex', 'initial_evidence'], orient='row')
    return df

def age_sex_transform(df: pl.DataFrame) -> pl.DataFrame:
    scaler = MinMaxScaler()
    encoded_age = scaler.fit_transform(df.select('age'))
    encoded_age = [age[0] for age in encoded_age]
    df = df.with_columns(pl.Series(encoded_age).alias('age_enc'))

    ohe = OneHotEncoder(sparse_output=False)
    encoded_sex = ohe.fit_transform(df.select('sex'))
    df = df.with_columns(pl.Series(encoded_sex).alias('sex_enc'))

    return df.select(pl.exclude(['age','sex']))

def bool_ev_encoder(config: dict) -> MultiLabelBinarizer:
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = """
            select distinct evidence_code from evidence_values_bool
            """
            cur.execute(query)
            raw = cur.fetchall()

    ev_bool = [ev[0] for ev in raw]
    mlb = MultiLabelBinarizer()
    mlb.fit([ev_bool])
    return mlb

def extract_binary_patient_ev(config: dict, info_df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""
            select * from patient_evidences_bool 
            where patient_id in
            (select patient_id from patient_info where dataset = '{dataset}')
            order by patient_id
            """
            cur.execute(query)
            raw = cur.fetchall()

    df = pl.DataFrame(raw, schema=['patient_id','evidence_code'], orient='row')
    df = df.group_by('patient_id').agg(pl.col('evidence_code')).sort('patient_id')
    df = info_df.join(df, on='patient_id', how='left'
                ).with_columns(pl.concat_list(['initial_evidence','evidence_code']).alias('evidence_code')
                ).select(['patient_id','evidence_code'])
    return df

def extract_num_patient_ev(config: dict, info_df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""
            select * from patient_evidences_num 
            where patient_id in
            (select patient_id from patient_info where dataset = '{dataset}')
            order by patient_id
            """
            cur.execute(query)
            raw = cur.fetchall()

    df = pl.DataFrame(raw, schema=['patient_id','evidence_code','value_num'], orient='row').cast({'value_num':pl.Float64})
    df = df.pivot('evidence_code',index='patient_id',values='value_num')
    df = info_df.join(df, on='patient_id', how='left').select(df.columns).fill_null(0)
    return df

def str_val_encoders(config: dict, evidence_type: str) -> tuple[dict[str,MultiLabelBinarizer], dict[str,str]]:
    if evidence_type != 'multichoice' and evidence_type != 'categorical':
        raise ValueError('Invalid evidence_type: should be either multichoice or categorical')

    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""
            select * from evidence_values_str 
            where evidence_code in 
            (select evidence_code from evidences where evidence_type ='{evidence_type}')
            """
            cur.execute(query)
            raw = cur.fetchall()

    str_vals = pl.DataFrame(raw, schema=['evidence_code', 'value_str','is_default'],orient='row')
    str_codes = sorted(str_vals.filter(pl.col('is_default')==1)['evidence_code'].to_list())
    val_defaults = str_vals.filter(pl.col('is_default')==1).pivot('evidence_code',
                                    values='value_str',sort_columns=True).select(pl.exclude('is_default')
                                                                        ).to_dict(as_series=False)

    enc_dict = {}
    for ev_code in str_codes:
        vals = str_vals.filter(pl.col('evidence_code')==ev_code)['value_str'].to_list()
        mlb = MultiLabelBinarizer()
        enc_dict[ev_code] = mlb.fit([vals])
    
    return enc_dict, val_defaults

def extract_str_patient_ev(config: dict, info_df: pl.DataFrame, val_defaults: dict, evidence_type: str, dataset: str) -> pl.DataFrame:
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""
            select * from patient_evidences_str
            where patient_id in
            (select patient_id from patient_info where dataset = '{dataset}')
            and evidence_code in
            (select evidence_code from evidences where evidence_type = '{evidence_type}')
            order by patient_id
            """
            cur.execute(query)
            raw = cur.fetchall()

    df = pl.DataFrame(raw, schema=['patient_id','evidence_code','value_str'],orient='row')
    df = df.group_by(['patient_id','evidence_code']
                ).agg('value_str'
                ).sort('patient_id'
                ).pivot(on='evidence_code',index='patient_id',values='value_str', sort_columns=True
                )
    df = info_df.join(df, on='patient_id', how='left').select(df.columns)
    df = df.with_columns([
                    pl.when(pl.col(col).is_null())
                    .then(pl.lit(default))
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col, default in val_defaults.items()
                ])
    return df

def encode_str_evidence(enc_dict: dict[str,MultiLabelBinarizer], df: pl.DataFrame) -> pl.DataFrame:
    df_enc = {}

    for ev_code, enc in enc_dict.items():
        vals = df[ev_code]
        encoded = enc.transform(vals).astype(np.float64)
        df_enc[ev_code] = encoded

    return pl.DataFrame(df_enc)

def create_symptom_vector(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    new_cols = []
    for column in df.columns:
        # if column == 'patient_id':
        #     new_cols += [column]
        #     continue
        c = df.select(column)
        arr_len = c.dtypes[0].size if c.dtypes[0]==pl.Array else 1
        if arr_len == 1:
            exprs += [pl.col(column)]
            new_cols += [column]
        else:
            for i in range(arr_len):
                exprs += [pl.col(column).arr.get(i).alias(f'{column}_{i}')]
                new_cols += [f'{column}_{i}']
                
    df = df.with_columns(exprs).select(new_cols).cast({pl.Float64:pl.Float16})
    vec_df = df.with_columns(pl.concat_arr(pl.col(pl.Float16)).alias('symptom_vector')).select(['patient_id','symptom_vector'])
    return vec_df

def extract_patient_pathologies(config: dict, dataset: str) -> pl.DataFrame:
    with connect(**config) as cnx:
        with cnx.cursor() as cur:
            query = f"""select * from patient_ddx
            where patient_id in 
            (select patient_id from patient_info where dataset = '{dataset}')
            """
            cur.execute(query)
            raw = cur.fetchall()

    df = pl.DataFrame(raw, schema=['patient_id','pathology_name','ddx_prob'],orient='row')
    df = df.pivot('pathology_name',index='patient_id',values='ddx_prob').fill_null(0)
    return df

def create_pathology_vector(df: pl.DataFrame) -> pl.DataFrame:
    df = df.cast({pl.Float64:pl.Float16})
    return df.with_columns(pl.concat_arr(pl.col(pl.Float16).alias('pathology_vector'))).select('pathology_vector')

def preprocessing_finalized(config: dict, dataset: str):
    if dataset != 'train' and dataset != 'validate' and dataset != 'test':
        raise ValueError('Invalid input: dataset should either be train, validate or test')
    
    # 1.1 Extract the patient_id, age, sex and initial evidence information from MySQL database
    info_df = extract_patient_info(config, dataset)
    print('Extracted initial patient info from MySQL database')
    print(info_df)
    # 1.2. Normalize age and one-hot encode sex
    info_df = age_sex_transform(info_df)
    print('Normalized age and one-hot encoded sex')
    print(info_df)

    # 2.1. Create multi-label binarizer (MLB) encoder for binary evidences
    binary_mlb = bool_ev_encoder(config)
    print('Created multi-label binarizer (MLB) for binary evidences')
    # 2.2. Extract the binary patient evidences from MySQL database (includes the initial evidence from info_df)
    binary_ev = extract_binary_patient_ev(config, info_df, dataset)
    print('Extracted binary patient evidences from MySQL database')
    print(binary_ev)
    # 2.3. Encode the patient evidences using the MLB
    binary_enc_arr = binary_mlb.transform(binary_ev['evidence_code']).astype(np.float64)
    binary_enc = pl.DataFrame({'binary_ev_enc': binary_enc_arr})
    print('Encoded binary evidences')
    print(binary_enc)

    # 3.1. Extract numerical patient evidences
    num_ev = extract_num_patient_ev(config, info_df, dataset)
    print('Extracted numerical patient evidences')
    print(num_ev)
    # 3.2. Rescale the 0-10 scores so they're from 0 to 1 (divide by 10)
    num_enc = num_ev.with_columns([(pl.col(col)/10).alias(col) for col in num_ev.columns if col != 'patient_id']
                                  ).select(pl.exclude('patient_id'))
    print('Encoded/rescaled numerical values')
    print(num_enc)

    # 4.1. Create a dictionary of value MLB encoders per string categorical evidence, and a dictionary of default evidence values
    cat_mlb, cat_defaults = str_val_encoders(config, evidence_type='categorical')
    print('Created MLBs for values of string categorical evidences')
    # 4.2. Extract string categorical patient evidences from MySQL database
    str_cat_ev = extract_str_patient_ev(config, info_df, cat_defaults, evidence_type='categorical', dataset=dataset)
    print('Extracted patient string categorical evidences from MySQL database')
    print(str_cat_ev)
    # 4.3. Encode string categorical evidences with the MLB dictionary
    str_cat_enc = encode_str_evidence(cat_mlb, str_cat_ev)
    print('Encoded string categorical evidences')
    print(str_cat_enc)

    # 5.1. Create a dictionary of value MLB encoders per string multichoice evidence, and a dictionary of default evidence values
    multi_mlb, multi_defaults = str_val_encoders(config, evidence_type='multichoice')
    print('Created MLBs for values of string multichoice evidences')
    # 5.2. Extract string multichoice patient evidences from MySQL database
    str_multi_ev = extract_str_patient_ev(config, info_df, multi_defaults, evidence_type='multichoice', dataset=dataset)
    print('Extracted patient string multichoice evidences from MySQL database')
    print(str_multi_ev)
    # 5.3. Encode string multichoice evidences with the MLB dictionary
    str_multi_enc = encode_str_evidence(multi_mlb, str_multi_ev)
    print('Encoded string multichoice evidences')
    print(str_multi_enc)

    # 6.1. Horizontally concatenate all of the encoded DataFrames with info_df:
    info_df = pl.concat([info_df, binary_enc, num_enc, str_cat_enc, str_multi_enc], how='horizontal')
    print('Appended the encoded values to initial patient info')
    print(info_df)
    # 6.2. From the new info_df, create a single-row DataFrame of input vectors with shape (915,):
    input_vec = create_symptom_vector(info_df)
    print('Created symptom vector')
    print(input_vec)

    # 7.1 Extract patient pathology data from MySQL database
    pathology_df = extract_patient_pathologies(config, dataset)
    print('Extracted patient pathology data from MySQL database')
    print(pathology_df)
    # 7.2. From pathology_df, create a single-row DataFrame of output vectors with shape (49,):
    output_vec = create_pathology_vector(pathology_df)
    print('Created pathology vector')
    print(output_vec)

    # 8.1. Horizontally concatenate input_vec and output_vec together:
    data = pl.concat([input_vec, output_vec],how='horizontal')
    return data

if __name__ == "__main__":
    start = time.perf_counter()
    dataset = 'validate'
    encoded_data = preprocessing_finalized(config, dataset)
    print(f'Preprocessing of {dataset} dataset took {time.perf_counter()-start} seconds.')
    # for label, target in encoded_data:
    #     print(label)
    #     print(target)
    #     break

    base_dir = Path(__file__).parent
    file_path = base_dir / f'{dataset}_data_vectors.parquet'
    encoded_data.write_parquet(file_path)
    print(f'DataFrame saved as Parquet file: {file_path}')
    
    # data_torch = encoded_data.to_torch('dataset',label='pathology_vector')
    # print('Created torch tensor dataset from symptom and pathology vectors')
    # filename = f'{dataset}_data_vectors.pt'
    # torch.save(encoded_data, filename)
