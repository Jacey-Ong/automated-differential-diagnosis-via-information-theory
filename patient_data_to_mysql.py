import time
# import tqdm
import polars as pl
from mysql.connector import connect
# import asyncio
# from mysql.connector.aio import connect as connect_async
from credentials import train_csv, validate_csv, test_csv, config

def parse_patient_csv(train_path: str, validate_path: str, test_path: str) -> pl.LazyFrame:
    train_data = pl.scan_csv(train_path)
    train_data = train_data.with_columns([pl.lit('train').alias('dataset')])

    validate_data = pl.scan_csv(validate_path)
    validate_data = validate_data.with_columns([pl.lit('validate').alias('dataset')])

    test_data = pl.scan_csv(test_csv)
    test_data = test_data.with_columns([pl.lit('test').alias('dataset')])

    data = pl.concat([train_data, validate_data, test_data])
    data = data.with_row_index(name='patient_id',offset=0)
    new_col_names = {
            "AGE": "age",
            "SEX": "sex",
            "PATHOLOGY": "ground_pathology",
            "INITIAL_EVIDENCE": "initial_evidence",
            "EVIDENCES":"evidence",
            "DIFFERENTIAL_DIAGNOSIS":"diff_diag"
        }

    data = data.rename(new_col_names)
    return data

def parse_patient_info(lf: pl.LazyFrame) -> dict[str,pl.LazyFrame]:
    info_lf = lf.select(['patient_id','age','sex','initial_evidence','ground_pathology','dataset'])
    return {'patient_info': info_lf}

def parse_patient_ddx(raw_lf: pl.LazyFrame) -> dict[str,pl.LazyFrame]:
    ddx = raw_lf.select(['patient_id','diff_diag'])
    ddx = ddx.with_columns(pl.col('diff_diag').str.replace_all("'",'"').alias('diff_diag')) \
                                    .with_columns(pl.col('diff_diag').str.json_decode(pl.List(pl.List(pl.Utf8))))
    ddx = ddx.explode('diff_diag')
    ddx = ddx.with_columns([pl.col('diff_diag').list.get(0).alias('pathology'),
                                                pl.col('diff_diag').list.get(1).alias('ddx_prob')
                                                ]).drop('diff_diag')
    ddx = ddx.cast({'ddx_prob':pl.Float32})

    return {'patient_ddx': ddx}

def parse_patient_ev(raw_lf: pl.LazyFrame) -> dict[str,pl.LazyFrame]:
    raw_ev = raw_lf.select(['patient_id','evidence'])
    raw_ev = raw_ev.with_columns(pl.col('evidence').str.replace_all("'",'"')) \
                    .with_columns(pl.col('evidence').str.json_decode(pl.List(pl.Utf8))) \
                    .explode('evidence')

    binary_ev = raw_ev.filter(~pl.col('evidence').str.contains('_@_'))
    binary_ev = binary_ev.with_columns(pl.lit(1).alias('value_bool'))

    cat_ev = raw_ev.filter(pl.col('evidence').str.contains('_@_'),~pl.col('evidence').str.contains('V_'))
    cat_ev = cat_ev.with_columns(pl.col('evidence').str.split(by='_@_').alias('split_evidence'))
    cat_ev = cat_ev.with_columns([
        pl.col("split_evidence").list.get(0).alias("evidence"),
        pl.col("split_evidence").list.get(1).alias("value_num")
    ]).drop("split_evidence")
    cat_ev = cat_ev.cast({'value_num':pl.Int8})

    multi_ev = raw_ev.filter(pl.col('evidence').str.contains('_@_'),pl.col('evidence').str.contains('V_'))
    multi_ev = multi_ev.with_columns(pl.col('evidence').str.split(by='_@_').alias('split_evidence'))
    multi_ev = multi_ev.with_columns([
        pl.col("split_evidence").list.get(0).alias("evidence"),
        pl.col("split_evidence").list.get(1).alias("value")
    ]).drop("split_evidence")
    multi_ev = multi_ev.group_by(['patient_id','evidence']).agg(pl.col('value').str.join(',').alias('value_str'))
    multi_ev = multi_ev.sort('patient_id')
    
    return {'binary_vals': binary_ev, 'categorical_vals': cat_ev, 'multichoice_vals': multi_ev}  

def collect_lf_to_df(lf_dict: dict[pl.LazyFrame]) -> dict[pl.DataFrame]:
    df_dict = {}
    for name, lf in lf_dict.items():
        begin_collect = time.perf_counter()
        df_dict[name] = lf.collect()
        row_count = df_dict[name].height
        print(f"Collected {name} table with {row_count} rows in {time.perf_counter()-begin_collect} seconds.")
    return df_dict

def df_to_csv(df_dict: dict[pl.DataFrame]) -> list[str]:
    csv_paths = []
    for name, df in df_dict.items():
        path = f'{name}.csv'
        csv_paths.append(path)

        df.write_csv(path,
        quote_style='always', 
        null_value='NULL'
        )

        print(f'Wrote {name} DataFrame to CSV.')
        
    return csv_paths

def db_settings(cred: dict, unique_key_check: bool = True):
    cnx = connect(**cred, allow_local_infile=True)
    cur = cnx.cursor()

    cur.execute('SELECT @@local_infile')
    is_local_infile = cur.fetchone()[0]
    if is_local_infile == 0:
        cur.execute('SET GLOBAL local_infile = 1')
        cnx.commit()
    
    if unique_key_check == False:
        # cur.execute("SET autocommit = 0;")
        cur.execute("SET UNIQUE_CHECKS = 0;")
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cnx.commit()
    elif unique_key_check == True: 
        # cur.execute("SET autocommit = 1;")
        cur.execute("SET UNIQUE_CHECKS = 1;")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        cnx.commit()
    
    cur.execute('SELECT @@local_infile')
    is_local_infile = cur.fetchone()[0]
    cur.execute("SELECT @@innodb_buffer_pool_size")
    buffer_size = cur.fetchone()[0]
    cur.execute("SELECT @@autocommit")
    autocommit_status = cur.fetchone()[0]
    cur.execute("SELECT @@unique_checks")
    unique_check_status = cur.fetchone()[0]
    cur.execute("SELECT @@foreign_key_checks")
    foreign_key_check_status = cur.fetchone()[0]

    status_check = f"""
    MySQL setting:
    Buffer size (MB) = {buffer_size/1_048_576}
    Local infile permission = {is_local_infile}
    Autocommit = {autocommit_status}
    Unique Check = {unique_check_status}
    Foreign Key Check = {foreign_key_check_status}
    """

    print(status_check)

    cur.close()
    cnx.close()

def csv_to_db(cred: dict, paths: list[str]) -> dict:
    # this assumes that the Python script is in the same directory as the csv files
    for path in paths:
        begin = time.perf_counter()
        match path: 
            case 'binary_vals.csv':
                final_tbl = 'patient_evidences_tbl'
                to_cols = 'patient_id, evidence, value_bool'
            case 'categorical_vals.csv':
                final_tbl = 'patient_evidences_tbl'
                to_cols = 'patient_id, evidence, value_num'
            case 'multichoice_vals.csv':
                final_tbl = 'patient_evidences_tbl'
                to_cols = 'patient_id, evidence, value_str'
            case 'patient_info.csv':
                final_tbl = 'patient_info_tbl'
                to_cols = ''
            case 'patient_ddx.csv':
                final_tbl = 'patient_ddx_tbl'
                to_cols = ''
        
        try:
            cnx = connect(**cred, allow_local_infile=True)
            cur = cnx.cursor()

            load_query = f"""
            LOAD DATA LOCAL INFILE '{path}'
            INTO TABLE {final_tbl}
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            ({to_cols})
            """

            cur.execute(load_query)
            cnx.commit()

            end = time.perf_counter()
            print(f"Loaded {final_tbl} to database in {end-begin} seconds.")

            cur.close()
            cnx.close()
        
        except Exception as e:
            print(e)
            raise

if __name__ == "__main__":
    start = time.perf_counter()
    patient_raw_lf = parse_patient_csv(train_csv, validate_csv, test_csv)

    info_dict_lf = parse_patient_info(patient_raw_lf)
    ev_dict_lf = parse_patient_ev(patient_raw_lf)
    ddx_dict_lf = parse_patient_ddx(patient_raw_lf)
    all_dict_lf = info_dict_lf | ev_dict_lf | ddx_dict_lf
    parse_end = time.perf_counter()
    print(f'Parsed all patient data done in {parse_end-start} seconds.')

    all_dict_df = collect_lf_to_df(all_dict_lf)
    collect_end = time.perf_counter()
    print(f'Collected all patient DataFrames done in {collect_end-parse_end} seconds.')
    
    all_csv_paths = df_to_csv(all_dict_df)

    db_settings(config, unique_key_check=False)
    csv_to_db(config, all_csv_paths)
    db_settings(config, unique_key_check=True)

    load_db_end = time.perf_counter()
    print(f"Loaded all of patient data to MySQL database in {load_db_end-collect_end} seconds.")

    print(f"Entire process took {load_db_end-start} seconds.")





