import time
import polars as pl
import asyncio
from mysql.connector.aio import connect as connect_async
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

# def collect_lf_to_df(lf_dict: dict[pl.LazyFrame]) -> dict[pl.DataFrame]:
#     df_dict = {}
#     for name, lf in lf_dict.items():
#         begin_collect = time.perf_counter()
#         df_dict[name] = lf.collect()
#         row_count = df_dict[name].height
#         print(f"Collected {name} table with {row_count} rows in {time.perf_counter()-begin_collect} seconds.")
#     return df_dict

def collect_lf_to_df(name: str, lf: pl.LazyFrame) -> pl.DataFrame:
    begin_collect = time.perf_counter()
    df = lf.collect()
    row_count = df.height
    print(f"Collected {name} table with {row_count} rows in {time.perf_counter()-begin_collect} seconds.")

    return df

def df_to_csv(name: str, df: pl.DataFrame) -> str:
    path = f'{name}.csv'

    df.write_csv(path,
    quote_style='always', 
    null_value='NULL'
    )

    print(f'Wrote {name} DataFrame to CSV.')
        
    return path

async def csv_to_db(cred: dict, path: str):
    # this assumes that the Python script is in the same directory as the csv files
    begin_load = time.perf_counter()
    match path: 
        case 'binary_vals.csv':
            # staging_table = 'patient_evidences_staging'
            final_table = 'patient_evidences_tbl'
            to_cols = '(patient_id, evidence, value_bool)'
        case 'categorical_vals.csv':
            # staging_table = 'patient_evidences_staging'
            final_table = 'patient_evidences_tbl'
            to_cols = '(patient_id, evidence, value_num)'
        case 'multichoice_vals.csv':
            # staging_table = 'patient_evidences_staging'
            final_table = 'patient_evidences_tbl'
            to_cols = '(patient_id, evidence, value_str)'
        case 'patient_info.csv':
            # staging_table = 'patient_info_staging'
            final_table = 'patient_info_tbl'
            to_cols = ''
        case 'patient_ddx.csv':
            # staging_table = 'patient_ddx_staging'
            final_table = 'patient_ddx_tbl'
            to_cols = ''
        
    try:
        async with await connect_async(**cred, allow_local_infile=True) as cnx:
            async with await cnx.cursor() as cur:
                load_query = f"""
                LOAD DATA LOCAL INFILE '{path}'
                INTO TABLE {final_table}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS
                {to_cols}
                """
                await cur.execute(load_query)
                await cnx.commit() 
    
    except Exception as e:
        print(e)
        raise

    end_load = time.perf_counter()
    print(f"{path} loaded to MySQL database in {end_load - begin_load} seconds")

async def df_to_db(cred: dict, table_name: str, df: pl.DataFrame):
    # df = await collect_async_lf_to_df(table_name, lf)
    csv_path = await asyncio.create_task(asyncio.to_thread(df_to_csv, table_name, df))
    await csv_to_db(cred, csv_path)

async def db_settings(cred: dict, unique_key_check: bool = True):
    async with await connect_async(**cred, allow_local_infile=True) as cnx:
        async with await cnx.cursor() as cur:
            await cur.execute('SELECT @@local_infile')
            is_local_infile = (await cur.fetchone())[0]

            if is_local_infile == 0:
                await cur.execute('SET GLOBAL local_infile = 1')
                await cnx.commit()

            if unique_key_check == False:
                # await cur.execute("SET autocommit = 0;")
                await cur.execute("SET UNIQUE_CHECKS = 0;")
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                await cnx.commit()
            elif unique_key_check == True:
                # await cur.execute("SET autocommit = 1;")
                await cur.execute("SET UNIQUE_CHECKS = 1;")
                await cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
                await cnx.commit()
            
            await cur.execute('SELECT @@local_infile')
            is_local_infile = (await cur.fetchone())[0]
            await cur.execute("SELECT @@innodb_buffer_pool_size")
            buffer_size = (await cur.fetchone())[0]
            await cur.execute("SELECT @@autocommit")
            autocommit_status = (await cur.fetchone())[0]
            await cur.execute("SELECT @@unique_checks")
            unique_check_status = (await cur.fetchone())[0]
            await cur.execute("SELECT @@foreign_key_checks")
            foreign_key_check_status = (await cur.fetchone())[0]

            status_check = f"""
            MySQL setting:
            Buffer size (MB) = {buffer_size/1_048_576}
            Local infile permission = {is_local_infile}
            Autocommit = {autocommit_status}
            Unique Check = {unique_check_status}
            Foreign Key Check = {foreign_key_check_status}
            """

            print(status_check)

async def df_to_db_concur(dict_lf: dict[str,pl.DataFrame], cred: dict, optimize_settings: bool = True):
    coroutines_start = time.perf_counter()

    # MySQL settings and optimizations. Optional, if these were already done in your MySQL database directly.
    if optimize_settings:
        await db_settings(config, unique_key_check=False)

    coroutines_start = time.perf_counter()

    # Perform the entire chain, from LazyFrame to database loading, asynchronously per table. 
    coroutine_list = []
    for name, lf in dict_lf.items():
        coroutine_list.append(df_to_db(config, name, lf))
    
    await asyncio.gather(*coroutine_list, return_exceptions=True)

    # Turn off MySQL settings and optimizations.
    if optimize_settings:
        await db_settings(config, unique_key_check=True)

    coroutines_end = time.perf_counter()
    print(f'Loading {len(coroutine_list)} fact tables into MySQL database completed in {coroutines_end-coroutines_start} seconds!')

if __name__ == "__main__":
    start = time.perf_counter()
    # The parsing of data into LazyFrames. This is very quick and so it is done synchronously.
    patient_raw_lf = parse_patient_csv(train_csv, validate_csv, test_csv)
    info_dict_lf = parse_patient_info(patient_raw_lf)
    ev_dict_lf = parse_patient_ev(patient_raw_lf)
    ddx_dict_lf = parse_patient_ddx(patient_raw_lf)
    all_dict_lf = info_dict_lf | ev_dict_lf | ddx_dict_lf
    parse_end = time.perf_counter()
    print(f'Parsed all patient data done in {parse_end-start} seconds.')
        
    # Collecting LazyFrames into DataFrames is faster when done synchronously than asynchronously. 
    all_dict_df = {}
    for name, lf in all_dict_lf.items():
        df = collect_lf_to_df(name, lf)
        all_dict_df[name] = df
    print(f'Collected all {len(all_dict_df)} LazyFrames into DataFrames in {time.perf_counter()-parse_end} seconds')
    print(all_dict_df)

    # The asynchronous part, where each DataFrame is saved as CSV and loaded to the database concurrently.
    asyncio.run(df_to_db_concur(all_dict_df, config, optimize_settings=True))