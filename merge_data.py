import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def get_transitions(file_path, num_of_days):
    """Select the transitions that happened within the given period.
       Args:
           file_path:
           num_of_days:
       Returns:
           transitons:
    """
    data = spark.read.parquet(file_path)
    data.registerTempTable("data")
    transitions = spark.sql("SELECT * FROM data WHERE period <= " + str(num_of_days))
    
    return transitions


def merge_tables(vic_df, obs_df, def_type):
    """Create a table that contains player (cheater) IDs and the start dates of 
       cheating adoption for each cheater and add vic data and obs data
       Args:
           vic_df:
           obs_df:
           def_type: simple - 0, strict - 1
       Returns:
           merged_df:
    """
    dates_from_obs = obs_df[['id', 'start_date']]
    dates_from_vic = vic_df[['id', 'start_date']]
    dates = pd.concat([dates_from_obs, dates_from_vic])
    dates = dates.drop_duplicates(subset=['id', 'start_date'])
    dates_df = spark.createDataFrame(dates)
    dates_df.registerTempTable("dates_df")
    
    add_obs_info = spark.sql("""SELECT t.id, t.start_date, 
                                CASE WHEN total_obs IS NULL THEN 0 ELSE total_obs END AS total_obs 
                                FROM dates_df t LEFT JOIN obs_data o ON t.id = o.id""")
    add_obs_info.registerTempTable("add_obs_info")

    if def_type == 0:
        merged_data = spark.sql("""SELECT t.id, t.start_date, t.total_obs, 
                                   CASE WHEN total_exp IS NULL THEN 0 ELSE total_exp END AS total_exp
                                   FROM add_obs_info t LEFT JOIN vic_data o ON t.id = o.id""")
    else:
        merged_data = spark.sql("""SELECT t.id, t.start_date, t.total_obs,
                                   CASE WHEN total_severe_damage IS NULL THEN 0 ELSE total_severe_damage END 
                                   AS total_exp 
                                   FROM add_obs_info t LEFT JOIN vic_data o ON t.id = o.id""")
    
    merged_df = merged_data.toPandas()
    frequency_table = merged_df.groupby(['total_obs', 'total_exp']).size().reset_index(name="freq")
    
    return frequency_table


def put_summary_table_in_csv_file(file_number, def_type):
    """docstrings
       Args:
           file_number:
           def_type: simple - 0, strict - 1
    """
    vic_data_path = "s3://social-research-cheating/summary-tables/rand-net/vic/vic_" + str(file_number) + ".parquet"
    vic_data = get_transitions(vic_data_path, 7)
    vic_data.registerTempTable("vic_data")
    
    if def_type == 0:
        obs_data_path = "s3://social-research-cheating/summary-tables/rand-net/obs/simple_obs/obs_" + str(file_number) + ".parquet"
    else:
        obs_data_path = "s3://social-research-cheating/summary-tables/rand-net/obs/strict_obs/obs_" + str(file_number) + ".parquet"
    
    obs_data = get_transitions(obs_data_path, 7)
    obs_data.registerTempTable("obs_data")
    
    vic_df = vic_data.toPandas()
    obs_df = obs_data.toPandas()
    
    if def_type == 0:
        frequency_table = merge_tables(vic_df, obs_df, 0)
    else:
        frequency_table = merge_tables(vic_df, obs_df, 1)
    
    frequency_table.to_csv('rand_data_' + str(file_number) + '.csv', index=False)
    

def create_merged_csv_file(emp_file, first_rand_file, num_of_files):
    """Merge multiple csv files into one file.
       Args:
           emp_file:
           first_rand_file:
           num_of_files:
       Returns:
           merged_data:
    """
    emp_data = pd.read_csv(emp_file)
    emp_data = emp_data.rename(columns={'freq': 'E'})

    rand_data = pd.read_csv(first_rand_file)
    merged_data = pd.merge(emp_data, rand_data, on=['total_obs','total_exp'], how='outer')
    merged_data = merged_data.fillna(0).sort_values(by=['total_obs','total_exp'])
    merged_data = merged_data.rename(columns={'freq': 'R1'})

    for i in range(2, num_of_files + 1):
        rand_data = pd.read_csv("rand_data_" + str(i) + ".csv")
        merged_data = pd.merge(merged_data, rand_data, on=['total_obs','total_exp'], how='outer')
        merged_data = merged_data.fillna(0).sort_values(by=['total_obs','total_exp'])
        merged_data = merged_data.rename(columns={'freq': 'R' + str(i)})
    
    return merged_data

