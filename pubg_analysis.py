from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


### Functions for controlling files in the file system and creating datasets.


def clean_edges(table_name):
    """This function removes invalid (null) records and matches in special mode 
       where players can revive multiple times from telemetry data in the given table.
       Args:
           table_name: Name of a table that contains raw telemetry data
       Returns:
           cleaned_logs: Kill records without invalid records and matches in special mode
    """
    path_to_file = "s3://jinny-capstone-data-test/telemetry_data/" + table_name + "_edges.txt"
    
    # Define the structure of telemetry data.
    edgeSchema = StructType([StructField("mid", StringType(), True),
                             StructField("aid", StringType(), True),
                             StructField("src", StringType(), True),
                             StructField("dst", StringType(), True),
                             StructField("time", TimestampType(), True),
                             StructField("m_date", StringType(), True)])
    
    # Read edges from my S3 bucket and create a local table.
    data = spark.read.options(header='false', delimiter='\t').schema(edgeSchema).csv(path_to_file)
    data.registerTempTable("data")
    
    # Get edges from a table and remove invalid records with missing src or dst.
    spark.sql("SELECT * FROM data WHERE src != 'null' AND dst != 'null'").createOrReplaceTempView("edges")
    
    # Remove matches in special mode where players revive multiple times.
    # Players should be killed only once if they are given only one life per match.
    # Compare the total number of victims and the number of unique victims to detect matches in special mode.
    spark.sql("""SELECT mid, COUNT(*) AS num_row, COUNT(DISTINCT dst) AS uniq_dst FROM edges 
                 GROUP BY mid""").createOrReplaceTempView("num_dst")

    # For each match, assign the value of zero if the match is in default mode 
    # and otherwise assign the value of one.
    spark.sql("""SELECT mid, num_row, uniq_dst, CASE WHEN num_row == uniq_dst THEN 0 ELSE 1 END AS spec_mod 
                 FROM num_dst""").createOrReplaceTempView("mod_tab")
    
    # Get match IDs in default mode.
    spark.sql("SELECT mid, num_row FROM mod_tab WHERE spec_mod = 0").createOrReplaceTempView("defalut_mods")
    
    # Get killings done during matches in default mode.
    cleaned_logs = spark.sql("""SELECT e.mid, src, dst, time, m_date 
                                FROM edges e JOIN defalut_mods d ON e.mid = d.mid""")
    
    return cleaned_logs


def combine_telemetry_data(day, num_of_files):
    """This function combines all telemetry files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store kill records created on the given date
    """
    PATH_TO_DATA = "s3://jinny-capstone-data-test/raw_td.parquet"

    if day == 1:
        # Create the first parquet file.
        cleaned_tab = clean_edges("td_day_1_1")
        cleaned_tab.write.parquet(PATH_TO_DATA)
        for i in range(2, num_of_files+1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(PATH_TO_DATA)
    else:
        for i in range(1, num_of_files+1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(PATH_TO_DATA)


def get_participants(table_name):
    """This function creates a tidy table that contains the team IDs of players 
       who took part in teamplay matches.
       Args:
           table_name: Name of a table that contains raw team membership data
       Returns:
           participants: Dataframe that contains the team IDs of players for all teamplay matches.
    """
    path_to_file = "s3://jinny-capstone-data-test/telemetry_data/team_data/" + table_name + "_edges.txt"
    
    # Define the structure of team membership data.
    edgeSchema = StructType([StructField("mid", StringType(), True),
                             StructField("src", StringType(), True),
                             StructField("dst", StringType(), True),
                             StructField("tid", StringType(), True),
                             StructField("time", TimestampType(), True),
                             StructField("mod", StringType(), True),
                             StructField("rank", IntegerType(), True),
                             StructField("m_date", StringType(), True)])
    
    # Read edges from the S3 bucket and create a local table.
    data = spark.read.options(header='false', delimiter='\t').schema(edgeSchema).csv(path_to_file)
    data.registerTempTable("data")
    participants = spark.sql("""SELECT * FROM (SELECT mid, src AS id, tid FROM data 
                                UNION SELECT mid, dst, tid FROM data)""")
    
    return participants


def combine_team_data(day, num_of_files):
    """This function combines all team membership data files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store team membership information 
                         created on the given date
    """
    PATH_TO_DATA = "s3://jinny-capstone-data-test/team_data.parquet"

    if day == 1:
        # Create the first parquet file.
        team_data = get_participants("md_day_1_1")
        team_data.write.parquet(PATH_TO_DATA)
        for i in range(2, num_of_files+1):
            new_team_data = get_participants("md_day_" + str(day) + "_" + str(i))
            new_team_data.write.mode("append").parquet(PATH_TO_DATA)
    else:
        for i in range(1, num_of_files+1):
            new_team_data = get_participants("md_day_" + str(day) + "_" + str(i))
            new_team_data.write.mode("append").parquet(PATH_TO_DATA)
            

def create_data_for_obs_mech(file_path, players):
    """This function creates a dataset that contains the killing records of matches 
       where cheaters killed at least one player including self-loops.
       Args:
           file_path: Path to a raw dataset in the S3 bucket
           players: Table (dataframe) that contains player data
    """
    spark.read.parquet(file_path).createOrReplaceTempView("raw_data")
    
    # Add cheating flags of killers and those of victims.
    # First, add cheating flags of killers.
    spark.sql("""SELECT mid, src, ban_date AS src_bd, cheating_flag AS src_flag, dst, time, m_date 
                 FROM raw_data r JOIN players p ON r.src = p.id""").createOrReplaceTempView("add_src_flags")
    
    # Add cheating flags of victims.
    spark.sql("""SELECT mid, src, src_bd, src_flag, 
                 dst, ban_date AS dst_bd, cheating_flag AS dst_flag, time, m_date 
                 FROM add_src_flags a JOIN players p ON a.dst = p.id""").createOrReplaceTempView("edges")

    # Find matches where at least one cheater took part in (without considering the start date of cheating).
    # For each match, the value of c_cnt should be zero if there is no cheater.
    spark.sql("""SELECT mid, (SUM(src_flag) + SUM(dst_flag)) AS c_cnt FROM edges 
                 GROUP BY mid""").createOrReplaceTempView("count_cheaters")
    spark.sql("SELECT mid FROM count_cheaters WHERE c_cnt > 0").createOrReplaceTempView("legit_mids")
    
    # Extract the records of matches where at least one cheater took part in.
    legit_logs = spark.sql("""SELECT e.mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date 
                              FROM edges e JOIN legit_mids l ON e.mid = l.mid""")
    legit_logs.write.parquet("s3://jinny-capstone-data-test/data_for_obs_mech.parquet")
    

### Functions for analysing cheaters and comparing them with non-cheaters


def get_avg_kill_ratio(kill_logs, death_logs):
    """This function calculates the average kill ratio for each player.
       Args:
           kill_logs: Dataframe that contains kill records of players
           death_logs: Dataframe that contains death records of players
       Returns:
           avg_kill_ratio: Dataframe that contains the values of overall average kill ratio for each player
    """
    # Calculate the number of kills of each player by date.
    kills_by_date = spark.sql("""SELECT src AS id, m_date, COUNT(*) AS kills FROM kill_logs 
                                 GROUP BY src, m_date""")
    kills_by_date_df = kills_by_date.toPandas()

    # Calculate the number of deaths of each player by date.
    deaths_by_date = spark.sql("""SELECT dst AS id, m_date, COUNT(*) AS deaths FROM death_logs 
                                  GROUP BY dst, m_date""")
    deaths_by_date_df = deaths_by_date.toPandas()

    # Create a dataframe that contains both kills and deaths of each player by date.
    dates_from_kills = kills_by_date_df[['id', 'm_date']]
    dates_from_deaths = deaths_by_date_df[['id', 'm_date']]
    temp = pd.concat([dates_from_kills, dates_from_deaths])
    temp = temp.drop_duplicates(subset=['id', 'm_date'])

    # Merge the two dataframes into one.
    kill_info = pd.merge(temp, kills_by_date_df, how='outer', on=['id', 'm_date'])
    kill_info = kill_info.fillna(0)
    merged_df = pd.merge(kill_info, deaths_by_date_df, how='outer', on=['id', 'm_date'])
    merged_df = merged_df.fillna(0)

    # Calculate the overall average kill ratio of each player.
    merged_df['kill_ratio'] = merged_df['kills']/(merged_df['kills']+merged_df['deaths'])
    avg_kill_ratio = merged_df[['id', 'kill_ratio']].groupby(['id'], as_index=False).mean()
    avg_kill_ratio.columns = ['id', 'avg_kill_ratio']

    return avg_kill_ratio


def get_avg_time_diff_between_kills(kill_logs):
    """This function calculates the average time difference between consecutive kills for each player.
       Args:
           kill_logs: Dataframe that contains kill records of players
       Returns:
           avg_kill_intervals_df: Dataframe that contains values of overall average time difference 
                                  between kills for each player
    """
    time_diff_logs = spark.sql("""SELECT mid, src, UNIX_TIMESTAMP(time) AS time, 
                                  UNIX_TIMESTAMP(LAG(time, 1) OVER (PARTITION BY mid, src ORDER BY time)) 
                                  AS prev_time FROM kill_logs ORDER BY mid, src""")
    time_diff_logs.registerTempTable("time_diff_logs")

    tdiff = spark.sql("""SELECT mid, src, time, (time - prev_time) AS tsdiff FROM time_diff_logs 
                         ORDER BY src, mid, time""")
    tdiff.registerTempTable("tdiff")

    avg_kill_intervals = spark.sql("""SELECT src, AVG(tsdiff) AS delta  FROM tdiff WHERE tsdiff IS NOT NULL 
                                      GROUP BY src""")
    avg_kill_intervals_df = avg_kill_intervals.toPandas()

    return avg_kill_intervals_df


### Functions for analysing the victimisation-based mechanism


def get_legit_cases(records, period):
    """This function gets the killings or observations in accordance with the motif.
       Args:
           records: Dataframe that contains kills or observations and player information
           period: Length of the transition period
       Returns:
           res: Dataframe that contains the cases in accordance with the motif
    """
    # Find cases which describe the given motif.
    legit_cases = spark.sql("""SELECT * FROM records 
                               WHERE dst_sd != 'NA' AND src_flag == 1 AND dst_flag == 1 AND m_date >= src_sd 
                               AND m_date < dst_sd""")
    legit_cases.registerTempTable("legit_cases")
    
    legit_cases_df = legit_cases.toPandas()
    legit_cases_df['m_date'] = pd.to_datetime(legit_cases['m_date'])
    legit_cases_df['dst_sd'] = pd.to_datetime(legit_cases['dst_sd'])
    legit_cases_df['period'] = (legit_cases_df['dst_sd'] - legit_cases_df['m_date']).astype('timedelta64[D]')
    
    legit_cases = spark.createDataFrame(legit_cases_df)
    legit_cases.registerTempTable("legit_cases")

    # Get the transitions from non-cheater to cheater that happened within the given period of time.
    res = spark.sql("SELECT * FROM legit_cases WHERE period <= " + str(period))
    
    return res


def get_cases_with_severe_harm(records, legit_cases, perc):
    """This function gets the killings or observations in accordance with the motif 
       and the given level of harm.
       Args:
           records: Dataframe that contains kills and player information
           legit_cases: Dataframe that contains the cases which describes the motif 
                        without considering the level of harm
           perc: Number between 0 and 100 which represents the percentage
       Returns:
           res: Dataframe that contains the cases in accordance with the motif
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("SELECT mid, COUNT(*) AS num_rows FROM records GROUP BY mid ORDER BY mid")
    num_of_rows.registerTempTable("num_of_rows")

    fin_c_match_logs = spark.sql("""SELECT c.mid, src, src_flag, dst, dst_flag, time, num_rows 
                                    FROM records c JOIN num_of_rows n ON c.mid = n.mid""")
    fin_c_match_logs.registerTempTable("fin_c_match_logs")

    # Get the ranking of each victim for each match.
    rank_tab = spark.sql("""SELECT mid, src, src_flag, dst, dst_flag, time, num_rows, 
                            RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS ranking 
                            FROM fin_c_match_logs ORDER BY mid, time DESC""")
    rank_tab.registerTempTable("rank_tab")

    # Get victims who were killed after getting into the top 30 percent.
    # The value of damage is one if the victim got killed after getting into the top 30 percent, and otherwise zero.
    top_percent = spark.sql("SELECT mid, src, dst, time, num_rows, ranking, CASE WHEN ((ranking + 1) / num_rows) > " + 
                            str(perc/100) + " THEN 0 ELSE 1 END AS damage FROM rank_tab")
    top_percent.registerTempTable("top_percent")

    # Add the information about damage into the cases above.
    tab_with_damage = spark.sql("""SELECT l.mid, l.src, l.dst, l.time, l.period, damage 
                                   FROM legit_cases l JOIN top_percent t ON l.mid = t.mid AND l.dst = t.dst""")
    tab_with_damage.registerTempTable("tab_with_damage")

    # Among paths found above, find the cases where non-cheaters were harmed severly by cheating.
    res = spark.sql("SELECT * FROM tab_with_damage WHERE damage == 1")

    return res


def with_column_index(sdf):
    """This function adds an index column to the given dataframe.
       Args:
           sdf: Dataframe without an index
       Returns:
           Dataframe that contains row numbers as an index.
    """
    new_schema = StructType(sdf.schema.fields + [StructField("ColumnIndex", LongType(), False),])
    
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)


def permute_node_labels(raw_td, nodes, team_ids):
    """This function permutes the node labels of the given network randomly and returns a mapping table.
       Args:
           raw_td: Original dataframe that contains kills
           nodes: Dataframe that contains player information
           team_ids: Dataframe that contains pairs of player and team ID
       Returns:
           mapping: Dataframe that can be used to replace the original node labels 
                    with the new node labels. 
    """
    spark.sql("""SELECT mid, m_date, dst AS id FROM td 
                 UNION SELECT mid, m_date, src FROM td ORDER BY mid""").createOrReplaceTempView("temp_1")
    spark.sql("""SELECT mid, m_date, dst AS id FROM td 
                 UNION SELECT mid, m_date, src FROM td ORDER BY mid""").createOrReplaceTempView("temp_2")

    # Add the cheating flags of cheaters.
    new_temp_1 = spark.sql("""SELECT mid, m_date, t.id, 
                              CASE WHEN m_date <= ban_date AND m_date >= start_date 
                              AND cheating_flag = 1 THEN 1 ELSE 0 END AS flag 
                              FROM temp_1 t JOIN nodes n ON t.id = n.id""")
    new_temp_2 = spark.sql("""SELECT mid, m_date, t.id, 
                              CASE WHEN m_date <= ban_date AND m_date >= start_date 
                              AND cheating_flag = 1 THEN 1 ELSE 0 END AS flag 
                              FROM temp_2 t JOIN nodes n ON t.id = n.id""")
    new_temp_1.registerTempTable("new_temp_1")
    new_temp_2.registerTempTable("new_temp_2")

    # Add team information of players for each teamplay match.
    temp_1_with_tids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                    CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                    FROM new_temp_1 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                    ORDER BY mid, tid, flag""")
    temp_1_with_tids.registerTempTable("temp_1_with_tids")
    
    temp_2_with_tids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                    CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                    FROM new_temp_2 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                    ORDER BY mid, tid, flag""")
    temp_2_with_tids.registerTempTable("temp_2_with_tids")

    df1_ci = with_column_index(temp_1_with_tids)

    # Create a table for mapping.
    rand_tab = spark.sql("""SELECT mid AS match_id, id AS rand, flag AS rand_flag, tid AS rand_tid 
                            FROM temp_2_with_tids ORDER BY mid, tid, flag, rand()""")
    rand_tab.registerTempTable("rand_tab")
    
    df2_ci = with_column_index(rand_tab)

    join_on_index = df1_ci.join(df2_ci, df1_ci.ColumnIndex == df2_ci.ColumnIndex, 'inner').drop("ColumnIndex")
    join_on_index.registerTempTable("join_on_index")

    mapping = spark.sql("""SELECT mid AS match_id, id AS original, flag AS orig_flag, tid AS orig_tid, 
                           rand AS randomised, rand_flag, rand_tid 
                           FROM join_on_index ORDER BY mid""")
    
    return mapping


def plot_dist_of_test_stats(num_of_motifs, test_stats_lst):
    """This function plots the distribution of the given test statistics.
       Args:
           num_of_motifs: Number of motifs on the empirical network
           test_stats_lst: List of test statistics from randomised networks
    """
    test_df = pd.DataFrame(test_stats_lst)
    test_df.columns = ['z-scores']
    zscore_df = (test_df - test_df.mean())/test_df.std(ddof=0)

    rand_mean = test_df.mean()
    rand_std = test_df.std(ddof=0)
    observed_stat = (num_of_motifs - rand_mean) / rand_std
    observed_stat = round(float(np.float64(observed_stat)), 4)

    bins = np.arange(0, zscore_df['z-scores'].max() + 1.5) - 0.5
    fig = zscore_df.hist(column='z-scores', histtype='step', bins=20)
    plt.xlabel("Test statistics")
    plt.ylabel("Frequency")
    plt.axvline(observed_stat, color='r', linewidth=2)
    # plt.xlim(xmin=-4)
    # plt.xlim(xmax=4)
    # plt.ylim(ymax=20)
    plt.title("")
    plt.tight_layout()
    plt.show()
    

### Functions for analysing the observation-based mechanism


def add_level_of_harm(td, perc):
    """This function checks whether a killing is critical or not 
       in accordance with the given level of harm.
       Args:
           td: Dataframe that contains killings
           perc: Number between 0 and 100 which represents the percentage
       Returns:
           res: Dataframe that contains the cases in accordance with the motif
    """
    # Count the number of rows (unique victims) for each match.
    num_of_rows = spark.sql("SELECT mid, COUNT(*) AS num_rows FROM td GROUP BY mid ORDER BY mid")
    num_of_rows.registerTempTable("num_of_rows")

    fin_c_match_logs = spark.sql("""SELECT c.mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, 
                                    time, m_date, num_rows FROM td c JOIN num_of_rows n ON c.mid = n.mid""")
    fin_c_match_logs.registerTempTable("fin_c_match_logs")

    # Get the ranking of each victim for each match.
    rank_tab = spark.sql("""SELECT mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date, num_rows, 
                            RANK(dst) OVER (PARTITION BY mid ORDER BY time DESC) AS ranking 
                            FROM fin_c_match_logs ORDER BY mid, time DESC""")
    rank_tab.registerTempTable("rank_tab")

    # Get victims who were killed after getting into the top 30 percent.
    # The value of damage is one if the victim got killed after getting into the top 30 percent, and otherwise zero.
    top_percent = spark.sql("SELECT mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date, " + 
                            "CASE WHEN ((ranking + 1) / num_rows) > " + str(perc/100) + 
                            " THEN 0 ELSE 1 END AS damage FROM rank_tab")
    top_percent.registerTempTable("top_percent")

    # Among paths found above, find the cases where non-cheaters were harmed severly by cheating.
    res = spark.sql("SELECT mid, src, src_bd, src_flag, dst, dst_bd, dst_flag, time, m_date, damage FROM top_percent")

    return res

