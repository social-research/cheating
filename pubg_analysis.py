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
    path_to_file = "s3://social-research-cheating/raw-data/" + table_name + "_edges.txt"
    
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


def combine_telemetry_data(day, num_of_files, PATH_TO_DATA):
    """This function combines all telemetry files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store kill records created on the given date
           PATH_TO_DATA: Path to raw data
    """
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
    path_to_file = "s3://social-research-cheating/team-data/" + table_name + "_edges.txt"
    
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


def combine_team_data(day, num_of_files, PATH_TO_DATA):
    """This function combines all team membership data files into one parquet file.
       Args:
           day: Day of the date when matches were played
           num_of_files: The total number of files that store team membership information 
                         created on the given date
           PATH_TO_DATA: Path to the data that contains team membership information
    """
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
            

def create_data_for_obs_mech(PATH_TO_DATA, players):
    """This function creates a dataset that contains the killing records of matches 
       where cheaters killed at least one player including self-loops.
       Args:
           PATH_TO_DATA: Path to a raw dataset in the S3 bucket
           players: Table (dataframe) that contains player data
    """
    spark.read.parquet(PATH_TO_DATA).createOrReplaceTempView("raw_data")
    
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
    legit_logs.write.parquet("s3://social-research-cheating/obs_mech_data.parquet")
    

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

    avg_kill_intervals = spark.sql("""SELECT src AS id, AVG(tsdiff) AS delta  FROM tdiff WHERE tsdiff IS NOT NULL 
                                      GROUP BY src""")
    avg_kill_intervals_df = avg_kill_intervals.toPandas()

    return avg_kill_intervals_df


### Functions for analysing the victimisation-based mechanism


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


def find_legit_cases(new_td, nodes):
    """This function collects the cases where players become cheaters after being killed by cheating.
       Args:
           new_td: Dataframe that contains killings and the level of harm for each killing
           nodes: Table (dataframe) that contains player data
       Returns:
           legit_cases: Dataframe that contains the cases in accordance with the motif
    """
    # First, add information of killers.
    src_info = spark.sql("""SELECT mid, src, start_date AS src_sd, src_bd, src_flag, 
                            dst, dst_bd, dst_flag, time, m_date, damage 
                            FROM new_td t JOIN nodes n ON t.src = n.id""")
    src_info.registerTempTable("src_info")

    # Add information of victims.
    full_info = spark.sql("""SELECT mid, src, src_sd, src_bd, src_flag, 
                             dst, start_date AS dst_sd, dst_bd, dst_flag, time, m_date, damage 
                             FROM src_info s JOIN nodes n ON s.dst = n.id""")
    full_info.registerTempTable("full_info")

    # Find the cases where players adopt cheating after being killed by a cheater.
    legit_cases = spark.sql("""SELECT * FROM full_info WHERE dst_sd != 'NA' AND src_flag == 1 
                               AND dst_flag == 1 AND m_date >= src_sd AND m_date < dst_sd""")
 
    return legit_cases


def get_vic_summary_tab(legit_cases):
    """This function returns a summary table for the victimisation-based mechanism.
       Args:
           legit_cases: Dataframe that contains the cases in accordance with the motif
       Returns:
           add_dates: Dataframe that contains the total number of victimisation experiences 
                      and the number of unique killers.
    """
    # Get the summary data by date for each cheater.
    victims_tab = spark.sql("""SELECT dst AS id, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(dst_sd, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                               TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                               CAST(DATEDIFF(dst_sd, m_date) AS INT) AS period, src AS killer, 
                               COUNT(*) AS exp, SUM(damage) AS sev_exp FROM legit_cases
                               GROUP BY dst, dst_sd, m_date, src""")
    victims_tab.registerTempTable("victims_tab")

    # Get the date when the player was first killed by cheating.
    first_m_dates = spark.sql("""SELECT * 
                                 FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                 AS rownumber FROM victims_tab) WHERE rownumber IN (1)""")
    first_m_dates.registerTempTable("first_m_dates")
    
    # Get the table that contains the total number of victimisation experiences and the number of unique cheaters.
    vic_info = spark.sql("""SELECT id, start_date, SUM(exp) AS total_exp, SUM(sev_exp) AS total_sev_exp, 
                            COUNT(DISTINCT killer) AS uniq_killers 
                            FROM victims_tab GROUP BY id, start_date""")
    vic_info.registerTempTable("vic_info")

    add_dates = spark.sql("""SELECT v.id, v.start_date, f.m_date, f.period, 
                             v.total_exp, v.total_sev_exp, v.uniq_killers 
                             FROM vic_info v LEFT JOIN first_m_dates f ON v.id = f.id""")
    
    return add_dates 
    

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
    

def add_more_info(new_td, nodes):
    """This function adds more information such as the start dates of cheating adoption 
       and cheating flags to the telemetry data.
       Args:
           new_td: Dataframe that contains killings and the level of harm for each killing
           nodes: Table (dataframe) that contains player data
       Returns:
           records: Dataframe that contains the cases in accordance with the motif
    """
    # First, add information of killers.
    src_info = spark.sql("""SELECT mid, src, start_date AS src_sd, src_bd, src_flag, 
                            dst, dst_bd, dst_flag, time, m_date, damage 
                            FROM new_td t JOIN nodes n ON t.src = n.id""")
    src_info.registerTempTable("src_info")

    # Add information of victims.
    full_info = spark.sql("""SELECT mid, src, src_sd, src_bd, src_flag, 
                             dst, start_date AS dst_sd, dst_bd, dst_flag, time, m_date, damage 
                             FROM src_info s JOIN nodes n ON s.dst = n.id""")
    full_info.registerTempTable("full_info")

    # Add information of cheaters.
    add_flags = spark.sql("""SELECT mid, src, src_sd, src_bd, src_flag,
                             CASE WHEN src_bd >= m_date AND src_sd <= m_date 
                             AND src_flag == 1 THEN 1 ELSE 0 END AS src_curr_flag, 
                             dst, dst_sd, dst_bd, dst_flag,
                             CASE WHEN dst_bd >= m_date AND dst_sd <= m_date 
                             AND dst_flag == 1 THEN 1 ELSE 0 END AS dst_curr_flag, time, m_date, damage 
                             FROM full_info ORDER BY mid, time""")
    add_flags.registerTempTable("add_flags")

    legit_matches = spark.sql("""SELECT mid 
                                 FROM (SELECT mid, SUM(src_curr_flag) AS c_kills FROM add_flags GROUP BY mid) 
                                 WHERE c_kills > 0""")
    legit_matches.registerTempTable("legit_matches")

    records = spark.sql("""SELECT r.mid, src, src_sd, src_bd, src_flag, src_curr_flag, 
                           dst, dst_sd, dst_bd, dst_flag, dst_curr_flag, time, m_date, damage 
                           FROM add_flags r JOIN legit_matches l ON r.mid = l.mid""")
    records.registerTempTable("records")
    records = spark.sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY mid ORDER BY time) AS aid FROM records")
    
    return records


def get_obs_summary_tab(records):
    """This function returns a summary table for the observation-based mechanism.
       Args:
           records: Dataframe that contains killings
       Returns:
           add_dates: Dataframe that contains the total number of observations
                      and the number of unique cheaters.
    """
    # Get a list of killings done by cheaters.
    kills_done_by_cheaters = spark.sql("""SELECT mid, src AS killer, time, aid FROM records 
                                          WHERE src_curr_flag = 1""")
    kills_done_by_cheaters.registerTempTable("kills_done_by_cheaters")

    # Get a table of players (both killers and victims) who observed killings done by cheaters when they were alive.
    observers_tab = spark.sql("""SELECT id, 
                                 TO_DATE(CAST(UNIX_TIMESTAMP(start_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS start_date, 
                                 TO_DATE(CAST(UNIX_TIMESTAMP(m_date, 'yyyy-MM-dd') AS TIMESTAMP)) AS m_date, 
                                 CAST(DATEDIFF(start_date, m_date) AS INT) AS period, killer, 
                                 COUNT(*) AS obs, SUM(damage) AS sev_dam 
                                 FROM (SELECT s.mid, s.src AS id, s.src_sd AS start_date, s.src_bd, k.time, s.m_date, 
                                 s.damage, k.killer, k.aid 
                                 FROM records s LEFT JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid < k.aid 
                                 WHERE src_flag == 1 AND src_sd != 'NA' AND src != killer AND src_curr_flag == 0
                                 UNION
                                 SELECT s.mid, s.src AS id, s.src_sd, s.src_bd, k.time, s.m_date, 
                                 s.damage, k.killer, k.aid 
                                 FROM records s LEFT JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                                 WHERE src_flag == 1 AND src_sd != 'NA' AND src != killer AND src_curr_flag == 0
                                 UNION
                                 SELECT s.mid, s.dst AS id, s.dst_sd, s.dst_bd, k.time, s.m_date, 
                                 s.damage, k.killer, k.aid 
                                 FROM records s LEFT JOIN kills_done_by_cheaters k ON s.mid = k.mid AND s.aid > k.aid 
                                 WHERE dst_flag == 1 AND dst_sd != 'NA' AND dst != killer AND dst_curr_flag == 0) 
                                 GROUP BY id, start_date, m_date, killer""")
    observers_tab.registerTempTable("observers_tab")

    # Get the table that contains the total number of observations and the number of unique cheaters.
    obs_info = spark.sql("""SELECT id, start_date, SUM(obs) AS total_obs, SUM(sev_dam) AS total_sev_dam, 
                            COUNT(DISTINCT killer) AS uniq_cheaters FROM observers_tab 
                            GROUP BY id, start_date""")
    obs_info.registerTempTable("obs_info")
    
    # Get the date when the player first observed cheating.
    first_m_dates = spark.sql("""SELECT * 
                                 FROM (SELECT id, m_date, period, ROW_NUMBER() OVER (PARTITION BY id ORDER BY m_date) 
                                 AS rownumber FROM observers_tab) WHERE rownumber IN (1)""")
    first_m_dates.registerTempTable("first_m_dates")
    
    add_dates = spark.sql("""SELECT o.id, o.start_date, f.m_date, f.period, 
                         o.total_obs, o.total_sev_dam, o.uniq_cheaters 
                         FROM obs_info o LEFT JOIN first_m_dates f ON o.id = f.id""")
    
    return add_dates

