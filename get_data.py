from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def clean_edges(table_name):
    """Removes matches in special mode where players can revive multiple times 
       from raw data in the given table.
    
    Uses the fact that players should be killed only once as they are given only one life per match.
    Compares the total number of victims and the number of unique victims to detect matches in special mode.
    Assigns the value of 0 if the match is in default mode and the value 1 otherwise for each match.
    
    Args:
        table_name: A string representing a unique text file that contains raw data.
    
    Returns:
        cleaned_logs: A Spark DataFrame without matches in special mode.
    """
    path_to_file = "s3://social-research-cheating/raw_text_files/" + table_name + "_edges.txt"
    
    edge_schema = StructType([StructField("mid", StringType(), True),
                              StructField("aid", StringType(), True),
                              StructField("src", StringType(), True),
                              StructField("dst", StringType(), True),
                              StructField("time", TimestampType(), True),
                              StructField("m_date", StringType(), True)])
    
    edges = spark.read.options(header='false', delimiter='\t').schema(edge_schema).csv(path_to_file)
    edges.registerTempTable("edges")
    
    spark.sql("""SELECT mid, COUNT(*) AS num_row, COUNT(DISTINCT dst) AS uniq_dst FROM edges 
                 GROUP BY mid""").createOrReplaceTempView("num_dst")

    spark.sql("""SELECT mid, num_row, uniq_dst, CASE WHEN num_row == uniq_dst THEN 0 ELSE 1 END AS spec_mod 
                 FROM num_dst""").createOrReplaceTempView("mod_tab")
    
    spark.sql("SELECT mid, num_row FROM mod_tab WHERE spec_mod = 0").createOrReplaceTempView("default_modes")
    
    cleaned_logs = spark.sql("SELECT e.mid, src, dst, time, m_date FROM edges e JOIN default_modes d ON e.mid = d.mid")
    
    return cleaned_logs


def combine_telemetry_data(day, num_of_files, path_to_data):
    """Combines the given telemetry files into one parquet file.
    
    Args:
        day: A number representing the day (from the date) when all matches in the files were played.
        num_of_files: A number representing a unique file that stores some kill records 
            created on the given date.
        path_to_data: A string specifying the path to a file in Amazon S3.
    """
    if day == 1:
        cleaned_tab = clean_edges("td_day_1_1")
        cleaned_tab.write.parquet(path_to_data)
        
        for i in range(2, num_of_files + 1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(path_to_data)
    else:
        for i in range(1, num_of_files + 1):
            new_edges = clean_edges("td_day_" + str(day) + "_" + str(i))
            new_edges.write.mode("append").parquet(path_to_data)


def read_team_data(table_name):
    """Creates a table with team IDs of players and the other one with ranks of teams in team matches.
    
    Args:
        table_name: A string representing a unique text file that stores raw team membership data.
    
    Returns:
        participants: A Spark DataFrame with team IDs of players in team matches.
        ranks: A Spark DataFrame with ranks of teams in team matches.
    """
    file_path = "s3://social-research-cheating/team-data/" + table_name + "_edges.txt"

    edge_schema = StructType([StructField("mid", StringType(), True),
                              StructField("src", StringType(), True),
                              StructField("dst", StringType(), True),
                              StructField("tid", StringType(), True),
                              StructField("time", TimestampType(), True),
                              StructField("mod", StringType(), True),
                              StructField("rank", IntegerType(), True),
                              StructField("m_date", StringType(), True)])

    data = spark.read.options(header='false', delimiter='\t').schema(edge_schema).csv(file_path)
    data.registerTempTable("data")

    participants = spark.sql("SELECT mid, src AS id, tid FROM data UNION SELECT mid, dst, tid FROM data")
    ranks = spark.sql("SELECT DISTINCT mid, tid, mod, rank, m_date FROM data")

    return participants, ranks


def combine_team_data(day, num_of_files, path_to_team_data, path_to_rank_data):
    """Stores the given team membership and rank data files into two different parquet files.
    
       Args:
           day: A number representing the day (from the date) when all matches in the files were played.
           num_of_files: A number representing the total number of files that store team membership data 
               created on the given date.
           path_to_team_data: A string specifying the path to a file that stores team IDs of players 
               in Amazon S3.
           path_to_rank_data: A string specifying the path to a file that stores rankds of teams 
               in Amazon S3.
    """
    if day == 1:
        team_data, rank_data = read_team_data("md_day_1_1")
        team_data.write.parquet(path_to_team_data)
        rank_data.write.parquet(path_to_rank_data)
        
        for i in range(2, num_of_files + 1):
            team_data, rank_data = read_team_data("md_day_" + str(day) + "_" + str(i))
            team_data.write.mode("append").parquet(path_to_team_data)
            rank_data.write.mode("append").parquet(path_to_rank_data)
    else:
        for i in range(1, num_of_files + 1):
            team_data, rank_data = read_team_data("md_day_" + str(day) + "_" + str(i))
            team_data.write.mode("append").parquet(path_to_team_data)
            rank_data.write.mode("append").parquet(path_to_rank_data)
    
    
def get_obs_data(file_path, nodes):
    """Collects killings (including self-loops) of the matches where at least one player was killed by cheating 
       and at least one potential cheater exists.
       
       Args:
           file_path: A string specifying the path to a raw data file in Amazon S3.
           nodes: A Spark DataFrame with player data.
    """
    spark.read.parquet(file_path).createOrReplaceTempView("raw_data")

    spark.sql("""SELECT mid, src, start_date AS src_sd, ban_date AS src_bd, cheating_flag AS src_flag, 
                 CASE WHEN m_date <= ban_date AND m_date >= start_date THEN 1 ELSE 0 END AS src_curr_flag, 
                 dst, time, m_date 
                 FROM raw_data r LEFT JOIN nodes n ON r.src = n.id""").createOrReplaceTempView("add_src_flags")
    
    spark.sql("""SELECT mid, src, src_sd, src_bd, src_flag, src_curr_flag,
                 dst, start_date AS dst_sd, ban_date AS dst_bd, cheating_flag AS dst_flag, 
                 CASE WHEN m_date <= ban_date AND m_date >= start_date THEN 1 ELSE 0 END AS dst_curr_flag,
                 time, m_date 
                 FROM add_src_flags a LEFT JOIN nodes n ON a.dst = n.id""").createOrReplaceTempView("edges")

    summary_table = spark.sql("""SELECT mid, 
                                 SUM(CASE WHEN src_curr_flag = 1 THEN 1 ELSE 0 END) AS num_of_cheaters, 
                                 (SUM(CASE WHEN src_curr_flag = 0 AND src_flag = 1 THEN 1 ELSE 0 END) + 
                                  SUM(CASE WHEN dst_curr_flag = 0 AND dst_flag = 1 THEN 1 ELSE 0 END)) AS potential_cheaters 
                                 FROM edges GROUP BY mid""")
    summary_table.registerTempTable("summary_table")
    
    spark.sql("""SELECT mid FROM summary_table 
                 WHERE num_of_cheaters > 0 AND potential_cheaters > 0""").createOrReplaceTempView("legit_matches")

    legit_logs = spark.sql("""SELECT e.mid, src, src_sd, src_bd, src_curr_flag, src_flag, 
                              dst, dst_sd, dst_bd, dst_curr_flag, dst_flag, time, m_date 
                              FROM edges e JOIN legit_matches l ON e.mid = l.mid""")

    legit_logs.write.parquet("s3://social-research-cheating/edges/obs_data.parquet")

