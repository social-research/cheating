from pyspark.sql.types import StructField, StructType, LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def add_column_index(sdf):
    """Add an index column to the given table.
       Args:
           sdf: DataFrame without row numbers
       Returns:
           DataFrame with row numbers as an index.
    """
    new_schema = StructType(sdf.schema.fields + [StructField("ColumnIndex", LongType(), False), ])
    
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)


def permute_node_labels(raw_td, nodes, team_ids):
    """Permute the node labels of the given network randomly and returns a mapping table.
       Args:
           raw_td: Original DataFrame that contains kills
           nodes: DataFrame that contains player information
           team_ids: DataFrame that contains pairs of player and team ID
       Returns:
           mapping: DataFrame that can be used to replace the original node labels
                    with the new node labels. 
    """
    spark.sql("""SELECT mid, m_date, dst AS id, dst_curr_flag AS flag FROM td 
                 UNION 
                 SELECT mid, m_date, src, src_curr_flag FROM td ORDER BY mid""").createOrReplaceTempView("temp_1")

    spark.sql("""SELECT mid, m_date, dst AS id, dst_curr_flag AS flag FROM td 
                 UNION 
                 SELECT mid, m_date, src, src_curr_flag FROM td ORDER BY mid""").createOrReplaceTempView("temp_2")

    # Add team IDs of players for each team match.
    temp_1_with_team_ids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                        CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                        FROM temp_1 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                        ORDER BY mid, tid, flag""")
    temp_1_with_team_ids.registerTempTable("temp_1_with_team_ids")
    
    temp_2_with_team_ids = spark.sql("""SELECT n.mid, m_date, n.id, flag, 
                                        CASE WHEN tid IS NULL THEN 'NA' ELSE tid END AS tid 
                                        FROM temp_2 n LEFT JOIN team_ids t ON n.mid = t.mid AND n.id = t.id 
                                        ORDER BY mid, tid, flag""")
    temp_2_with_team_ids.registerTempTable("temp_2_with_team_ids")

    temp_1_with_col_idx = add_column_index(temp_1_with_team_ids)

    randomized_table = spark.sql("""SELECT mid AS match_id, id AS rand, flag AS rand_flag, tid AS rand_tid 
                                    FROM temp_2_with_team_ids 
                                    ORDER BY mid, tid, flag, rand()""")
    randomized_table.registerTempTable("randomized_table")
    
    temp_2_with_col_idx = add_column_index(randomized_table)

    join_on_index = temp_1_with_col_idx.join(temp_2_with_col_idx,
                                             temp_1_with_col_idx.ColumnIndex == temp_2_with_col_idx.ColumnIndex,
                                             'inner').drop("ColumnIndex")
    join_on_index.registerTempTable("join_on_index")

    mapping = spark.sql("""SELECT mid AS match_id, id AS original, flag AS orig_flag, tid AS orig_tid, 
                           rand AS randomised, rand_flag, rand_tid 
                           FROM join_on_index ORDER BY mid""")
    
    return mapping

