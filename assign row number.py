import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def assign_row_numbers(ai_research_papers, ai_authors):
    # Join the DataFrames on 'paper_id'
    combined_df = ai_authors.join(ai_research_papers, on="paper_id", how="inner")
    
    # Define a window specification partitioned by 'paper_id' and ordered by 'author_id'
    window_spec = Window.partitionBy("paper_id").orderBy("author_id")
    
    # Add a row number column
    result_df = combined_df.withColumn("row_number", row_number().over(window_spec))
    
    # Select the required columns
    return result_df.select("paper_id", "author_id", "name", "row_number") 