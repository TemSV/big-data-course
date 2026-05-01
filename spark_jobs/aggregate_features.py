import sys
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, round

def main():
    if len(sys.argv) != 2:
        print("Usage: aggregate_features.py <input_path>")
        sys.exit(1)

    input_path = sys.argv[1]

    spark = SparkSession.builder \
        .appName("ML_Feature_Aggregation") \
        .getOrCreate()

    print(f"Reading enriched data from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    aggregated_df = df.groupBy("user_id").agg(
        sum("session_duration_sec").alias("total_session_duration"),
        round(avg("clicks"), 2).alias("avg_clicks"),
        count("user_id").alias("total_sessions")
    )

    print("Writing to ClickHouse via HTTP API...")
    rows = aggregated_df.collect()

    csv_lines = ["user_id,total_session_duration,avg_clicks,total_sessions\n"]
    for row in rows:
        csv_lines.append(f"{row.user_id},{row.total_session_duration},{row.avg_clicks},{row.total_sessions}\n")

    csv_data = "".join(csv_lines).encode('utf-8')
    url = "http://clickhouse:8123/?query=INSERT+INTO+default.user_aggregated_features+FORMAT+CSVWithNames"

    req = urllib.request.Request(url, data=csv_data, method='POST')
    req.add_header('X-ClickHouse-User', 'default')
    req.add_header('X-ClickHouse-Key', 'clickhouse_pass')

    with urllib.request.urlopen(req) as response:
        print(f"ClickHouse response code: {response.getcode()}")

    spark.stop()

if __name__ == "__main__":
    main()