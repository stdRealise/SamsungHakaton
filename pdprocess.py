import os
import re
import pdfplumber
import pandas as pd
from pdsearcher import PDSearcher
from pdextractor import PDExtractor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row

# --- CONFIG ---
DATA_PATH = "/DATA/ПДнDataset/share"
OUTPUT_PATH = "./results_local"
SUPPORTED_EXT = {'.pdf', '.txt', '.html', '.csv', '.parquet'}

os.environ.update({
    "PYSPARK_PYTHON": "/opt/python3.10/bin/python3.10",
    "PYSPARK_DRIVER_PYTHON": "/opt/python3.10/bin/python3.10",
    "PYTHONPATH": '/opt/jupyter_venvs/student35282_venv/lib/python3.10/site-packages'
})


class PDProcess:
    """Главный класс управления процессом запуска кода"""
    def __init__(self, spark_session):
        self.spark = spark_session

    @staticmethod
    def determine_uz_level(findings: dict) -> str:
        critical_keys = {'snils', 'inn', 'passport', 'bank_card'}
        identity_keys = {'name', 'phone', 'email'}
        found_keys = set(findings.keys())
        if found_keys.intersection(critical_keys):
            return "УЗ-3"
        simple_found = found_keys.intersection(identity_keys)
        if len(simple_found) >= 2:
            return "УЗ-4"
        return ""

    def run(self, paths: List[str]):
        schema = StructType([
            StructField("path", StringType(), True),
            StructField("pd_categories", StringType(), True),
            StructField("pd_count", IntegerType(), True),
            StructField("uz_level", StringType(), True),
        ])
        path_rdd = self.spark.sparkContext.parallelize(paths, numSlices=50)
        
        def mapper(path_iterator):
            ext = PDExtractor()
            src = PDSearcher()
            for p in path_iterator:
                try:
                    text = ext.extract(p)
                    res = src.search(text)
                    cnt = sum(res.values())
                    if cnt > 0:
                        yield (p, ", ".join(sorted(res.keys())), cnt, self.determine_uz_level(res))
                except:
                    yield (p, "Error", 0, "Unknown")

        results_df = self.spark.createDataFrame(path_rdd.mapPartitions(mapper), schema=schema)
        return results_df.filter(F.col("pd_count") > 0)


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
        .master("local[*]")
        .appName("PDn_App_Local")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark_session = SparkSession.builder.master("local[*]").appName("PDn_Final_Audit").getOrCreate()
    

    all_paths = [os.path.join(r, f) for r, d, fs in os.walk(DATA_PATH) 
                 for f in fs if os.path.splitext(f)[1].lower() in SUPPORTED_EXT]

    process = PDProcess(spark_session)
    final_df = process.run(all_paths)
    
    # Сохранение и вывод
    final_df.repartition(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)