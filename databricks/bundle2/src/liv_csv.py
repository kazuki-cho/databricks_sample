import re, math, uuid
from pyspark.sql import functions as F

def normalize_columns(df):
    def norm(c):
        c = c.replace('\ufeff', '')
        c = re.sub(r'[^0-9a-zA-Z_]', '_', c)
        c = re.sub(r'_+$', '', c)
        return c.lower()
    return df.toDF(*[norm(c) for c in df.columns])


def get_file_size_bytes(path):
    return dbutils.fs.ls(path)[0].size


def calc_partitions(size, target_mb=128, min_p=4, max_p=64):
    p = math.ceil(size / (target_mb * 1024 * 1024))
    return max(min_p, min(p, max_p))


def read_csv_with_fallback(path, schema=None, bad_path=None):
    encodings = ["UTF-8", "UTF-8-SIG", "MS932"]
    for enc in encodings:
        try:
            reader = (spark.read
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .option("encoding", enc)
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
            )
            if bad_path:
                reader = reader.option("badRecordsPath", bad_path)
            if schema:
                reader = reader.schema(schema)

            df = reader.csv(path)
            df.limit(1).collect()
            return df, enc
        except Exception:
            pass
    raise RuntimeError("Encoding detection failed")
