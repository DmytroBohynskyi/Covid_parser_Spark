import re
from tempfile import TemporaryFile

import boto3
import botocore
# Pyspark
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from warcio.archiveiterator import ArchiveIterator

# STATIC FILE
INDEXES_URL = 'https://index.commoncrawl.org/collinfo.json'
PREFIX_URL = 'https://commoncrawl.s3.amazonaws.com/'
MONTH_INDEX = ["CC-MAIN-2019-13",
               "CC-MAIN-2019-18",
               "CC-MAIN-2020-16",
               "CC-MAIN-2020-13"]


# ---------------------------- "common crawl" API ----------------------------
def spark_conf():
    """
    This function creates initialize spark objects
    :return: spark object
    """
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlc = SQLContext(sparkContext=sc)
    spark = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()

    return spark, sc, sqlc


def get_response(spark_obj, month_index=0):
    """
    This feature uses an "common crawl" API to recover records data
    :param month: The month we'll be to parse
    :return: List with records parameters
    """

    # CC-index usage
    input_bucket = 'https://commoncrawl.s3.amazonaws.com/cc-index/table/cc-main/warc/'
    records = spark_obj.read.parquet(input_bucket)
    records.createOrReplaceTempView("ccindex")

    query = create_query(month_index)
    records = spark_obj.sql(query)

    return records


def create_query(index=0):
    """
    This feature create Spark SQL query
    :param index: index in MONTH_INDEX :
                0) "CC-MAIN-2019-13",
                1) "CC-MAIN-2019-18",
                2) "CC-MAIN-2020-16",
                3) "CC-MAIN-2020-13"]
    :return:
    """
    month_str = MONTH_INDEX[index]
    fetch_time = ''
    if index == 3:
        fetch_time = "AND fetch_time < '2019-04-01'"
    elif index == 4:
        fetch_time = "AND fetch_time >= '2019-04-01'"
    # Queries for filtering polish webpages from given period of time, which content type is html and returned OK status
    query = f"SELECT collect_list(url) as url, collect_list(fetch_time) as fetch_time, warc_filename, collect_list(" \
            f"warc_record_offset) as warc_record_offset FROM ccindex WHERE crawl = {month_str} " \
            f"AND content_mime_type = 'text/html' AND subset = 'warc' {fetch_time} " \
            f"AND url_host_tld = 'pl' AND fetch_status=200 GROUP BY warc_filename LIMIT 12000 "

    return query


# ---------------------------- Parsing https://commoncrawl.s3.amazonaws.com/ ----------------------------
def get_text(html):
    # Detect encoding and extract plain text from page
    encoding = EncodingDetector.find_declared_encoding(html, is_html=True)
    soup = BeautifulSoup(html, "lxml", from_encoding=encoding)
    for script in soup(["script", "style"]):
        script.extract()

    return soup.get_text(" ", strip=True)


def find_covid(html):
    """
    This function returns the number of count of words like ['.wirus.', '.covid19.'r'.covid\D', '.pandemi.'] in the site
    :param htmls:
    :return: List with number of count ['.wirus.', '.covid19.'r'.covid\D', '.pandemi.':int]
    """
    covid_words = ('.wirus.', '.covid19.'r'.covid\D', '.pandemi.')
    try:
        # Detect encoding and extract plain text from page
        plain = get_text(html=html)
        container = [len(re.findall(word, plain, flags=re.IGNORECASE)) for word in covid_words]

        return container
    # If cannot parse html content, return none
    except:
        return [ 0, 0, 0, 0]


def boto_conf():
    """
    This config create boto configuration
    :return:  boto3 object
    """
    # Connection to AWS S3
    boto_config = botocore.client.Config(signature_version=botocore.UNSIGNED, ead_timeout=180,
                                         retries={'max_attempts': 20})
    s3client = boto3.client('s3', config=boto_config)
    return s3client


def get_http(rows):
    s3client = boto_conf()
    # Create container for warc.gz file
    gz_file = TemporaryFile(mode='w+b', dir=None)
    for row in rows[0]:
        # Download warc.gz from S3 Bucket
        s3client.download_fileobj('commoncrawl', row.warc_filename, gz_file)
        for i, offset in enumerate(row.warc_record_offset):
            # Set offset
            gz_file.seek(offset)
            for record in ArchiveIterator(gz_file):
                # Read record content
                response = record.content_stream().read().strip()
                wirus, covid19, covid, pandemia = find_covid(response)
                yield (row.fetch_time[i].strftime("%Y, %m"), row.fetch_time[i].strftime("%d")), (
                    1, wirus, covid19, covid, pandemia)
                break


if __name__ == '__main__':
    spark, sc, sqlc = spark_conf()
    df = get_response(spark)

    results = df.rdd.repartition(21) \
        .mapPartitions(get_http) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4])) \
        .coalesce(1)

    results.saveAsTextFile('s3://bohynskyi-ase/data/results.csv')
