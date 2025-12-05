import time
import psutil
from influxdb_client import InfluxDBClient, Point, WritePrecision
from pymongo import MongoClient
import redis
#from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# ---------------- INFLUXDB CONFIG ----------------
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "HicGzaKcJ-tiqSLYWGKaD6k74WKoOS0as4aLppGdcW3DNRashMrWRjF7QHn-I8N63yYMLAPe_sSjsckmunLE-w=="
INFLUX_ORG = "ensa"
INFLUX_BUCKET = "bench"

client_influx = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client_influx.write_api()


# ---------------- MONGO TEST ----------------
def test_mongo():
    start = time.time()
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    db.testcol.insert_one({"x": 1})
    db.testcol.find_one()
    end = time.time()
    return end - start

# ---------------- REDIS TEST ----------------
def test_redis():
    r = redis.Redis(host="localhost", port=6379)
    start = time.time()
    r.set("key", "value")
    r.get("key")
    end = time.time()
    return end - start

# ---------------- CASSANDRA TEST ----------------
#def test_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect()
    start = time.time()
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy','replication_factor':1}")
    session.execute("CREATE TABLE IF NOT EXISTS test.users (id int PRIMARY KEY, name text)")
    session.execute("INSERT INTO test.users (id, name) VALUES (1, 'test')")
    session.execute("SELECT * FROM test.users")
    end = time.time()
    return end - start

# ---------------- NEO4J TEST ----------------
def test_neo4j():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    start = time.time()
    with driver.session() as session:
        session.run("CREATE (n:Test {x:1})")
        session.run("MATCH (n:Test) RETURN n LIMIT 1")
    end = time.time()
    return end - start

# ---------------- MAIN ----------------
while True:
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent

    mongo_time = test_mongo()
    redis_time = test_redis()
    cass_time = 0
    neo_time = test_neo4j()

    point = Point("benchmark") \
        .tag("machine", "local") \
        .field("cpu", cpu) \
        .field("ram", ram) \
        .field("mongo_latency", mongo_time) \
        .field("redis_latency", redis_time) \
        .field("cassandra_latency", cass_time) \
        .field("neo4j_latency", neo_time)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

    print("Metrics sent !")
    time.sleep(5)
