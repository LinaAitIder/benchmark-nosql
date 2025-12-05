import time
import psutil
from influxdb_client import InfluxDBClient, Point, WritePrecision
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
from datetime import datetime, timedelta
import random
import subprocess
from dotenv import load_dotenv
import os

load_dotenv()

import warnings
warnings.filterwarnings('ignore')

# ---------------- INFLUXDB CONFIG ----------------
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

client_influx = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client_influx.write_api()

# ---------------- CONFIGURATION ----------------
NUM_SENSORS = 100  # Nombre de capteurs
NUM_RECORDS = 100  # Nombre d'enregistrements (changez à 100000 ou 1000000 pour tests finaux)
BATCH_SIZE = 1000  # Taille des batchs pour insertion

# ---------------- GENERATION DE DONNEES ----------------
def generate_sensor_data(record_id):
    """Génère une entrée de capteur IoT"""
    sensor_ids = [f"A{i:02d}" for i in range(1, NUM_SENSORS + 1)]
    base_time = datetime(2025, 12, 3, 18, 0, 0)
    
    return {
        "record_id": record_id,
        "sensor_id": random.choice(sensor_ids),
        "timestamp": (base_time + timedelta(seconds=record_id)).strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": random.randint(30, 90)
    }

# ============================================
# MONGODB - IoT Tests
# ============================================
def test_mongodb_iot():
    print("\n🔵 Testing MongoDB IoT...")
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    collection = db.iot_sensors
    
    collection.drop()
    results = {}
    
    # 1️⃣ BATCH INSERT
    print(f"  📝 BATCH INSERT {NUM_RECORDS} records...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    # Insertion par batch
    for batch_start in range(0, NUM_RECORDS, BATCH_SIZE):
        batch = [generate_sensor_data(i) for i in range(batch_start, min(batch_start + BATCH_SIZE, NUM_RECORDS))]
        collection.insert_many(batch)
    
    insert_time = time.time() - start
    results['insert_time'] = insert_time
    results['insert_throughput'] = NUM_RECORDS / insert_time
    results['insert_cpu'] = psutil.cpu_percent() - cpu_start
    results['insert_mem'] = psutil.virtual_memory().percent - mem_start
    print(f"     ✅ Done in {insert_time:.2f}s ({results['insert_throughput']:.0f} records/sec)")
    
    # 2️⃣ INDEXATION
    print(f"  🔍 CREATE INDEX on timestamp...")
    start = time.time()
    collection.create_index([("timestamp", 1)])
    collection.create_index([("sensor_id", 1)])
    index_time = time.time() - start
    results['index_time'] = index_time
    print(f"     ✅ Done in {index_time:.2f}s")
    
    # 3️⃣ RANGE QUERY (par timestamp)
    print(f"  📖 RANGE QUERY (timestamp range)...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    start_ts = "2025-12-03 18:00:00"
    end_ts = "2025-12-03 19:00:00"
    cursor = collection.find({
        "timestamp": {"$gte": start_ts, "$lte": end_ts}
    })
    count = len(list(cursor))
    
    query_time = time.time() - start
    results['range_query_time'] = query_time
    results['range_query_cpu'] = psutil.cpu_percent() - cpu_start
    results['records_found'] = count
    print(f"     ✅ Done in {query_time:.2f}s ({count} records found)")
    
    # 4️⃣ AGGREGATION (par sensor_id)
    print(f"  📊 AGGREGATION (avg temperature per sensor)...")
    start = time.time()
    
    pipeline = [
        {"$group": {
            "_id": "$sensor_id",
            "avg_temp": {"$avg": "$temperature"},
            "count": {"$sum": 1}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    
    agg_time = time.time() - start
    results['aggregation_time'] = agg_time
    print(f"     ✅ Done in {agg_time:.2f}s ({len(result)} sensors)")
    
    mongo.close()
    return results

# ============================================
# REDIS - IoT Tests
# ============================================
def test_redis_iot():
    print("\n🔴 Testing Redis IoT...")
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()
    
    results = {}
    
    # 1️⃣ BATCH INSERT
    print(f"  📝 BATCH INSERT {NUM_RECORDS} records...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    pipe = r.pipeline()
    for i in range(NUM_RECORDS):
        data = generate_sensor_data(i)
        pipe.hset(f"sensor:{data['sensor_id']}:{i}", mapping=data)
    pipe.execute()
    
    insert_time = time.time() - start
    results['insert_time'] = insert_time
    results['insert_throughput'] = NUM_RECORDS / insert_time
    results['insert_cpu'] = psutil.cpu_percent() - cpu_start
    results['insert_mem'] = psutil.virtual_memory().percent - mem_start
    print(f"     ✅ Done in {insert_time:.2f}s ({results['insert_throughput']:.0f} records/sec)")
    
    # 2️⃣ MEMORY USAGE
    info = r.info('memory')
    memory_used_mb = info['used_memory'] / (1024 * 1024)
    results['memory_used_mb'] = memory_used_mb
    print(f"  💾 Memory used: {memory_used_mb:.2f} MB")
    
    # 3️⃣ KEY SCAN (simulation range query)
    print(f"  📖 SCAN keys (pattern search)...")
    start = time.time()
    
    keys = list(r.scan_iter(match="sensor:A01:*", count=100))
    
    scan_time = time.time() - start
    results['scan_time'] = scan_time
    results['keys_found'] = len(keys)
    print(f"     ✅ Done in {scan_time:.2f}s ({len(keys)} keys found)")
    
    return results

# ============================================
# CASSANDRA - IoT Tests
# ============================================
def test_cassandra_iot():
    print("\n🟣 Testing Cassandra IoT...")
    print("  ⚠️  Using docker exec for Cassandra (Python 3.12+ compatibility)")
    
    results = {}
    
    def run_cql(query, silent=False):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    # Setup
    print("  ⚙️  Setup keyspace and table...")
    run_cql("CREATE KEYSPACE IF NOT EXISTS iotks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("DROP TABLE IF EXISTS iotks.sensors")
    run_cql("""CREATE TABLE iotks.sensors (
        sensor_id text,
        record_id int,
        timestamp text,
        temperature double,
        humidity int,
        PRIMARY KEY (sensor_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)""")
    
    # 1️⃣ BATCH INSERT
    print(f"  📝 BATCH INSERT {NUM_RECORDS} records...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    batch_size = 50  # Plus petit pour éviter timeout
    for batch_start in range(0, NUM_RECORDS, batch_size):
        batch_queries = []
        for i in range(batch_start, min(batch_start + batch_size, NUM_RECORDS)):
            data = generate_sensor_data(i)
            batch_queries.append(
                f"INSERT INTO iotks.sensors (sensor_id, record_id, timestamp, temperature, humidity) "
                f"VALUES ('{data['sensor_id']}', {data['record_id']}, '{data['timestamp']}', {data['temperature']}, {data['humidity']})"
            )
        
        batch_query = "BEGIN BATCH " + "; ".join(batch_queries) + "; APPLY BATCH"
        run_cql(batch_query, silent=True)
    
    insert_time = time.time() - start
    results['insert_time'] = insert_time
    results['insert_throughput'] = NUM_RECORDS / insert_time
    results['insert_cpu'] = psutil.cpu_percent() - cpu_start
    results['insert_mem'] = psutil.virtual_memory().percent - mem_start
    print(f"     ✅ Done in {insert_time:.2f}s ({results['insert_throughput']:.0f} records/sec)")
    
    # 2️⃣ RANGE QUERY
    print(f"  📖 RANGE QUERY (by sensor and timestamp)...")
    start = time.time()
    
    query = "SELECT * FROM iotks.sensors WHERE sensor_id = 'A01' AND timestamp >= '2025-12-03 18:00:00' AND timestamp <= '2025-12-03 19:00:00'"
    run_cql(query)
    
    query_time = time.time() - start
    results['range_query_time'] = query_time
    print(f"     ✅ Done in {query_time:.2f}s")
    
    return results

# ============================================
# NEO4J - IoT Tests (non recommandé pour IoT)
# ============================================
def test_neo4j_iot():
    print("\n🟢 Testing Neo4j IoT...")
    print("  ⚠️  Neo4j n'est pas adapté aux données IoT massives")
    
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    results = {}
    
    with driver.session() as session:
        session.run("MATCH (n:Sensor) DELETE n")
        
        # 1️⃣ INSERT (réduit pour Neo4j)
        limited_records = min(NUM_RECORDS, 1000)  # Limité à 1000 pour Neo4j
        print(f"  📝 INSERT {limited_records} records (limited)...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        
        for i in range(limited_records):
            data = generate_sensor_data(i)
            session.run(
                "CREATE (s:Sensor {record_id: $record_id, sensor_id: $sensor_id, timestamp: $timestamp, temperature: $temperature, humidity: $humidity})",
                **data
            )
        
        insert_time = time.time() - start
        results['insert_time'] = insert_time
        results['insert_throughput'] = limited_records / insert_time
        results['insert_cpu'] = psutil.cpu_percent() - cpu_start
        results['records_inserted'] = limited_records
        print(f"     ✅ Done in {insert_time:.2f}s ({results['insert_throughput']:.0f} records/sec)")
        
        # 2️⃣ QUERY
        print(f"  📖 QUERY (by sensor_id)...")
        start = time.time()
        
        result = session.run("MATCH (s:Sensor {sensor_id: 'A01'}) RETURN count(s) as cnt")
        count = result.single()['cnt']
        
        query_time = time.time() - start
        results['query_time'] = query_time
        results['records_found'] = count
        print(f"     ✅ Done in {query_time:.2f}s ({count} records)")
    
    driver.close()
    return results

# ============================================
# ENVOI DES RESULTATS
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario2_iot"):
    """Envoie les résultats vers InfluxDB"""
    point = Point(scenario).tag("database", db_name)
    
    for key, value in results.items():
        if isinstance(value, (int, float)):
            point.field(key, value)
    
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    print(f"  📊 Résultats envoyés vers InfluxDB pour {db_name}")

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("="*60)
    print("🔥 SCENARIO 2 - DONNÉES MASSIVES (IoT/Logs)")
    print(f"📊 Nombre d'enregistrements: {NUM_RECORDS}")
    print(f"📊 Nombre de capteurs: {NUM_SENSORS}")
    print("="*60)
    
    try:
        # MongoDB
        try:
            print("\n" + "="*60)
            mongo_results = test_mongodb_iot()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Redis
        try:
            print("\n" + "="*60)
            redis_results = test_redis_iot()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # Cassandra
        try:
            print("\n" + "="*60)
            cassandra_results = test_cassandra_iot()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        time.sleep(2)
        
        # Neo4j
        try:
            print("\n" + "="*60)
            neo4j_results = test_neo4j_iot()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        print("\n" + "="*60)
        print("✅ SCENARIO 2 TERMINÉ !")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
        
    finally:
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")