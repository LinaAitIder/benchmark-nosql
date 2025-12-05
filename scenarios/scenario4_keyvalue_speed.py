import time
import psutil
from influxdb_client import InfluxDBClient, Point
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
import random
import string
import subprocess
from dotenv import load_dotenv
import os

load_dotenv()

import warnings
warnings.filterwarnings('ignore')

# ---------------- INFLUXDB CONFIG ----------------
INFLUX_URL = "http://localhost:8086"
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
NUM_OPS = 100  # 100k opérations SET/GET
TTL_SECONDS = 60  # Time to live pour expiration

# ---------------- GENERATION DE DONNEES ----------------
def generate_random_key():
    """Génère une clé aléatoire"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def generate_random_value():
    """Génère une valeur aléatoire (token/session)"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=64))

# ============================================
# REDIS - Key-Value (OPTIMAL)
# ============================================
def test_redis_keyvalue():
    print("\n🔴 Testing Redis Key-Value...")
    print("  ⚡ Redis est conçu pour cela !")
    
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()
    
    results = {}
    
    # 1️⃣ SET Operations
    print(f"  📝 SET {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    keys_data = [(generate_random_key(), generate_random_value()) for _ in range(NUM_OPS)]
    
    for key, value in keys_data:
        r.set(key, value)
    
    set_time = time.time() - start
    results['set_time'] = float(set_time)
    results['set_throughput'] = float(NUM_OPS / set_time)
    results['set_latency_ms'] = float((set_time / NUM_OPS) * 1000)
    results['set_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['set_mem'] = float(psutil.virtual_memory().percent - mem_start)
    print(f"     ✅ Done in {set_time:.2f}s ({results['set_throughput']:.0f} ops/sec)")
    print(f"     ⏱️  Latency: {results['set_latency_ms']:.4f}ms per operation")
    
    # 2️⃣ GET Operations
    print(f"  📖 GET {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    for key, _ in keys_data:
        r.get(key)
    
    get_time = time.time() - start
    results['get_time'] = float(get_time)
    results['get_throughput'] = float(NUM_OPS / get_time)
    results['get_latency_ms'] = float((get_time / NUM_OPS) * 1000)
    results['get_cpu'] = float(psutil.cpu_percent() - cpu_start)
    print(f"     ✅ Done in {get_time:.2f}s ({results['get_throughput']:.0f} ops/sec)")
    print(f"     ⏱️  Latency: {results['get_latency_ms']:.4f}ms per operation")
    
    # 3️⃣ SET with TTL (Expiration)
    print(f"  ⏰ SET with TTL ({TTL_SECONDS}s)...")
    start = time.time()
    
    ttl_keys = 10000
    for i in range(ttl_keys):
        key = f"session:{i}"
        value = generate_random_value()
        r.setex(key, TTL_SECONDS, value)
    
    ttl_time = time.time() - start
    results['ttl_set_time'] = float(ttl_time)
    results['ttl_throughput'] = float(ttl_keys / ttl_time)
    print(f"     ✅ Done in {ttl_time:.2f}s ({results['ttl_throughput']:.0f} ops/sec)")
    
    # 4️⃣ Pipeline Operations (bulk)
    print(f"  🚀 Pipeline operations...")
    start = time.time()
    
    pipe = r.pipeline()
    pipeline_ops = 50000
    for i in range(pipeline_ops):
        pipe.set(f"pipe:{i}", f"value_{i}")
    pipe.execute()
    
    pipeline_time = time.time() - start
    results['pipeline_time'] = float(pipeline_time)
    results['pipeline_throughput'] = float(pipeline_ops / pipeline_time)
    print(f"     ✅ Done in {pipeline_time:.2f}s ({results['pipeline_throughput']:.0f} ops/sec)")
    
    # 5️⃣ Memory Info
    info = r.info('memory')
    results['memory_used_mb'] = float(info['used_memory'] / (1024 * 1024))
    print(f"  💾 Memory used: {results['memory_used_mb']:.2f} MB")
    
    return results

# ============================================
# MONGODB - Key-Value (SUBOPTIMAL)
# ============================================
def test_mongodb_keyvalue():
    print("\n🔵 Testing MongoDB Key-Value...")
    print("  ⚠️  MongoDB n'est pas optimal pour simple GET/SET")
    
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    collection = db.keyvalue
    
    collection.drop()
    
    results = {}
    
    # 1️⃣ SET (INSERT)
    print(f"  📝 SET {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    keys_data = [(generate_random_key(), generate_random_value()) for _ in range(NUM_OPS)]
    docs = [{"_id": key, "value": value} for key, value in keys_data]
    
    # Insertion par batch
    batch_size = 1000
    for i in range(0, len(docs), batch_size):
        collection.insert_many(docs[i:i+batch_size])
    
    set_time = time.time() - start
    results['set_time'] = float(set_time)
    results['set_throughput'] = float(NUM_OPS / set_time)
    results['set_latency_ms'] = float((set_time / NUM_OPS) * 1000)
    results['set_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['set_mem'] = float(psutil.virtual_memory().percent - mem_start)
    print(f"     ✅ Done in {set_time:.2f}s ({results['set_throughput']:.0f} ops/sec)")
    
    # 2️⃣ GET (FIND)
    print(f"  📖 GET {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    for key, _ in keys_data:
        collection.find_one({"_id": key})
    
    get_time = time.time() - start
    results['get_time'] = float(get_time)
    results['get_throughput'] = float(NUM_OPS / get_time)
    results['get_latency_ms'] = float((get_time / NUM_OPS) * 1000)
    results['get_cpu'] = float(psutil.cpu_percent() - cpu_start)
    print(f"     ✅ Done in {get_time:.2f}s ({results['get_throughput']:.0f} ops/sec)")
    print(f"     ⏱️  Latency: {results['get_latency_ms']:.4f}ms per operation")
    
    # 3️⃣ TTL (avec index)
    print(f"  ⏰ Creating TTL index...")
    start = time.time()
    
    # MongoDB TTL index
    collection.create_index("created_at", expireAfterSeconds=TTL_SECONDS)
    
    from datetime import datetime
    ttl_docs = [
        {"_id": f"session:{i}", "value": generate_random_value(), "created_at": datetime.utcnow()}
        for i in range(10000)
    ]
    collection.insert_many(ttl_docs)
    
    ttl_time = time.time() - start
    results['ttl_set_time'] = float(ttl_time)
    print(f"     ✅ Done in {ttl_time:.2f}s")
    
    mongo.close()
    return results

# ============================================
# CASSANDRA - Key-Value (SUITABLE)
# ============================================
def test_cassandra_keyvalue():
    print("\n🟣 Testing Cassandra Key-Value...")
    print("  ℹ️  Cassandra peut faire du key-value")
    
    results = {}
    
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    # Setup
    run_cql("CREATE KEYSPACE IF NOT EXISTS kvks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("DROP TABLE IF EXISTS kvks.keyvalue")
    run_cql("CREATE TABLE kvks.keyvalue (key text PRIMARY KEY, value text)")
    
    # 1️⃣ SET (INSERT)
    limited_ops = min(NUM_OPS, 10000)  # Limité pour éviter timeout
    print(f"  📝 SET {limited_ops} keys (limited)...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    # Batch insert
    batch_size = 100
    for batch_start in range(0, limited_ops, batch_size):
        batch_queries = []
        for i in range(batch_start, min(batch_start + batch_size, limited_ops)):
            key = f"key_{i}"
            value = generate_random_value()
            batch_queries.append(f"INSERT INTO kvks.keyvalue (key, value) VALUES ('{key}', '{value}')")
        
        batch_query = "BEGIN BATCH " + "; ".join(batch_queries) + "; APPLY BATCH"
        run_cql(batch_query)
    
    set_time = time.time() - start
    results['set_time'] = float(set_time)
    results['set_throughput'] = float(limited_ops / set_time)
    results['set_cpu'] = float(psutil.cpu_percent() - cpu_start)
    print(f"     ✅ Done in {set_time:.2f}s ({results['set_throughput']:.0f} ops/sec)")
    
    # 2️⃣ GET (SELECT)
    print(f"  📖 GET {min(limited_ops, 1000)} keys...")
    start = time.time()
    
    test_gets = min(limited_ops, 1000)
    for i in range(test_gets):
        run_cql(f"SELECT value FROM kvks.keyvalue WHERE key = 'key_{i}'")
    
    get_time = time.time() - start
    results['get_time'] = float(get_time)
    results['get_throughput'] = float(test_gets / get_time)
    print(f"     ✅ Done in {get_time:.2f}s ({results['get_throughput']:.0f} ops/sec)")
    
    # 3️⃣ TTL
    print(f"  ⏰ SET with TTL...")
    start = time.time()
    
    for i in range(100):
        run_cql(f"INSERT INTO kvks.keyvalue (key, value) VALUES ('ttl_{i}', 'value') USING TTL {TTL_SECONDS}")
    
    ttl_time = time.time() - start
    results['ttl_set_time'] = float(ttl_time)
    print(f"     ✅ Done in {ttl_time:.2f}s")
    
    return results

# ============================================
# NEO4J - Key-Value (NOT SUITABLE)
# ============================================
def test_neo4j_keyvalue():
    print("\n🟢 Testing Neo4j Key-Value...")
    print("  ⚠️  Neo4j n'est PAS conçu pour du simple key-value")
    
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    results = {}
    
    with driver.session() as session:
        session.run("MATCH (n:KeyValue) DELETE n")
        
        # 1️⃣ SET (CREATE)
        limited_ops = min(NUM_OPS, 5000)  # Très limité pour Neo4j
        print(f"  📝 SET {limited_ops} keys (very limited)...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        
        for i in range(limited_ops):
            key = f"key_{i}"
            value = generate_random_value()
            session.run(
                "CREATE (kv:KeyValue {key: $key, value: $value})",
                key=key, value=value
            )
        
        set_time = time.time() - start
        results['set_time'] = float(set_time)
        results['set_throughput'] = float(limited_ops / set_time)
        results['set_cpu'] = float(psutil.cpu_percent() - cpu_start)
        print(f"     ✅ Done in {set_time:.2f}s ({results['set_throughput']:.0f} ops/sec)")
        
        # 2️⃣ GET (MATCH)
        print(f"  📖 GET {limited_ops} keys...")
        start = time.time()
        
        for i in range(limited_ops):
            session.run("MATCH (kv:KeyValue {key: $key}) RETURN kv.value", key=f"key_{i}")
        
        get_time = time.time() - start
        results['get_time'] = float(get_time)
        results['get_throughput'] = float(limited_ops / get_time)
        print(f"     ✅ Done in {get_time:.2f}s ({results['get_throughput']:.0f} ops/sec)")
        
        print(f"  ⚠️  Neo4j est très lent pour du key-value simple")
    
    driver.close()
    return results

# ============================================
# ENVOI DES RESULTATS
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario4_keyvalue"):
    """Envoie les résultats vers InfluxDB"""
    point = Point(scenario).tag("database", db_name)
    
    for key, value in results.items():
        if isinstance(value, (int, float)):
            # S'assurer que toutes les valeurs sont des floats
            point.field(key, float(value))
    
    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f"  📊 Résultats envoyés vers InfluxDB pour {db_name}")
    except Exception as e:
        print(f"  ⚠️  Erreur lors de l'envoi vers InfluxDB: {e}")

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("="*60)
    print("🔥 SCENARIO 4 - KEY-VALUE ULTRA RAPIDE")
    print(f"📊 Nombre d'opérations: {NUM_OPS}")
    print(f"⏰ TTL: {TTL_SECONDS} secondes")
    print("="*60)
    
    try:
        # Redis (OPTIMAL)
        try:
            print("\n" + "="*60)
            redis_results = test_redis_keyvalue()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # MongoDB (SUBOPTIMAL)
        try:
            print("\n" + "="*60)
            mongo_results = test_mongodb_keyvalue()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Cassandra (OK)
        try:
            print("\n" + "="*60)
            cassandra_results = test_cassandra_keyvalue()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        time.sleep(2)
        
        # Neo4j (NOT SUITABLE)
        try:
            print("\n" + "="*60)
            neo4j_results = test_neo4j_keyvalue()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        print("\n" + "="*60)
        print("✅ SCENARIO 4 TERMINÉ !")
        print("📊 Résultats attendus:")
        print("   🔴 Redis: IMBATTABLE (ultra rapide)")
        print("   🔵 MongoDB: LENT pour GET/SET simple")
        print("   🟣 Cassandra: OK mais pas optimal")
        print("   🟢 Neo4j: TRÈS LENT, inutile")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
        
    finally:
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")