import time
import psutil
from influxdb_client import InfluxDBClient, Point
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
import random
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv
import os

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

import warnings
warnings.filterwarnings('ignore')

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

# ---------------- CONFIGURATION ----------------
THREAD_COUNTS = [1, 5, 10, 20, 50]  # Nombre de threads à tester
OPS_PER_THREAD = 1000  # Opérations par thread

# Thread-local storage pour les connexions
thread_local = threading.local()

# ---------------- DONNEES DE TEST ----------------
def generate_test_doc(doc_id):
    """Génère un document de test"""
    return {
        "doc_id": doc_id,
        "name": f"Document_{doc_id}",
        "value": random.randint(1, 1000),
        "data": f"Data_{doc_id}_" + "x" * 50
    }

# ============================================
# MONGODB - Scalability Tests
# ============================================
def mongodb_worker(worker_id, num_ops):
    """Worker MongoDB pour un thread"""
    # Connexion thread-safe
    if not hasattr(thread_local, 'mongo'):
        thread_local.mongo = MongoClient("mongodb://localhost:27017")
    
    db = thread_local.mongo.testdb
    collection = db.scalability_test
    
    ops_done = 0
    
    for i in range(num_ops):
        doc_id = worker_id * num_ops + i
        
        # INSERT
        doc = generate_test_doc(doc_id)
        collection.insert_one(doc)
        ops_done += 1
        
        # READ
        collection.find_one({"doc_id": doc_id})
        ops_done += 1
    
    return ops_done

def test_mongodb_scalability():
    print("\n🔵 Testing MongoDB Scalability...")
    
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    collection = db.scalability_test
    
    results = {}
    
    for num_threads in THREAD_COUNTS:
        print(f"\n  📊 Testing with {num_threads} thread(s)...")
        
        # Nettoyer
        collection.drop()
        collection.create_index([("doc_id", 1)])
        
        start_time = time.time()
        cpu_start = psutil.cpu_percent(interval=0.1)
        mem_start = psutil.virtual_memory().percent
        
        # Lancer les threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(mongodb_worker, i, OPS_PER_THREAD)
                for i in range(num_threads)
            ]
            
            total_ops = sum(f.result() for f in as_completed(futures))
        
        elapsed = time.time() - start_time
        cpu_used = psutil.cpu_percent(interval=0.1) - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        throughput = total_ops / elapsed
        
        results[f'threads_{num_threads}_time'] = float(elapsed)
        results[f'threads_{num_threads}_throughput'] = float(throughput)
        results[f'threads_{num_threads}_cpu'] = float(cpu_used)
        results[f'threads_{num_threads}_mem'] = float(mem_used)
        results[f'threads_{num_threads}_total_ops'] = float(total_ops)
        
        print(f"     ✅ {num_threads} threads: {elapsed:.2f}s ({throughput:.0f} ops/sec)")
        print(f"        CPU: {cpu_used:.1f}%, MEM: {mem_used:.1f}%")
    
    mongo.close()
    return results

# ============================================
# REDIS - Scalability Tests
# ============================================
def redis_worker(worker_id, num_ops):
    """Worker Redis pour un thread"""
    # Connexion thread-safe
    if not hasattr(thread_local, 'redis'):
        thread_local.redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
    
    r = thread_local.redis
    ops_done = 0
    
    for i in range(num_ops):
        doc_id = worker_id * num_ops + i
        
        # INSERT (SET)
        doc = generate_test_doc(doc_id)
        r.hset(f"doc:{doc_id}", mapping=doc)
        ops_done += 1
        
        # READ (GET)
        r.hgetall(f"doc:{doc_id}")
        ops_done += 1
    
    return ops_done

def test_redis_scalability():
    print("\n🔴 Testing Redis Scalability...")
    
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()
    
    results = {}
    
    for num_threads in THREAD_COUNTS:
        print(f"\n  📊 Testing with {num_threads} thread(s)...")
        
        # Nettoyer
        r.flushdb()
        
        start_time = time.time()
        cpu_start = psutil.cpu_percent(interval=0.1)
        mem_start = psutil.virtual_memory().percent
        
        # Lancer les threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(redis_worker, i, OPS_PER_THREAD)
                for i in range(num_threads)
            ]
            
            total_ops = sum(f.result() for f in as_completed(futures))
        
        elapsed = time.time() - start_time
        cpu_used = psutil.cpu_percent(interval=0.1) - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        throughput = total_ops / elapsed
        
        results[f'threads_{num_threads}_time'] = float(elapsed)
        results[f'threads_{num_threads}_throughput'] = float(throughput)
        results[f'threads_{num_threads}_cpu'] = float(cpu_used)
        results[f'threads_{num_threads}_mem'] = float(mem_used)
        results[f'threads_{num_threads}_total_ops'] = float(total_ops)
        
        print(f"     ✅ {num_threads} threads: {elapsed:.2f}s ({throughput:.0f} ops/sec)")
        print(f"        CPU: {cpu_used:.1f}%, MEM: {mem_used:.1f}%")
    
    return results

# ============================================
# CASSANDRA - Scalability Tests
# ============================================
def cassandra_worker(worker_id, num_ops):
    """Worker Cassandra pour un thread"""
    ops_done = 0
    
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=10)
            return result.returncode == 0
        except:
            return False
    
    for i in range(num_ops):
        doc_id = worker_id * num_ops + i
        doc = generate_test_doc(doc_id)
        
        # INSERT
        query = f"INSERT INTO scaleks.docs (doc_id, name, value, data) VALUES ({doc['doc_id']}, '{doc['name']}', {doc['value']}, '{doc['data']}')"
        if run_cql(query):
            ops_done += 1
        
        # READ
        query = f"SELECT * FROM scaleks.docs WHERE doc_id = {doc_id}"
        if run_cql(query):
            ops_done += 1
    
    return ops_done

def test_cassandra_scalability():
    print("\n🟣 Testing Cassandra Scalability...")
    print("  ⚠️  Test limité (docker exec est lent)")
    
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    # Setup
    run_cql("CREATE KEYSPACE IF NOT EXISTS scaleks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("DROP TABLE IF EXISTS scaleks.docs")
    run_cql("""CREATE TABLE scaleks.docs (
        doc_id int PRIMARY KEY,
        name text,
        value int,
        data text
    )""")
    
    results = {}
    
    # Test avec moins de threads pour Cassandra
    limited_threads = [1, 5, 10]
    limited_ops = 100  # Réduit pour éviter timeout
    
    for num_threads in limited_threads:
        print(f"\n  📊 Testing with {num_threads} thread(s) ({limited_ops} ops/thread)...")
        
        # Nettoyer
        run_cql("TRUNCATE scaleks.docs")
        
        start_time = time.time()
        cpu_start = psutil.cpu_percent(interval=0.1)
        
        # Lancer les threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(cassandra_worker, i, limited_ops)
                for i in range(num_threads)
            ]
            
            total_ops = sum(f.result() for f in as_completed(futures))
        
        elapsed = time.time() - start_time
        cpu_used = psutil.cpu_percent(interval=0.1) - cpu_start
        
        throughput = total_ops / elapsed if elapsed > 0 else 0
        
        results[f'threads_{num_threads}_time'] = float(elapsed)
        results[f'threads_{num_threads}_throughput'] = float(throughput)
        results[f'threads_{num_threads}_cpu'] = float(cpu_used)
        results[f'threads_{num_threads}_total_ops'] = float(total_ops)
        
        print(f"     ✅ {num_threads} threads: {elapsed:.2f}s ({throughput:.0f} ops/sec)")
        print(f"        CPU: {cpu_used:.1f}%")
    
    return results

# ============================================
# NEO4J - Scalability Tests
# ============================================
def neo4j_worker(worker_id, num_ops):
    """Worker Neo4j pour un thread"""
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    ops_done = 0
    
    with driver.session() as session:
        for i in range(num_ops):
            doc_id = worker_id * num_ops + i
            doc = generate_test_doc(doc_id)
            
            # INSERT (CREATE)
            session.run(
                "CREATE (d:Doc {doc_id: $doc_id, name: $name, value: $value, data: $data})",
                **doc
            )
            ops_done += 1
            
            # READ (MATCH)
            session.run("MATCH (d:Doc {doc_id: $doc_id}) RETURN d", doc_id=doc_id)
            ops_done += 1
    
    driver.close()
    return ops_done

def test_neo4j_scalability():
    print("\n🟢 Testing Neo4j Scalability...")
    print("  ⚠️  Neo4j a des limitations en multi-threading")
    
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    with driver.session() as session:
        session.run("MATCH (n:Doc) DELETE n")
    
    driver.close()
    
    results = {}
    
    # Test avec moins de threads pour Neo4j
    limited_threads = [1, 5, 10]
    limited_ops = 200  # Réduit
    
    for num_threads in limited_threads:
        print(f"\n  📊 Testing with {num_threads} thread(s) ({limited_ops} ops/thread)...")
        
        start_time = time.time()
        cpu_start = psutil.cpu_percent(interval=0.1)
        mem_start = psutil.virtual_memory().percent
        
        # Lancer les threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(neo4j_worker, i, limited_ops)
                for i in range(num_threads)
            ]
            
            total_ops = sum(f.result() for f in as_completed(futures))
        
        elapsed = time.time() - start_time
        cpu_used = psutil.cpu_percent(interval=0.1) - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        throughput = total_ops / elapsed
        
        results[f'threads_{num_threads}_time'] = float(elapsed)
        results[f'threads_{num_threads}_throughput'] = float(throughput)
        results[f'threads_{num_threads}_cpu'] = float(cpu_used)
        results[f'threads_{num_threads}_mem'] = float(mem_used)
        results[f'threads_{num_threads}_total_ops'] = float(total_ops)
        
        print(f"     ✅ {num_threads} threads: {elapsed:.2f}s ({throughput:.0f} ops/sec)")
        print(f"        CPU: {cpu_used:.1f}%, MEM: {mem_used:.1f}%")
    
    return results

# ============================================
# ENVOI DES RESULTATS
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario6_scalability"):
    """Envoie les résultats vers InfluxDB"""
    point = Point(scenario).tag("database", db_name)
    
    for key, value in results.items():
        if isinstance(value, (int, float)):
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
    print("🔥 SCENARIO 6 - SCALABILITÉ HORIZONTALE (MULTI-THREADING)")
    print(f"📊 Threads testés: {THREAD_COUNTS}")
    print(f"📊 Opérations par thread: {OPS_PER_THREAD}")
    print("="*60)
    
    try:
        # Redis (EXCELLENT)
        try:
            print("\n" + "="*60)
            redis_results = test_redis_scalability()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # MongoDB (GOOD)
        try:
            print("\n" + "="*60)
            mongo_results = test_mongodb_scalability()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Cassandra (EXCELLENT mais limité par docker exec)
        try:
            print("\n" + "="*60)
            cassandra_results = test_cassandra_scalability()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        time.sleep(2)
        
        # Neo4j (LIMITED)
        try:
            print("\n" + "="*60)
            neo4j_results = test_neo4j_scalability()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        print("\n" + "="*60)
        print("✅ SCENARIO 6 TERMINÉ !")
        print("📊 Résultats attendus:")
        print("   🔴 Redis: EXCELLENT (très rapide, scalable)")
        print("   🔵 MongoDB: BON (bonne scalabilité)")
        print("   🟣 Cassandra: EXCELLENT en production (limité ici par docker)")
        print("   🟢 Neo4j: LIMITÉ (contraintes multi-threading)")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
        
    finally:
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")