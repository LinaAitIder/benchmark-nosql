import time
import psutil
from influxdb_client import InfluxDBClient, Point, WritePrecision
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

import warnings
warnings.filterwarnings('ignore')

load_dotenv()

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

# ---------------- NOMBRE D'OPERATIONS ----------------
NUM_OPS = 100 # 100k opérations pour chaque test

# ---------------- DONNEES DE TEST ----------------
def get_test_data(user_id):
    """Génère un document de test"""
    return {
        "user_id": user_id,
        "name": f"Test User {user_id}",
        "age": 25 + (user_id % 50),
        "city": "Paris"
    }

# ============================================
# MONGODB CRUD TESTS
# ============================================
def test_mongodb_crud():
    print("\n🔵 Testing MongoDB CRUD...")
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    collection = db.users_crud
    
    # Nettoyer la collection
    collection.drop()
    
    results = {}
    
    # 1️⃣ INSERT
    print(f"  📝 INSERT {NUM_OPS} documents...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    docs = [get_test_data(i) for i in range(NUM_OPS)]
    collection.insert_many(docs)
    
    insert_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['insert_time'] = insert_time
    results['insert_latency'] = insert_time / NUM_OPS
    results['insert_cpu'] = cpu_used
    results['insert_mem'] = mem_used
    print(f"     ✅ Done in {insert_time:.2f}s (avg: {results['insert_latency']*1000:.4f}ms)")
    
    # 2️⃣ READ
    print(f"  📖 READ {NUM_OPS} documents...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        collection.find_one({"user_id": i})
    
    read_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['read_time'] = read_time
    results['read_latency'] = read_time / NUM_OPS
    results['read_cpu'] = cpu_used
    results['read_mem'] = mem_used
    print(f"     ✅ Done in {read_time:.2f}s (avg: {results['read_latency']*1000:.4f}ms)")
    
    # 3️⃣ UPDATE
    print(f"  🔄 UPDATE {NUM_OPS} documents...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        collection.update_one({"user_id": i}, {"$set": {"age": 30}})
    
    update_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['update_time'] = update_time
    results['update_latency'] = update_time / NUM_OPS
    results['update_cpu'] = cpu_used
    results['update_mem'] = mem_used
    print(f"     ✅ Done in {update_time:.2f}s (avg: {results['update_latency']*1000:.4f}ms)")
    
    # 4️⃣ DELETE
    print(f"  🗑️  DELETE {NUM_OPS} documents...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        collection.delete_one({"user_id": i})
    
    delete_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['delete_time'] = delete_time
    results['delete_latency'] = delete_time / NUM_OPS
    results['delete_cpu'] = cpu_used
    results['delete_mem'] = mem_used
    print(f"     ✅ Done in {delete_time:.2f}s (avg: {results['delete_latency']*1000:.4f}ms)")
    
    mongo.close()
    return results

# ============================================
# REDIS CRUD TESTS
# ============================================
def test_redis_crud():
    print("\n🔴 Testing Redis CRUD...")
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    
    # Nettoyer
    r.flushdb()
    
    results = {}
    
    # 1️⃣ INSERT (SET)
    print(f"  📝 INSERT {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        data = get_test_data(i)
        r.hset(f"user:{i}", mapping=data)
    
    insert_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['insert_time'] = insert_time
    results['insert_latency'] = insert_time / NUM_OPS
    results['insert_cpu'] = cpu_used
    results['insert_mem'] = mem_used
    print(f"     ✅ Done in {insert_time:.2f}s (avg: {results['insert_latency']*1000:.4f}ms)")
    
    # 2️⃣ READ (GET)
    print(f"  📖 READ {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        r.hgetall(f"user:{i}")
    
    read_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['read_time'] = read_time
    results['read_latency'] = read_time / NUM_OPS
    results['read_cpu'] = cpu_used
    results['read_mem'] = mem_used
    print(f"     ✅ Done in {read_time:.2f}s (avg: {results['read_latency']*1000:.4f}ms)")
    
    # 3️⃣ UPDATE
    print(f"  🔄 UPDATE {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        r.hset(f"user:{i}", "age", 30)
    
    update_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['update_time'] = update_time
    results['update_latency'] = update_time / NUM_OPS
    results['update_cpu'] = cpu_used
    results['update_mem'] = mem_used
    print(f"     ✅ Done in {update_time:.2f}s (avg: {results['update_latency']*1000:.4f}ms)")
    
    # 4️⃣ DELETE
    print(f"  🗑️  DELETE {NUM_OPS} keys...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        r.delete(f"user:{i}")
    
    delete_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['delete_time'] = delete_time
    results['delete_latency'] = delete_time / NUM_OPS
    results['delete_cpu'] = cpu_used
    results['delete_mem'] = mem_used
    print(f"     ✅ Done in {delete_time:.2f}s (avg: {results['delete_latency']*1000:.4f}ms)")
    
    return results

# ============================================
# CASSANDRA CRUD TESTS (Version simplifiée sans driver Python)
# ============================================
def test_cassandra_crud():
    print("\n🟣 Testing Cassandra CRUD...")
    print("  ⚠️  Cassandra nécessite une configuration spéciale pour Python 3.12+")
    print("  ℹ️  Cassandra sera testé via docker exec pour ce scénario")
    
    import subprocess
    results = {}
    
    # Fonction helper pour exécuter CQL via docker
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=10)
            return result.returncode == 0
        except:
            return False
    
    # Setup keyspace et table
    print("  ⚙️  Configuration du keyspace et table...")
    run_cql("CREATE KEYSPACE IF NOT EXISTS testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("CREATE TABLE IF NOT EXISTS testks.users (user_id int PRIMARY KEY, name text, age int, city text)")
    run_cql("TRUNCATE testks.users")
    
    # 1️⃣ INSERT
    print(f"  📝 INSERT {NUM_OPS} rows...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        data = get_test_data(i)
        query = f"INSERT INTO testks.users (user_id, name, age, city) VALUES ({data['user_id']}, '{data['name']}', {data['age']}, '{data['city']}')"
        run_cql(query)
    
    insert_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['insert_time'] = insert_time
    results['insert_latency'] = insert_time / NUM_OPS
    results['insert_cpu'] = cpu_used
    results['insert_mem'] = mem_used
    print(f"     ✅ Done in {insert_time:.2f}s (avg: {results['insert_latency']*1000:.4f}ms)")
    
    # 2️⃣ READ
    print(f"  📖 READ {NUM_OPS} rows...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        query = f"SELECT * FROM testks.users WHERE user_id = {i}"
        run_cql(query)
    
    read_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['read_time'] = read_time
    results['read_latency'] = read_time / NUM_OPS
    results['read_cpu'] = cpu_used
    results['read_mem'] = mem_used
    print(f"     ✅ Done in {read_time:.2f}s (avg: {results['read_latency']*1000:.4f}ms)")
    
    # 3️⃣ UPDATE
    print(f"  🔄 UPDATE {NUM_OPS} rows...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        query = f"UPDATE testks.users SET age = 30 WHERE user_id = {i}"
        run_cql(query)
    
    update_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['update_time'] = update_time
    results['update_latency'] = update_time / NUM_OPS
    results['update_cpu'] = cpu_used
    results['update_mem'] = mem_used
    print(f"     ✅ Done in {update_time:.2f}s (avg: {results['update_latency']*1000:.4f}ms)")
    
    # 4️⃣ DELETE
    print(f"  🗑️  DELETE {NUM_OPS} rows...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    for i in range(NUM_OPS):
        query = f"DELETE FROM testks.users WHERE user_id = {i}"
        run_cql(query)
    
    delete_time = time.time() - start
    cpu_used = psutil.cpu_percent() - cpu_start
    mem_used = psutil.virtual_memory().percent - mem_start
    
    results['delete_time'] = delete_time
    results['delete_latency'] = delete_time / NUM_OPS
    results['delete_cpu'] = cpu_used
    results['delete_mem'] = mem_used
    print(f"     ✅ Done in {delete_time:.2f}s (avg: {results['delete_latency']*1000:.4f}ms)")
    
    return results

# ============================================
# NEO4J CRUD TESTS
# ============================================
def test_neo4j_crud():
    print("\n🟢 Testing Neo4j CRUD...")
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    results = {}
    
    with driver.session() as session:
        # Nettoyer
        session.run("MATCH (n:User) DELETE n")
        
        # 1️⃣ INSERT (CREATE)
        print(f"  📝 INSERT {NUM_OPS} nodes...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        mem_start = psutil.virtual_memory().percent
        
        for i in range(NUM_OPS):
            data = get_test_data(i)
            session.run(
                "CREATE (u:User {user_id: $user_id, name: $name, age: $age, city: $city})",
                **data
            )
        
        insert_time = time.time() - start
        cpu_used = psutil.cpu_percent() - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        results['insert_time'] = insert_time
        results['insert_latency'] = insert_time / NUM_OPS
        results['insert_cpu'] = cpu_used
        results['insert_mem'] = mem_used
        print(f"     ✅ Done in {insert_time:.2f}s (avg: {results['insert_latency']*1000:.4f}ms)")
        
        # 2️⃣ READ (MATCH)
        print(f"  📖 READ {NUM_OPS} nodes...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        mem_start = psutil.virtual_memory().percent
        
        for i in range(NUM_OPS):
            session.run("MATCH (u:User {user_id: $user_id}) RETURN u", user_id=i)
        
        read_time = time.time() - start
        cpu_used = psutil.cpu_percent() - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        results['read_time'] = read_time
        results['read_latency'] = read_time / NUM_OPS
        results['read_cpu'] = cpu_used
        results['read_mem'] = mem_used
        print(f"     ✅ Done in {read_time:.2f}s (avg: {results['read_latency']*1000:.4f}ms)")
        
        # 3️⃣ UPDATE (SET)
        print(f"  🔄 UPDATE {NUM_OPS} nodes...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        mem_start = psutil.virtual_memory().percent
        
        for i in range(NUM_OPS):
            session.run("MATCH (u:User {user_id: $user_id}) SET u.age = 30", user_id=i)
        
        update_time = time.time() - start
        cpu_used = psutil.cpu_percent() - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        results['update_time'] = update_time
        results['update_latency'] = update_time / NUM_OPS
        results['update_cpu'] = cpu_used
        results['update_mem'] = mem_used
        print(f"     ✅ Done in {update_time:.2f}s (avg: {results['update_latency']*1000:.4f}ms)")
        
        # 4️⃣ DELETE
        print(f"  🗑️  DELETE {NUM_OPS} nodes...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        mem_start = psutil.virtual_memory().percent
        
        for i in range(NUM_OPS):
            session.run("MATCH (u:User {user_id: $user_id}) DELETE u", user_id=i)
        
        delete_time = time.time() - start
        cpu_used = psutil.cpu_percent() - cpu_start
        mem_used = psutil.virtual_memory().percent - mem_start
        
        results['delete_time'] = delete_time
        results['delete_latency'] = delete_time / NUM_OPS
        results['delete_cpu'] = cpu_used
        results['delete_mem'] = mem_used
        print(f"     ✅ Done in {delete_time:.2f}s (avg: {results['delete_latency']*1000:.4f}ms)")
    
    driver.close()
    return results

# ============================================
# ENVOI DES RESULTATS VERS INFLUXDB
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario1_crud"):
    """Envoie les résultats vers InfluxDB"""
    for operation in ['insert', 'read', 'update', 'delete']:
        point = Point(scenario) \
            .tag("database", db_name) \
            .tag("operation", operation) \
            .field("total_time", results[f'{operation}_time']) \
            .field("latency_ms", results[f'{operation}_latency'] * 1000) \
            .field("cpu_percent", results[f'{operation}_cpu']) \
            .field("memory_percent", results[f'{operation}_mem'])
        
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    
    print(f"  📊 Résultats envoyés vers InfluxDB pour {db_name}")

# ============================================
# MAIN - EXECUTION DES TESTS
# ============================================
if __name__ == "__main__":
    print("="*60)
    print("🔥 SCENARIO 1 - TESTS CRUD BASIQUES")
    print(f"📊 Nombre d'opérations par test: {NUM_OPS}")
    print("="*60)
    
    try:
        # Test MongoDB
        try:
            mongo_results = test_mongodb_crud()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Test Redis
        try:
            redis_results = test_redis_crud()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # Test Cassandra
        try:
            cassandra_results = test_cassandra_crud()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        time.sleep(2)
        
        # Test Neo4j
        try:
            neo4j_results = test_neo4j_crud()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        print("\n" + "="*60)
        print("✅ TOUS LES TESTS SONT TERMINÉS !")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
    
    finally:
        # Fermer proprement la connexion InfluxDB
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")