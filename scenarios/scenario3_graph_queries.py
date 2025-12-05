import time
import psutil
from influxdb_client import InfluxDBClient, Point
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
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
NUM_USERS = 1000  # Nombre d'utilisateurs
NUM_FRIENDSHIPS = 2000  # Nombre de relations d'amitié
NUM_LIKES = 3000  # Nombre de "likes"

# ============================================
# NEO4J - Graph Queries (OPTIMAL)
# ============================================
def test_neo4j_graph():
    print("\n🟢 Testing Neo4j Graph Queries...")
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    results = {}
    
    with driver.session() as session:
        # Nettoyage
        print("  🗑️  Cleaning database...")
        session.run("MATCH (n) DETACH DELETE n")
        
        # 1️⃣ CREATE USERS
        print(f"  👥 Creating {NUM_USERS} users...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        
        for i in range(NUM_USERS):
            session.run(
                "CREATE (u:User {user_id: $user_id, name: $name})",
                user_id=i,
                name=f"User_{i}"
            )
        
        create_time = time.time() - start
        results['create_users_time'] = float(create_time)
        results['create_users_cpu'] = float(psutil.cpu_percent() - cpu_start)
        print(f"     ✅ Done in {create_time:.2f}s")
        
        # 2️⃣ CREATE FRIENDSHIPS
        print(f"  🤝 Creating {NUM_FRIENDSHIPS} friendships...")
        start = time.time()
        
        for _ in range(NUM_FRIENDSHIPS):
            user1 = random.randint(0, NUM_USERS - 1)
            user2 = random.randint(0, NUM_USERS - 1)
            if user1 != user2:
                session.run(
                    "MATCH (u1:User {user_id: $user1}), (u2:User {user_id: $user2}) "
                    "MERGE (u1)-[:FRIEND_OF]->(u2)",
                    user1=user1, user2=user2
                )
        
        friendship_time = time.time() - start
        results['create_friendships_time'] = float(friendship_time)
        print(f"     ✅ Done in {friendship_time:.2f}s")
        
        # 3️⃣ CREATE LIKES
        print(f"  ❤️  Creating {NUM_LIKES} likes...")
        start = time.time()
        
        for _ in range(NUM_LIKES):
            user1 = random.randint(0, NUM_USERS - 1)
            user2 = random.randint(0, NUM_USERS - 1)
            if user1 != user2:
                session.run(
                    "MATCH (u1:User {user_id: $user1}), (u2:User {user_id: $user2}) "
                    "MERGE (u1)-[:LIKES]->(u2)",
                    user1=user1, user2=user2
                )
        
        likes_time = time.time() - start
        results['create_likes_time'] = float(likes_time)
        print(f"     ✅ Done in {likes_time:.2f}s")
        
        # 4️⃣ QUERY: Friends of Friends
        print(f"  🔍 Query: Friends of friends...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        
        result = session.run(
            "MATCH (u:User {user_id: 0})-[:FRIEND_OF*2]-(fof:User) "
            "RETURN DISTINCT fof.user_id LIMIT 100"
        )
        fof_list = [record['fof.user_id'] for record in result]
        
        fof_time = time.time() - start
        results['friends_of_friends_time'] = float(fof_time)
        results['friends_of_friends_cpu'] = float(psutil.cpu_percent() - cpu_start)
        results['fof_count'] = float(len(fof_list))
        print(f"     ✅ Done in {fof_time:.2f}s (found {len(fof_list)} friends)")
        
        # 5️⃣ QUERY: 3-Level Connections
        print(f"  🔍 Query: 3-level connections...")
        start = time.time()
        
        result = session.run(
            "MATCH (u:User {user_id: 0})-[:FRIEND_OF*1..3]-(connected:User) "
            "RETURN DISTINCT connected.user_id LIMIT 100"
        )
        connections = [record['connected.user_id'] for record in result]
        
        three_level_time = time.time() - start
        results['three_level_time'] = float(three_level_time)
        results['three_level_count'] = float(len(connections))
        print(f"     ✅ Done in {three_level_time:.2f}s (found {len(connections)} connections)")
        
        # 6️⃣ QUERY: Community Detection (simple)
        print(f"  🔍 Query: Find communities (shared friends)...")
        start = time.time()
        
        result = session.run(
            "MATCH (u1:User)-[:FRIEND_OF]->(common:User)<-[:FRIEND_OF]-(u2:User) "
            "WHERE u1.user_id < u2.user_id "
            "RETURN u1.user_id, u2.user_id, count(common) as shared_friends "
            "ORDER BY shared_friends DESC LIMIT 10"
        )
        communities = list(result)
        
        community_time = time.time() - start
        results['community_detection_time'] = float(community_time)
        results['top_communities'] = float(len(communities))
        print(f"     ✅ Done in {community_time:.2f}s (found {len(communities)} pairs)")
    
    driver.close()
    return results

# ============================================
# MONGODB - Graph Queries (SUBOPTIMAL)
# ============================================
def test_mongodb_graph():
    print("\n🔵 Testing MongoDB Graph Queries...")
    print("  ⚠️  MongoDB n'est pas optimal pour les requêtes de graphes")
    
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    users = db.graph_users
    friendships = db.graph_friendships
    
    users.drop()
    friendships.drop()
    
    results = {}
    
    # 1️⃣ CREATE USERS
    print(f"  👥 Creating {NUM_USERS} users...")
    start = time.time()
    
    user_docs = [{"user_id": i, "name": f"User_{i}"} for i in range(NUM_USERS)]
    users.insert_many(user_docs)
    
    create_time = time.time() - start
    results['create_users_time'] = float(create_time)
    print(f"     ✅ Done in {create_time:.2f}s")
    
    # 2️⃣ CREATE FRIENDSHIPS
    print(f"  🤝 Creating {NUM_FRIENDSHIPS} friendships...")
    start = time.time()
    
    friendship_docs = []
    for _ in range(NUM_FRIENDSHIPS):
        user1 = random.randint(0, NUM_USERS - 1)
        user2 = random.randint(0, NUM_USERS - 1)
        if user1 != user2:
            friendship_docs.append({"user1": user1, "user2": user2, "type": "friend"})
    
    if friendship_docs:
        friendships.insert_many(friendship_docs)
    
    friendship_time = time.time() - start
    results['create_friendships_time'] = float(friendship_time)
    print(f"     ✅ Done in {friendship_time:.2f}s")
    
    # 3️⃣ CREATE INDEX
    print(f"  🔍 Creating indexes...")
    friendships.create_index([("user1", 1)])
    friendships.create_index([("user2", 1)])
    
    # 4️⃣ QUERY: Friends of Friends (SLOW)
    print(f"  🔍 Query: Friends of friends...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    # Trouver les amis directs
    direct_friends = friendships.find({"user1": 0})
    friend_ids = [f["user2"] for f in direct_friends]
    
    # Trouver les amis des amis
    fof_set = set()
    for friend_id in friend_ids[:10]:  # Limité pour performance
        fof = friendships.find({"user1": friend_id})
        fof_set.update([f["user2"] for f in fof])
    
    fof_time = time.time() - start
    results['friends_of_friends_time'] = float(fof_time)
    results['friends_of_friends_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['fof_count'] = float(len(fof_set))
    print(f"     ✅ Done in {fof_time:.2f}s (found {len(fof_set)} friends)")
    
    # 5️⃣ QUERY: 3-Level Connections (VERY SLOW - skip)
    print(f"  ⚠️  Skipping 3-level query (too slow for MongoDB)")
    results['three_level_time'] = float(0.0)  # Changed from -1 to 0.0
    results['three_level_supported'] = 0.0  # Added flag
    
    mongo.close()
    return results

# ============================================
# CASSANDRA - Graph Queries (NOT SUITABLE)
# ============================================
def test_cassandra_graph():
    print("\n🟣 Testing Cassandra Graph Queries...")
    print("  ⚠️  Cassandra n'est PAS conçu pour les requêtes de graphes")
    print("  ℹ️  Test basique uniquement")
    
    results = {}
    
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    # Setup
    run_cql("CREATE KEYSPACE IF NOT EXISTS graphks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("DROP TABLE IF EXISTS graphks.users")
    run_cql("DROP TABLE IF EXISTS graphks.friendships")
    
    run_cql("CREATE TABLE graphks.users (user_id int PRIMARY KEY, name text)")
    run_cql("CREATE TABLE graphks.friendships (user1 int, user2 int, PRIMARY KEY (user1, user2))")
    
    # 1️⃣ INSERT USERS
    print(f"  👥 Creating {min(NUM_USERS, 100)} users (limited)...")
    start = time.time()
    
    limited_users = min(NUM_USERS, 100)
    for i in range(limited_users):
        run_cql(f"INSERT INTO graphks.users (user_id, name) VALUES ({i}, 'User_{i}')")
    
    create_time = time.time() - start
    results['create_users_time'] = float(create_time)
    print(f"     ✅ Done in {create_time:.2f}s")
    
    # 2️⃣ Note
    print(f"  ⚠️  Cassandra ne peut pas effectuer de traversée de graphe")
    print(f"  ℹ️  Requêtes de graphes impossibles avec Cassandra")
    
    results['graph_suitable'] = 0.0  # Changed from False to 0.0
    results['friends_of_friends_time'] = float(0.0)  # Changed from -1 to 0.0
    results['three_level_time'] = float(0.0)  # Changed from -1 to 0.0
    
    return results

# ============================================
# REDIS - Graph Queries (avec RedisGraph si disponible)
# ============================================
def test_redis_graph():
    print("\n🔴 Testing Redis Graph Queries...")
    print("  ⚠️  Redis standard ne supporte pas les graphes natifs")
    print("  ℹ️  Simulation avec structures Redis")
    
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()
    
    results = {}
    
    # 1️⃣ CREATE USERS
    print(f"  👥 Creating {NUM_USERS} users...")
    start = time.time()
    
    pipe = r.pipeline()
    for i in range(NUM_USERS):
        pipe.hset(f"user:{i}", mapping={"user_id": i, "name": f"User_{i}"})
    pipe.execute()
    
    create_time = time.time() - start
    results['create_users_time'] = float(create_time)
    print(f"     ✅ Done in {create_time:.2f}s")
    
    # 2️⃣ CREATE FRIENDSHIPS (via Sets)
    print(f"  🤝 Creating {NUM_FRIENDSHIPS} friendships...")
    start = time.time()
    
    pipe = r.pipeline()
    for _ in range(NUM_FRIENDSHIPS):
        user1 = random.randint(0, NUM_USERS - 1)
        user2 = random.randint(0, NUM_USERS - 1)
        if user1 != user2:
            pipe.sadd(f"friends:{user1}", user2)
            pipe.sadd(f"friends:{user2}", user1)
    pipe.execute()
    
    friendship_time = time.time() - start
    results['create_friendships_time'] = float(friendship_time)
    print(f"     ✅ Done in {friendship_time:.2f}s")
    
    # 3️⃣ QUERY: Friends of Friends
    print(f"  🔍 Query: Friends of friends...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    # Amis directs
    direct_friends = r.smembers("friends:0")
    
    # Amis des amis
    fof_set = set()
    for friend in list(direct_friends)[:20]:  # Limité pour performance
        fof = r.smembers(f"friends:{friend}")
        fof_set.update(fof)
    
    fof_time = time.time() - start
    results['friends_of_friends_time'] = float(fof_time)
    results['friends_of_friends_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['fof_count'] = float(len(fof_set))
    print(f"     ✅ Done in {fof_time:.2f}s (found {len(fof_set)} friends)")
    
    # 4️⃣ Note
    print(f"  ⚠️  Redis ne peut pas faire de traversée profonde efficacement")
    results['three_level_time'] = float(0.0)  # Changed from -1 to 0.0
    results['three_level_supported'] = 0.0  # Added flag
    
    return results

# ============================================
# ENVOI DES RESULTATS
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario3_graph_v2"):
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
    print("🔥 SCENARIO 3 - REQUÊTES RELATIONNELLES (GRAPHES)")
    print(f"📊 Nombre d'utilisateurs: {NUM_USERS}")
    print(f"📊 Nombre de relations: {NUM_FRIENDSHIPS}")
    print("="*60)
    
    try:
        # Neo4j (OPTIMAL)
        try:
            print("\n" + "="*60)
            neo4j_results = test_neo4j_graph()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        time.sleep(2)
        
        # MongoDB (SUBOPTIMAL)
        try:
            print("\n" + "="*60)
            mongo_results = test_mongodb_graph()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Redis (LIMITED)
        try:
            print("\n" + "="*60)
            redis_results = test_redis_graph()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # Cassandra (NOT SUITABLE)
        try:
            print("\n" + "="*60)
            cassandra_results = test_cassandra_graph()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        print("\n" + "="*60)
        print("✅ SCENARIO 3 TERMINÉ !")
        print("📊 Résultats attendus:")
        print("   🟢 Neo4j: EXCELLENT pour graphes")
        print("   🔵 MongoDB: LENT, pas adapté")
        print("   🔴 Redis: LIMITÉ, traversée difficile")
        print("   🟣 Cassandra: NON ADAPTÉ")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
        
    finally:
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")