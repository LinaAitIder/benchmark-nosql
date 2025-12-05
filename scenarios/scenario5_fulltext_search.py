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
NUM_ARTICLES = 100000  # Nombre d'articles à créer

# ---------------- DONNEES DE TEST ----------------
SAMPLE_TITLES = [
    "Introduction to Machine Learning",
    "Deep Learning with Neural Networks",
    "Natural Language Processing Basics",
    "Computer Vision Applications",
    "Data Science Best Practices",
    "Python Programming Tutorial",
    "Web Development with Flask",
    "Database Design Principles",
    "Cloud Computing Overview",
    "Cybersecurity Fundamentals"
]

SAMPLE_CONTENT = [
    "This article covers the fundamental concepts of machine learning including supervised and unsupervised learning.",
    "Deep learning has revolutionized artificial intelligence with neural networks achieving state-of-the-art results.",
    "Natural language processing enables computers to understand and generate human language effectively.",
    "Computer vision techniques allow machines to interpret and analyze visual information from images.",
    "Data science combines statistics, programming, and domain knowledge to extract insights from data.",
    "Python is a versatile programming language widely used in data science and web development.",
    "Flask is a lightweight web framework that makes building web applications simple and efficient.",
    "Good database design is crucial for building scalable and maintainable applications.",
    "Cloud computing provides on-demand access to computing resources and services over the internet.",
    "Cybersecurity protects systems and networks from digital attacks and unauthorized access."
]

SAMPLE_TAGS = ["machine-learning", "deep-learning", "nlp", "computer-vision", "data-science", 
               "python", "web-dev", "database", "cloud", "security"]

def generate_article(article_id):
    """Génère un article de test"""
    return {
        "article_id": article_id,
        "title": random.choice(SAMPLE_TITLES) + f" - Part {article_id}",
        "content": random.choice(SAMPLE_CONTENT) + f" Article number {article_id}.",
        "tags": random.sample(SAMPLE_TAGS, random.randint(1, 3)),
        "author": f"Author_{article_id % 100}"
    }

# ============================================
# MONGODB - Full-Text Search (OPTIMAL)
# ============================================
def test_mongodb_fulltext():
    print("\n🔵 Testing MongoDB Full-Text Search...")
    print("  ⚡ MongoDB est excellent pour la recherche full-text")
    
    mongo = MongoClient("mongodb://localhost:27017")
    db = mongo.testdb
    collection = db.articles
    
    collection.drop()
    
    results = {}
    
    # 1️⃣ INSERT ARTICLES
    print(f"  📝 Inserting {NUM_ARTICLES} articles...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    mem_start = psutil.virtual_memory().percent
    
    # Insertion par batch
    batch_size = 1000
    for batch_start in range(0, NUM_ARTICLES, batch_size):
        batch = [generate_article(i) for i in range(batch_start, min(batch_start + batch_size, NUM_ARTICLES))]
        collection.insert_many(batch)
    
    insert_time = time.time() - start
    results['insert_time'] = float(insert_time)
    results['insert_throughput'] = float(NUM_ARTICLES / insert_time)
    results['insert_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['insert_mem'] = float(psutil.virtual_memory().percent - mem_start)
    print(f"     ✅ Done in {insert_time:.2f}s ({results['insert_throughput']:.0f} articles/sec)")
    
    # 2️⃣ CREATE TEXT INDEX
    print(f"  🔍 Creating text index...")
    start = time.time()
    
    collection.create_index([
        ("title", "text"),
        ("content", "text"),
        ("tags", "text")
    ])
    
    index_time = time.time() - start
    results['index_time'] = float(index_time)
    print(f"     ✅ Done in {index_time:.2f}s")
    
    # 3️⃣ SINGLE KEYWORD SEARCH
    print(f"  🔍 Search: Single keyword 'machine'...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    cursor = collection.find({"$text": {"$search": "machine"}})
    results_single = list(cursor)
    
    search_single_time = time.time() - start
    results['search_single_time'] = float(search_single_time)
    results['search_single_cpu'] = float(psutil.cpu_percent() - cpu_start)
    results['search_single_count'] = float(len(results_single))
    print(f"     ✅ Done in {search_single_time:.2f}s (found {len(results_single)} articles)")
    
    # 4️⃣ MULTIPLE KEYWORDS SEARCH
    print(f"  🔍 Search: Multiple keywords 'machine learning python'...")
    start = time.time()
    
    cursor = collection.find({"$text": {"$search": "machine learning python"}})
    results_multi = list(cursor)
    
    search_multi_time = time.time() - start
    results['search_multi_time'] = float(search_multi_time)
    results['search_multi_count'] = float(len(results_multi))
    print(f"     ✅ Done in {search_multi_time:.2f}s (found {len(results_multi)} articles)")
    
    # 5️⃣ SEARCH WITH RELEVANCE SCORE
    print(f"  🔍 Search: With relevance scoring...")
    start = time.time()
    
    cursor = collection.find(
        {"$text": {"$search": "deep learning neural"}},
        {"score": {"$meta": "textScore"}}
    ).sort([("score", {"$meta": "textScore"})]).limit(10)
    
    results_scored = list(cursor)
    
    search_scored_time = time.time() - start
    results['search_scored_time'] = float(search_scored_time)
    results['search_scored_count'] = float(len(results_scored))
    print(f"     ✅ Done in {search_scored_time:.2f}s (top {len(results_scored)} articles)")
    
    # 6️⃣ TAG SEARCH
    print(f"  🏷️  Search: By tags...")
    start = time.time()
    
    cursor = collection.find({"tags": "machine-learning"})
    results_tag = list(cursor)
    
    tag_search_time = time.time() - start
    results['tag_search_time'] = float(tag_search_time)
    results['tag_search_count'] = float(len(results_tag))
    print(f"     ✅ Done in {tag_search_time:.2f}s (found {len(results_tag)} articles)")
    
    mongo.close()
    return results

# ============================================
# REDIS - Full-Text Search (avec RedisSearch si disponible)
# ============================================
def test_redis_fulltext():
    print("\n🔴 Testing Redis Full-Text Search...")
    print("  ⚠️  Redis standard ne supporte pas le full-text")
    print("  ℹ️  Nécessite RedisSearch module (non installé par défaut)")
    
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()
    
    results = {}
    
    # 1️⃣ INSERT ARTICLES (comme hash)
    print(f"  📝 Inserting {min(NUM_ARTICLES, 10000)} articles...")
    start = time.time()
    cpu_start = psutil.cpu_percent()
    
    limited_articles = min(NUM_ARTICLES, 10000)
    pipe = r.pipeline()
    for i in range(limited_articles):
        article = generate_article(i)
        pipe.hset(f"article:{i}", mapping={
            "article_id": article["article_id"],
            "title": article["title"],
            "content": article["content"],
            "tags": ",".join(article["tags"])
        })
    pipe.execute()
    
    insert_time = time.time() - start
    results['insert_time'] = float(insert_time)
    results['insert_throughput'] = float(limited_articles / insert_time)
    results['insert_cpu'] = float(psutil.cpu_percent() - cpu_start)
    print(f"     ✅ Done in {insert_time:.2f}s")
    
    # 2️⃣ SIMPLE KEY SEARCH (pas de vraie recherche full-text)
    print(f"  🔍 Key-based search (not true full-text)...")
    start = time.time()
    
    keys = list(r.scan_iter(match="article:*", count=100))
    
    search_time = time.time() - start
    results['search_time'] = float(search_time)
    results['keys_found'] = float(len(keys))
    print(f"     ✅ Done in {search_time:.2f}s (found {len(keys)} keys)")
    
    print(f"  ⚠️  Redis nécessite RedisSearch pour vraie recherche full-text")
    results['fulltext_supported'] = 0.0
    
    return results

# ============================================
# CASSANDRA - Full-Text Search (NOT SUPPORTED)
# ============================================
def test_cassandra_fulltext():
    print("\n🟣 Testing Cassandra Full-Text Search...")
    print("  ⚠️  Cassandra ne supporte PAS le full-text nativement")
    print("  ℹ️  Nécessite intégration avec Solr ou Elasticsearch")
    
    results = {}
    
    def run_cql(query):
        cmd = f'docker exec cassandra cqlsh -e "{query}"'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    # Setup basique
    run_cql("CREATE KEYSPACE IF NOT EXISTS textks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    run_cql("DROP TABLE IF EXISTS textks.articles")
    run_cql("""CREATE TABLE textks.articles (
        article_id int PRIMARY KEY,
        title text,
        content text,
        tags list<text>
    )""")
    
    # 1️⃣ INSERT (limité)
    limited_articles = min(NUM_ARTICLES, 1000)
    print(f"  📝 Inserting {limited_articles} articles (limited)...")
    start = time.time()
    
    batch_size = 50
    for batch_start in range(0, limited_articles, batch_size):
        batch_queries = []
        for i in range(batch_start, min(batch_start + batch_size, limited_articles)):
            article = generate_article(i)
            tags_str = "'" + "','".join(article['tags']) + "'"
            batch_queries.append(
                f"INSERT INTO textks.articles (article_id, title, content, tags) "
                f"VALUES ({article['article_id']}, '{article['title']}', '{article['content']}', [{tags_str}])"
            )
        
        batch_query = "BEGIN BATCH " + "; ".join(batch_queries) + "; APPLY BATCH"
        run_cql(batch_query)
    
    insert_time = time.time() - start
    results['insert_time'] = float(insert_time)
    results['insert_throughput'] = float(limited_articles / insert_time)
    print(f"     ✅ Done in {insert_time:.2f}s")
    
    print(f"  ⚠️  Cassandra ne peut pas faire de recherche full-text")
    print(f"  ℹ️  Seules les requêtes par clé primaire sont supportées")
    
    results['fulltext_supported'] = 0.0
    results['search_time'] = 0.0
    
    return results

# ============================================
# NEO4J - Full-Text Search (NOT SUITABLE)
# ============================================
def test_neo4j_fulltext():
    print("\n🟢 Testing Neo4j Full-Text Search...")
    print("  ⚠️  Neo4j n'est pas conçu pour la recherche full-text")
    
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    results = {}
    
    with driver.session() as session:
        session.run("MATCH (n:Article) DELETE n")
        
        # 1️⃣ INSERT (très limité)
        limited_articles = min(NUM_ARTICLES, 1000)
        print(f"  📝 Inserting {limited_articles} articles (very limited)...")
        start = time.time()
        cpu_start = psutil.cpu_percent()
        
        for i in range(limited_articles):
            article = generate_article(i)
            session.run(
                """CREATE (a:Article {
                    article_id: $article_id,
                    title: $title,
                    content: $content,
                    tags: $tags
                })""",
                article_id=article["article_id"],
                title=article["title"],
                content=article["content"],
                tags=article["tags"]
            )
        
        insert_time = time.time() - start
        results['insert_time'] = float(insert_time)
        results['insert_throughput'] = float(limited_articles / insert_time)
        results['insert_cpu'] = float(psutil.cpu_percent() - cpu_start)
        print(f"     ✅ Done in {insert_time:.2f}s")
        
        # 2️⃣ SIMPLE SEARCH (CONTAINS - pas de vraie recherche full-text)
        print(f"  🔍 Search: CONTAINS 'machine' (not true full-text)...")
        start = time.time()
        
        result = session.run(
            "MATCH (a:Article) WHERE a.title CONTAINS 'machine' RETURN count(a) as cnt"
        )
        count = result.single()['cnt']
        
        search_time = time.time() - start
        results['search_time'] = float(search_time)
        results['search_count'] = float(count)
        print(f"     ✅ Done in {search_time:.2f}s (found {count} articles)")
        
        print(f"  ⚠️  Neo4j n'a pas d'index full-text natif")
        results['fulltext_supported'] = 0.0
    
    driver.close()
    return results

# ============================================
# ENVOI DES RESULTATS
# ============================================
def send_results_to_influx(db_name, results, scenario="scenario5_fulltext"):
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
    print("🔥 SCENARIO 5 - RECHERCHE TEXTUELLE (FULL-TEXT)")
    print(f"📊 Nombre d'articles: {NUM_ARTICLES}")
    print("="*60)
    
    try:
        # MongoDB (OPTIMAL)
        try:
            print("\n" + "="*60)
            mongo_results = test_mongodb_fulltext()
            send_results_to_influx("MongoDB", mongo_results)
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
        
        time.sleep(2)
        
        # Redis (LIMITED)
        try:
            print("\n" + "="*60)
            redis_results = test_redis_fulltext()
            send_results_to_influx("Redis", redis_results)
        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
        
        time.sleep(2)
        
        # Cassandra (NOT SUPPORTED)
        try:
            print("\n" + "="*60)
            cassandra_results = test_cassandra_fulltext()
            send_results_to_influx("Cassandra", cassandra_results)
        except Exception as e:
            print(f"❌ Erreur Cassandra: {e}")
        
        time.sleep(2)
        
        # Neo4j (NOT SUITABLE)
        try:
            print("\n" + "="*60)
            neo4j_results = test_neo4j_fulltext()
            send_results_to_influx("Neo4j", neo4j_results)
        except Exception as e:
            print(f"❌ Erreur Neo4j: {e}")
        
        print("\n" + "="*60)
        print("✅ SCENARIO 5 TERMINÉ !")
        print("📊 Résultats attendus:")
        print("   🔵 MongoDB: MEILLEUR (index full-text natif)")
        print("   🔴 Redis: LIMITÉ (nécessite RedisSearch)")
        print("   🟣 Cassandra: NON SUPPORTÉ")
        print("   🟢 Neo4j: PAS ADAPTÉ")
        print("📊 Vérifie Grafana sur http://localhost:3000")
        print("="*60)
        
    finally:
        write_api.close()
        client_influx.close()
        print("🔒 Connexion InfluxDB fermée proprement")