"""
Script pour afficher un résumé des résultats depuis InfluxDB
Usage: python analyze_results.py
"""

from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as pyo

load_dotenv()

# InfluxDB Configuration
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ensa")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "bench")

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

# Database names
DATABASES = ["MongoDB", "Redis", "Cassandra", "Neo4j"]

# Scenario configurations
SCENARIOS = {
    "scenario1_crud": {
        "name": "CRUD Operations",
        "fields": ["latency_ms", "total_time", "cpu_percent", "memory_percent"],
        "operations": ["insert", "read", "update", "delete"]
    },
    "scenario2_iot": {
        "name": "IoT/Logs (Time-Series)",
        "fields": ["insert_time", "insert_throughput", "range_query_time", "insert_cpu", "insert_mem"]
    },
    "scenario3_graph": {
        "name": "Graph Queries",
        "fields": ["create_users_time", "create_friendships_time", "friends_of_friends_time", "three_level_time"]
    },
    "scenario4_keyvalue": {
        "name": "Key-Value Speed",
        "fields": ["set_latency_ms", "get_latency_ms", "throughput_ops", "cpu_usage"]
    },
    "scenario5_fulltext": {
        "name": "Full-Text Search",
        "fields": ["insert_time", "index_build_time", "search_latency", "cpu_usage"]
    },
    "scenario6_scalability": {
        "name": "Scalability Test",
        "fields": ["create_time", "read_time", "update_time", "delete_time", "throughput_ops"]
    }
}

def print_header(title):
    """Print a formatted header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def print_separator():
    """Print a separator line"""
    print("-"*80)

def query_scenario_data(scenario_name, time_range="-24h"):
    """Query data for a specific scenario from InfluxDB"""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {time_range})
      |> filter(fn: (r) => r["_measurement"] == "{scenario_name}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []
        for table in tables:
            for record in table.records:
                results.append(record.values)
        return results
    except Exception as e:
        print(f"    ⚠️  Error querying {scenario_name}: {e}")
        return []

def analyze_scenario(scenario_key, scenario_info):
    """Analyze and display results for a single scenario"""
    print_header(f"📊 {scenario_info['name']} ({scenario_key})")
    
    data = query_scenario_data(scenario_key)
    
    if not data:
        print("    ⚠️  No data found for this scenario")
        return
    
    print(f"\n    Found {len(data)} measurement(s)\n")
    
    # Group by database
    db_results = {}
    for record in data:
        db_name = record.get('database', 'Unknown')
        if db_name not in db_results:
            db_results[db_name] = []
        db_results[db_name].append(record)
    
    # Display results by database
    for db_name in DATABASES:
        if db_name in db_results:
            print(f"\n  🔹 {db_name}")
            print_separator()
            
            # If scenario has operations (like CRUD)
            if 'operations' in scenario_info:
                ops = {}
                for record in db_results[db_name]:
                    op = record.get('operation', 'unknown')
                    if op not in ops:
                        ops[op] = record
                
                for op in scenario_info['operations']:
                    if op in ops:
                        rec = ops[op]
                        print(f"\n    {op.upper()}:")
                        for field in scenario_info['fields']:
                            value = rec.get(field)
                            if value is not None:
                                if 'time' in field:
                                    print(f"      • {field}: {value:.4f}s")
                                elif 'latency' in field:
                                    print(f"      • {field}: {value:.4f}ms")
                                elif 'percent' in field or 'cpu' in field or 'mem' in field:
                                    print(f"      • {field}: {value:.2f}%")
                                else:
                                    print(f"      • {field}: {value:.2f}")
            else:
                # For scenarios without operations
                for record in db_results[db_name]:
                    for field in scenario_info['fields']:
                        value = record.get(field)
                        if value is not None:
                            if 'time' in field:
                                print(f"    • {field}: {value:.4f}s")
                            elif 'latency' in field:
                                print(f"    • {field}: {value:.4f}ms")
                            elif 'throughput' in field:
                                print(f"    • {field}: {value:.0f} ops/sec")
                            elif 'percent' in field or 'cpu' in field or 'mem' in field:
                                print(f"    • {field}: {value:.2f}%")
                            else:
                                print(f"    • {field}: {value:.2f}")
                    break  # Only show first record for non-operation scenarios

def compare_databases(scenario_key, metric, operation=None):
    """Compare a specific metric across all databases"""
    data = query_scenario_data(scenario_key)
    
    if not data:
        return None
    
    results = {}
    for record in data:
        db_name = record.get('database')
        if operation:
            if record.get('operation') == operation:
                value = record.get(metric)
                if value is not None:
                    results[db_name] = value
        else:
            value = record.get(metric)
            if value is not None:
                results[db_name] = value
    
    return results

def display_comparison():
    """Display key comparisons between databases"""
    print_header("🏆 DATABASE PERFORMANCE COMPARISON")
    
    comparisons = [
        ("scenario1_crud", "latency_ms", "insert", "CRUD Insert Latency (ms) - Lower is better"),
        ("scenario1_crud", "latency_ms", "read", "CRUD Read Latency (ms) - Lower is better"),
        ("scenario2_iot", "insert_throughput", None, "IoT Insert Throughput (records/sec) - Higher is better"),
        ("scenario4_keyvalue", "get_latency_ms", None, "Key-Value GET Latency (ms) - Lower is better"),
    ]
    
    for scenario, metric, operation, title in comparisons:
        results = compare_databases(scenario, metric, operation)
        if results:
            print(f"\n  📈 {title}")
            print_separator()
            
            # Sort results
            sorted_results = sorted(results.items(), key=lambda x: x[1], 
                                   reverse=("throughput" in metric or "Higher" in title))
            
            for i, (db, value) in enumerate(sorted_results, 1):
                if "throughput" in metric:
                    print(f"    {i}. {db:12s} : {value:>10.0f} ops/sec")
                elif "latency" in metric or "time" in metric:
                    print(f"    {i}. {db:12s} : {value:>10.4f} ms")
                else:
                    print(f"    {i}. {db:12s} : {value:>10.2f}")


def display_summary():
    """Display overall summary"""
    print_header("📋 BENCHMARK SUMMARY")
    
    total_measurements = 0
    scenarios_with_data = []
    
    for scenario_key in SCENARIOS.keys():
        data = query_scenario_data(scenario_key)
        if data:
            total_measurements += len(data)
            scenarios_with_data.append(scenario_key)
    
    print(f"\n  • Total scenarios with data: {len(scenarios_with_data)}/{len(SCENARIOS)}")
    print(f"  • Total measurements: {total_measurements}")
    print(f"  • Databases tested: {', '.join(DATABASES)}")
    print(f"  • InfluxDB Bucket: {INFLUX_BUCKET}")
    print(f"  • Organization: {INFLUX_ORG}")
    
    if scenarios_with_data:
        print(f"\n  Scenarios with data:")
        for scenario in scenarios_with_data:
            print(f"    ✅ {SCENARIOS[scenario]['name']} ({scenario})")
    
    missing_scenarios = set(SCENARIOS.keys()) - set(scenarios_with_data)
    if missing_scenarios:
        print(f"\n  Scenarios without data:")
        for scenario in missing_scenarios:
            print(f"    ⚠️  {SCENARIOS[scenario]['name']} ({scenario})")

def generate_pdf_report():
    """Generate PDF report if the generate_professional_pdf_report.py script exists"""
    import os
    if os.path.exists("generate_professional_pdf_report.py"):
        try:
            import subprocess
            result = subprocess.run(["python", "generate_professional_pdf_report.py"], 
                                  capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print("  📄 PDF Report: rapport_benchmark_nosql_professionnel.pdf")
            else:
                print(f"  ⚠️  PDF generation failed: {result.stderr}")
        except Exception as e:
            print(f"  ⚠️  Could not generate PDF report: {e}")
    else:
        print("  ℹ️  PDF generation script not found")

def main():
    """Main function to analyze all benchmark results"""
    print("\n" + "#"*80)
    print("#" + " "*78 + "#")
    print("#" + " "*20 + "NOSQL BENCHMARK ANALYSIS" + " "*33 + "#")
    print("#" + " "*78 + "#")
    print("#"*80)
    
    # Display summary first
    display_summary()
    
    # Analyze each scenario
    for scenario_key, scenario_info in SCENARIOS.items():
        analyze_scenario(scenario_key, scenario_info)
    
    # Display comparison
    display_comparison()
    
    # Generate PDF report
    try:
        generate_pdf_report()
    except Exception as e:
        print(f"⚠️  Could not generate PDF report: {e}")
    
    # Footer
    print_header("✅ ANALYSIS COMPLETE")
    print(f"\n  💡 View detailed metrics at: {INFLUX_URL}")
    print(f"  📊 Grafana Dashboard: http://localhost:3000")
    print(f"  📄 PDF Report: Veuillez consulter le rapport dans le dossier results")
    # Close client
    client.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        client.close()
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        client.close()