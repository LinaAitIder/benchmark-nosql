"""
Script principal pour lancer TOUS les benchmarks automatiquement
Usage: python run_all_benchmarks.py
"""
import subprocess
import time
import sys

# Liste de tous les scénarios
SCENARIOS = [
    "scenarios/scenario1_crud_benchmark.py",
    "scenarios/scenario2_iot_logs.py", 
    "scenarios/scenario3_graph_queries.py",
    "scenarios/scenario4_keyvalue_speed.py",
    "scenarios/scenario5_fulltext_search.py",
    "scenarios/scenario6_scalability.py"
]

def run_scenario(script_name):
    """Execute un scénario"""
    print(f"\n{'='*70}")
    print(f"🚀 LANCEMENT: {script_name}")
    print(f"{'='*70}\n")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=False
        )
        print(f"\n✅ {script_name} terminé avec succès")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erreur dans {script_name}: {e}")
        return False
    except KeyboardInterrupt:
        print("\n⚠️ Interruption utilisateur")
        return False

def main():
    print("="*70)
    print("🔥 BENCHMARK NOSQL - EXECUTION COMPLETE")
    print("="*70)
    print(f"📊 {len(SCENARIOS)} scénarios à exécuter")
    print("⏱️  Durée estimée: 15-30 minutes")
    print("="*70)
    
    input("\nAppuyez sur ENTREE pour démarrer...")
    
    start_time = time.time()
    success_count = 0
    
    # Exécuter chaque scénario
    for i, scenario in enumerate(SCENARIOS, 1):
        print(f"\n📍 Progression: {i}/{len(SCENARIOS)}")
        
        if run_scenario(scenario):
            success_count += 1
        
        # Pause entre les scénarios
        if i < len(SCENARIOS):
            print("\n⏸️  Pause de 3 secondes...")
            time.sleep(3)
    
    # Résumé
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("✅ BENCHMARK TERMINÉ")
    print("="*70)
    print(f"⏱️  Temps total: {elapsed/60:.1f} minutes")
    print(f"📊 Réussis: {success_count}/{len(SCENARIOS)}")
    print(f"📈 Résultats disponibles sur:")
    print(f"   - Grafana: http://localhost:3000")
    print(f"   - InfluxDB: http://localhost:8086")
    print("="*70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Benchmark interrompu par l'utilisateur")
        sys.exit(1)