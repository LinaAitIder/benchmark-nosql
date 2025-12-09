# 🔍 Analyse des Performances des Modèles de Bases de Données NoSQL
## Requêtes InfluxDB
- _**Le temps de réponse des opérations CRUD**_
    ```
    from(bucket: "benchmark")
    |> range(start: -24h)
    |> filter(fn: (r) => r["_measurement"] == "scenario1_crud")
    |> filter(fn: (r) => r["_field"] == "latency_ms")
    |> filter(fn: (r) => r["operation"] == "insert")
    |> group(columns: ["database"])
    |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    |> yield(name: "insert_latency")
    ```
- _**Utilisation du CPU**_
    ``` from(bucket: "benchmark")
    |> range(start: -24h)
    |> filter(fn: (r) => r["_measurement"] == "scenario1_crud")
    |> filter(fn: (r) => r["_field"] == "cpu_percent")
    |> filter(fn: (r) => r["operation"] == "insert")
    |> group(columns: ["database"])
    |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    |> yield(name: "cpu_insert")
    
    ```
- _**La performance de tous les métriques d'une BD spécifique**_
    ```
    from(bucket: "benchmark")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "scenario1_crud")
  |> filter(fn: (r) => r["database"] == "Redis")
  |> group(columns: ["_field"])
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> yield(name: "redis_all_metrics")
  ```
- _**Comparaison des performances de BD**_ :
```
    from(bucket: "benchmark")
  |> range(start: -24h)
  |> filter(fn: (r) => r["database"] == "Redis")
  |> pivot(rowKey:["_time"], columnKey: ["_measurement"], valueColumn: "_value")
```
- _**Performance de Traversée de Graphes**_
```
  from(bucket: "benchmark")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "scenario3_graph_v2")
  |> filter(fn: (r) => r["_field"] == "friends_of_friends_time" or r["_field"] == "three_level_time")
  |> pivot(rowKey:["_time"], columnKey: ["database"], valueColumn: "_value")
  |> yield(name: "graph_traversal_performance")
  ```
- _**Performance IoT : Insertion vs Requêtage**_
```
  from(bucket: "benchmark")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "scenario2_iot")
  |> filter(fn: (r) => r["_field"] == "insert_time" or r["_field"] == "range_query_time")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> yield(name: "insert_vs_query_time")
  ```
-_**Débit d'Insertion IoT**_
```
from(bucket: "benchmark")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "scenario2_iot")
  |> filter(fn: (r) => r["_field"] == "insert_throughput")
  |> pivot(rowKey:["_time"], columnKey: ["database"], valueColumn: "_value")
  |> yield(name: "insertion_throughput")
```
## 💻 Visualisation des résultat en StreamLit
```cd ./analyse```

```streamlit run visualize_results.py ```

