"""
Script pour générer un rapport PDF avec les vraies données d'InfluxDB
Utilisation: python generate_real_pdf_report.py
"""

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import io
import os
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration InfluxDB
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ensa")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "bench")

# Initialiser le client InfluxDB
try:
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    influx_available = True
except Exception as e:
    print(f"Avertissement: Impossible de se connecter à InfluxDB: {e}")
    influx_available = False

# Noms des bases de données
BASES_DE_DONNEES = ["MongoDB", "Redis", "Cassandra", "Neo4j"]

# Configurations des scénarios
SCENARIOS = {
    "scenario1_crud": {
        "nom": "Opérations CRUD",
        "description": "Analyse des opérations de base sur les données : INSERTION, LECTURE, MISE À JOUR, SUPPRESSION",
        "champs": ["latency_ms", "total_time", "cpu_percent", "memory_percent"],
        "operations": ["insert", "read", "update", "delete"]
    },
    "scenario2_iot": {
        "nom": "IoT/Journaux (Time-Series)",
        "description": "Analyse de l'ingestion de données de séries temporelles et des performances de requêtage",
        "champs": ["insert_time", "insert_throughput", "range_query_time", "insert_cpu", "insert_mem"]
    },
    "scenario3_graph": {
        "nom": "Requêtes Graphiques",
        "description": "Analyse des relations de type réseau social et de la traversée de graphes",
        "champs": ["create_users_time", "create_friendships_time", "friends_of_friends_time", "three_level_time"]
    },
    "scenario4_keyvalue": {
        "nom": "Vitesse Clé-Valeur",
        "description": "Analyse des opérations GET/SET ultra-rapides",
        "champs": ["set_latency_ms", "get_latency_ms", "throughput_ops", "cpu_usage"]
    },
    "scenario5_fulltext": {
        "nom": "Recherche Full-Text",
        "description": "Analyse de l'indexation de texte et des opérations de recherche",
        "champs": ["insert_time", "index_build_time", "search_latency", "cpu_usage"]
    },
    "scenario6_scalability": {
        "nom": "Test de Scalabilité",
        "description": "Analyse des opérations multi-thread et des charges concurrentes",
        "champs": ["create_time", "read_time", "update_time", "delete_time", "throughput_ops"]
    }
}

def formater_valeur_iot(valeur, champ):
    """Formater les valeurs pour le scénario IoT de manière spécifique"""
    if valeur is None:
        return "N/A"
    
    if 'throughput' in champ:
        if valeur >= 1000000:
            return f"{valeur/1000000:.2f}M"
        elif valeur >= 1000:
            return f"{valeur/1000:.1f}K"
        else:
            return f"{valeur:,.0f}"
    elif 'time' in champ:
        if valeur >= 1:
            return f"{valeur:.2f} s"
        elif valeur >= 0.001:
            return f"{valeur*1000:.1f} ms"
        else:
            return f"{valeur*1000:.3f} ms"
    elif 'cpu' in champ or 'mem' in champ:
        return f"{valeur:.1f} %"
    else:
        return f"{valeur:.4f}"

def interroger_donnees_scenario(nom_scenario, plage_temps="-24h"):
    """Interroger les VRAIES données d'un scénario depuis InfluxDB"""
    if not influx_available:
        print(f"❌ InfluxDB non disponible pour {nom_scenario}")
        return []
        
    requete = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {plage_temps})
      |> filter(fn: (r) => r["_measurement"] == "{nom_scenario}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        tables = query_api.query(requete, org=INFLUX_ORG)
        resultats = []
        for table in tables:
            for record in table.records:
                resultats.append(record.values)
        
        print(f"✅ Données récupérées pour {nom_scenario}: {len(resultats)} enregistrements")
        return resultats
    except Exception as e:
        print(f"❌ Erreur lors de l'interrogation de {nom_scenario}: {e}")
        return []

def extraire_donnees_structurees(donnees_brutes):
    """Structure les données brutes d'InfluxDB pour l'analyse"""
    donnees_structurees = {}
    
    for cle_scenario in SCENARIOS.keys():
        donnees_structurees[cle_scenario] = []
        
        # Filtrer les données pour ce scénario
        for record in donnees_brutes.get(cle_scenario, []):
            donnee_struct = {}
            
            # Copier les champs pertinents
            for champ in SCENARIOS[cle_scenario]['champs']:
                if champ in record:
                    donnee_struct[champ] = record[champ]
            
            # Ajouter le nom de la base de données
            if 'database' in record:
                donnee_struct['database'] = record['database']
            
            # Ajouter l'opération pour CRUD
            if cle_scenario == "scenario1_crud" and 'operation' in record:
                donnee_struct['operation'] = record['operation']
            
            if donnee_struct:  # Ne pas ajouter d'enregistrements vides
                donnees_structurees[cle_scenario].append(donnee_struct)
    
    return donnees_structurees

def calculer_moyennes_par_base(donnees_structurees):
    """Calcule les moyennes par base de données"""
    donnees_moyennes = {}
    
    for cle_scenario, info_scenario in SCENARIOS.items():
        donnees_moyennes[cle_scenario] = []
        
        # Grouper par base de données
        donnees_par_base = {}
        for record in donnees_structurees[cle_scenario]:
            db = record.get('database', 'Inconnu')
            if db not in donnees_par_base:
                donnees_par_base[db] = {'count': 0, 'values': {}}
            
            # Pour CRUD, grouper aussi par opération
            if cle_scenario == "scenario1_crud":
                op = record.get('operation', 'unknown')
                key = f"{db}_{op}"
            else:
                key = db
            
            if key not in donnees_par_base[db]['values']:
                donnees_par_base[db]['values'][key] = {}
            
            # Accumuler les valeurs
            for champ in info_scenario['champs']:
                if champ in record and record[champ] is not None:
                    if champ not in donnees_par_base[db]['values'][key]:
                        donnees_par_base[db]['values'][key][champ] = []
                    donnees_par_base[db]['values'][key][champ].append(record[champ])
            donnees_par_base[db]['count'] += 1
        
        # Calculer les moyennes
        for db, data in donnees_par_base.items():
            for key, valeurs in data['values'].items():
                record_moyen = {'database': db}
                
                if cle_scenario == "scenario1_crud":
                    # Extraire l'opération de la clé
                    parts = key.split('_')
                    if len(parts) > 1:
                        record_moyen['operation'] = parts[1]
                
                for champ, vals in valeurs.items():
                    if vals:
                        record_moyen[champ] = sum(vals) / len(vals)
                
                donnees_moyennes[cle_scenario].append(record_moyen)
    
    return donnees_moyennes

def creer_graphique_reel(donnees, cle_scenario, titre_graphique, etiquette_y, champ_metrique):
    """Créer un graphique avec les VRAIES données d'InfluxDB et des couleurs"""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # Extraire les données réelles pour le tracé
    bases_donnees = []
    valeurs = []
    
    info_scenario = SCENARIOS[cle_scenario]
    
    if cle_scenario == "scenario1_crud":
        # Pour CRUD, utiliser l'opération d'insertion pour le graphique
        for enregistrement in donnees[cle_scenario]:
            if enregistrement.get("operation") == "insert":
                bases_donnees.append(enregistrement["database"])
                if champ_metrique in enregistrement:
                    valeurs.append(enregistrement[champ_metrique])
    else:
        # Pour les autres scénarios, utiliser la première valeur par base
        vus = set()
        for enregistrement in donnees[cle_scenario]:
            db = enregistrement["database"]
            if db not in vus:
                bases_donnees.append(db)
                if champ_metrique in enregistrement:
                    valeurs.append(enregistrement[champ_metrique])
                vus.add(db)
    
    # Si pas de données, afficher un message
    if len(bases_donnees) == 0 or len(valeurs) == 0:
        bases_donnees = ["Pas de données"]
        valeurs = [0]
        ax.set_title(f"{titre_graphique} - DONNÉES MANQUANTES", fontsize=12, color='red')
        couleurs = ['#cccccc']
    else:
        ax.set_title(titre_graphique, fontsize=14, pad=15, fontweight='bold', color='#2c3e50')
        
        # Palette de couleurs par type de scénario
        palettes = {
            "scenario1_crud": ['#3498db', '#2ecc71', '#e74c3c', '#f39c12'],  # Bleu, Vert, Rouge, Orange
            "scenario2_iot": ['#9b59b6', '#1abc9c', '#34495e', '#d35400'],   # Violet, Turquoise, Gris foncé, Orange foncé
            "scenario3_graph": ['#e74c3c', '#27ae60', '#8e44ad', '#f39c12'], # Rouge, Vert, Violet, Orange
            "scenario4_keyvalue": ['#2c3e50', '#16a085', '#c0392b', '#7f8c8d'], # Noir, Vert foncé, Rouge foncé, Gris
            "scenario5_fulltext": ['#2980b9', '#27ae60', '#8e44ad', '#f1c40f'], # Bleu, Vert, Violet, Jaune
            "scenario6_scalability": ['#2c3e50', '#e74c3c', '#3498db', '#2ecc71'] # Noir, Rouge, Bleu, Vert
        }
        
        palette = palettes.get(cle_scenario, ['#4a4a4a', '#7a7a7a', '#a1a1a1', '#c9c9c9'])
        # Répéter la palette si nécessaire
        couleurs = []
        for i in range(len(bases_donnees)):
            couleurs.append(palette[i % len(palette)])
    
    # Créer les barres avec bordures
    barres = ax.bar(bases_donnees, valeurs, color=couleurs, width=0.6, edgecolor='white', linewidth=2)
    
    # Style amélioré
    ax.set_ylabel(etiquette_y, fontsize=11, fontweight='bold', color='#2c3e50')
    ax.set_xlabel("Base de données", fontsize=11, fontweight='bold', color='#2c3e50')
    
    # Valeurs sur les barres (seulement si nous avons des données réelles)
    if len(valeurs) > 0 and valeurs[0] != 0:
        for barre, valeur in zip(barres, valeurs):
            hauteur = barre.get_height()
            # Afficher les valeurs avec format adapté
            if valeur > 1000:
                texte = f'{valeur/1000:.1f}k'
            elif valeur > 100:
                texte = f'{valeur:.0f}'
            else:
                texte = f'{valeur:.2f}'
            
            ax.text(barre.get_x() + barre.get_width()/2., hauteur + (hauteur*0.02),
                    texte,
                    ha='center', va='bottom', fontsize=10, fontweight='bold',
                    color='#2c3e50')
    
    plt.xticks(rotation=0, ha='center', fontsize=10, fontweight='bold')
    plt.yticks(fontsize=10)
    
    # Grille subtile
    ax.grid(axis='y', alpha=0.3, linestyle='--', color='#cccccc')
    
    # Ajouter un fond léger
    ax.set_facecolor('#f8f9fa')
    
    # Couleurs des bordures
    for spine in ax.spines.values():
        spine.set_color('#dddddd')
        spine.set_linewidth(1)
    
    plt.tight_layout()
    
    # Sauvegarder avec haute qualité
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    buf.seek(0)
    plt.close()
    
    return buf

def ajouter_en_tete_pied(canvas_doc, doc):
    """Ajouter un en-tête et un pied de page minimalistes"""
    canvas_doc.saveState()
    
    # En-tête simple
    canvas_doc.setFont("Helvetica", 10)
    canvas_doc.setFillColor(colors.black)
    canvas_doc.drawString(inch, 10.7*inch, "Rapport Benchmark NoSQL - Données Réelles InfluxDB")
    
    # Ligne séparatrice
    canvas_doc.setLineWidth(0.5)
    canvas_doc.setStrokeColor(colors.HexColor('#cccccc'))
    canvas_doc.line(inch, 10.6*inch, 7.5*inch, 10.6*inch)
    
    # Pied de page
    canvas_doc.setFont("Helvetica", 8)
    canvas_doc.drawRightString(7.5*inch, 0.5*inch, f"Page {canvas_doc.getPageNumber()}")
    canvas_doc.drawString(inch, 0.5*inch, f"{datetime.now().strftime('%d/%m/%Y %H:%M')}")
    
    canvas_doc.restoreState()

def creer_styles_simples():
    """Créer des styles minimalistes"""
    styles = getSampleStyleSheet()
    
    # Titre principal
    styles.add(ParagraphStyle(
        name='TitrePrincipal',
        parent=styles['Heading1'],
        fontSize=20,
        spaceAfter=20,
        alignment=TA_CENTER,
        textColor=colors.black,
        leading=24,
        fontName='Helvetica-Bold'
    ))
    
    # Titre de section
    styles.add(ParagraphStyle(
        name='TitreSection',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=12,
        spaceBefore=20,
        alignment=TA_LEFT,
        textColor=colors.black,
        leading=20,
        fontName='Helvetica-Bold'
    ))
    
    # Titre de scénario
    styles.add(ParagraphStyle(
        name='TitreScenario',
        parent=styles['Heading2'],
        fontSize=14,
        spaceAfter=8,
        spaceBefore=16,
        alignment=TA_LEFT,
        textColor=colors.HexColor('#333333'),
        leading=18,
        fontName='Helvetica-Bold'
    ))
    
    # Description
    styles.add(ParagraphStyle(
        name='Description',
        parent=styles['Normal'],
        fontSize=11,
        spaceAfter=10,
        alignment=TA_JUSTIFY,
        leading=15,
        textColor=colors.HexColor('#444444')
    ))
    
    # Texte simple
    styles.add(ParagraphStyle(
        name='TexteSimple',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=8,
        alignment=TA_LEFT,
        leading=14,
        textColor=colors.HexColor('#666666')
    ))
    
    # Avertissement
    styles.add(ParagraphStyle(
        name='Avertissement',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=10,
        alignment=TA_CENTER,
        leading=14,
        textColor=colors.red,
        backColor=colors.HexColor('#ffebee'),
        borderColor=colors.red,
        borderWidth=1,
        borderPadding=5
    ))
    
    return styles

def generer_page_garde(histoire, styles, donnees_reelles):
    """Générer une page de garde simple"""
    histoire.append(Spacer(1, 120))
    
    # Titre principal
    histoire.append(Paragraph("RAPPORT D'ANALYSE", styles['TitrePrincipal']))
    histoire.append(Paragraph("BENCHMARK NoSQL", styles['TitrePrincipal']))
    histoire.append(Spacer(1, 20))
    
    # Avertissement si données manquantes
    total_scenarios = len(SCENARIOS)
    scenarios_avec_donnees = sum(1 for s in SCENARIOS if donnees_reelles.get(s))
    
    if scenarios_avec_donnees < total_scenarios:
        histoire.append(Paragraph(
            f"⚠️ {total_scenarios - scenarios_avec_donnees} scénario(s) sans données", 
            styles['Avertissement']
        ))
        histoire.append(Spacer(1, 10))
    
    # Ligne séparatrice
    ligne = Table([[""]], colWidths=[6*inch])
    ligne.setStyle(TableStyle([
        ('LINEABOVE', (0, 0), (0, 0), 1, colors.HexColor('#cccccc')),
    ]))
    histoire.append(ligne)
    histoire.append(Spacer(1, 40))
    
    # Informations avec données réelles
    info_data = [
        ["Date", datetime.now().strftime('%d %B %Y')],
        ["Heure", datetime.now().strftime('%H:%M')],
        ["Source", "InfluxDB"],
        ["Bucket", INFLUX_BUCKET],
        ["Bases testées", ", ".join(BASES_DE_DONNEES)],
        ["Scénarios avec données", f"{scenarios_avec_donnees}/{total_scenarios}"]
    ]
    
    for label, value in info_data:
        histoire.append(Paragraph(f"<b>{label}:</b> {value}", styles['TexteSimple']))
        histoire.append(Spacer(1, 6))
    
    histoire.append(PageBreak())

def generer_analyse_scenario(histoire, styles, donnees):
    """Générer l'analyse par scénario avec données réelles"""
    histoire.append(Paragraph("4. Scénarios de test - Données Réelles", styles['TitreSection']))
    histoire.append(Spacer(1, 20))
    
    for i, (cle_scenario, info_scenario) in enumerate(SCENARIOS.items(), 1):
        histoire.append(Paragraph(f"4.{i} {info_scenario['nom']}", styles['TitreScenario']))
        histoire.append(Spacer(1, 8))
        histoire.append(Paragraph(info_scenario['description'], styles['TexteSimple']))
        histoire.append(Spacer(1, 12))
        
        # Vérifier si nous avons des données pour ce scénario
        if cle_scenario not in donnees or len(donnees[cle_scenario]) == 0:
            histoire.append(Paragraph(
                "⚠️ Aucune donnée disponible pour ce scénario dans InfluxDB", 
                styles['Avertissement']
            ))
            histoire.append(Spacer(1, 15))
            continue
        
        # Tableau avec données réelles
        if cle_scenario == "scenario1_crud":
            # Tableau CRUD par opération
            for operation in info_scenario['operations']:
                histoire.append(Paragraph(f"Opération {operation.upper()}", styles['TexteSimple']))
                histoire.append(Spacer(1, 6))
                
                # Préparer les données pour cette opération
                donnees_op = [["Base", "Latence (ms)", "Temps total (s)", "CPU (%)", "Mémoire (%)"]]
                
                for db in BASES_DE_DONNEES:
                    # Chercher les données pour cette base et cette opération
                    donnee = next((d for d in donnees[cle_scenario] 
                                 if d.get("database") == db and d.get("operation") == operation), None)
                    
                    if donnee:
                        ligne = [
                            db,
                            f"{donnee.get('latency_ms', 'N/A'):.4f}" if donnee.get('latency_ms') is not None else "N/A",
                            f"{donnee.get('total_time', 'N/A'):.4f}" if donnee.get('total_time') is not None else "N/A",
                            f"{donnee.get('cpu_percent', 'N/A'):.2f}" if donnee.get('cpu_percent') is not None else "N/A",
                            f"{donnee.get('memory_percent', 'N/A'):.2f}" if donnee.get('memory_percent') is not None else "N/A"
                        ]
                        donnees_op.append(ligne)
                
                if len(donnees_op) > 1:  # Au moins une ligne de données
                    tableau = Table(donnees_op, colWidths=[1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch])
                    tableau.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A90E2')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 9),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#eeeeee')),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 5),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                        ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')])
                    ]))
                    histoire.append(tableau)
                    histoire.append(Spacer(1, 12))
        else:
            # Tableau pour les autres scénarios
            entetes = ["Base"]
            for champ in info_scenario['champs']:
                nom_champ = champ.replace("_", " ").title()
                # Traductions spécifiques pour IoT
                if champ == "insert_time":
                    nom_champ = "Insert Time"
                elif champ == "insert_throughput":
                    nom_champ = "Insert Throughput"
                elif champ == "range_query_time":
                    nom_champ = "Range Query Time"
                elif champ == "insert_cpu":
                    nom_champ = "CPU %"
                elif champ == "insert_mem":
                    nom_champ = "Memory %"
                entetes.append(nom_champ)
            
            donnees_tableau = [entetes]
            
            # Ajouter les données par base
            for db in BASES_DE_DONNEES:
                # Chercher les données pour cette base
                donnee = next((d for d in donnees[cle_scenario] 
                             if d.get("database") == db), None)
                
                if donnee:
                    ligne = [db]
                    if cle_scenario == "scenario2_iot":
                        # Format spécial pour IoT
                        for champ in info_scenario['champs']:
                            valeur = donnee.get(champ)
                            if valeur is not None:
                                ligne.append(formater_valeur_iot(valeur, champ))
                            else:
                                ligne.append("N/A")
                    else:
                        for champ in info_scenario['champs']:
                            valeur = donnee.get(champ)
                            if valeur is not None:
                                if 'time' in champ:
                                    ligne.append(f"{valeur:.4f} s")
                                elif 'latency' in champ:
                                    ligne.append(f"{valeur:.4f} ms")
                                elif 'throughput' in champ:
                                    ligne.append(f"{valeur:.0f} ops/sec")
                                elif 'percent' in champ or 'cpu' in champ or 'mem' in champ:
                                    ligne.append(f"{valeur:.2f} %")
                                else:
                                    ligne.append(f"{valeur:.2f}")
                            else:
                                ligne.append("N/A")
                    donnees_tableau.append(ligne)
            
            if len(donnees_tableau) > 1:  # Au moins une ligne de données
                # Définir les largeurs de colonnes selon le scénario
                if cle_scenario == "scenario2_iot":
                    col_widths = [1.2*inch, 1.1*inch, 1.3*inch, 1.3*inch, 1.0*inch, 1.0*inch]
                else:
                    col_widths = [1.2*inch] + [1.5*inch]*(len(entetes)-1)
                
                tableau = Table(donnees_tableau, colWidths=col_widths)
                tableau.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A90E2')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dddddd')),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 5),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    # Alternance de couleurs pour les lignes
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')])
                ]))
                histoire.append(tableau)
                histoire.append(Spacer(1, 15))
        
        # Graphique avec données réelles
        try:
            # Sélection métrique selon scénario
            if cle_scenario == "scenario1_crud":
                champ, etiq = "latency_ms", "ms"
            elif cle_scenario == "scenario2_iot":
                champ, etiq = "insert_throughput", "ops/sec"
            elif cle_scenario == "scenario3_graph":
                champ, etiq = "friends_of_friends_time", "sec"
            elif cle_scenario == "scenario4_keyvalue":
                champ, etiq = "throughput_ops", "ops/sec"
            elif cle_scenario == "scenario5_fulltext":
                champ, etiq = "search_latency", "sec"
            else:
                champ, etiq = "throughput_ops", "ops/sec"
            
            graphique = creer_graphique_reel(donnees, cle_scenario, 
                                           info_scenario['nom'], etiq, champ)
            img = Image(graphique, width=6.5*inch, height=3.5*inch)
            histoire.append(img)
            histoire.append(Spacer(1, 20))
        except Exception as e:
            print(f"Graphique non généré pour {cle_scenario}: {e}")
        
        histoire.append(PageBreak())

def generer_rapport_pdf():
    """Générer un rapport PDF avec les VRAIES données d'InfluxDB"""
    print("📊 Récupération des données depuis InfluxDB...")
    
    # Récupérer les VRAIES données d'InfluxDB
    donnees_brutes = {}
    for cle_scenario in SCENARIOS.keys():
        print(f"  • Récupération {cle_scenario}...")
        donnees_brutes[cle_scenario] = interroger_donnees_scenario(cle_scenario)
    
    # Structurer et calculer les moyennes
    print("📈 Traitement des données...")
    donnees_structurees = extraire_donnees_structurees(donnees_brutes)
    donnees = calculer_moyennes_par_base(donnees_structurees)
    
    # Créer le dossier results
    chemin_results = os.path.join(os.getcwd(), "results")
    if not os.path.exists(chemin_results):
        os.makedirs(chemin_results)
        print(f"Dossier créé : {chemin_results}")
    
    # Créer le nom de fichier avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nom_fichier = f"rapport_benchmark_reel_{timestamp}.pdf"
    chemin_complet = os.path.join(chemin_results, nom_fichier)
    
    # Créer le document
    doc = SimpleDocTemplate(chemin_complet, pagesize=A4,
                           rightMargin=72, leftMargin=72,
                           topMargin=80, bottomMargin=60)
    
    # Créer les styles
    styles = creer_styles_simples()
    
    # Construire le rapport
    histoire = []
    
    # Page de garde
    histoire.append(Spacer(1, 120))
    histoire.append(Paragraph("RAPPORT D'ANALYSE - DONNÉES RÉELLES", styles['TitrePrincipal']))
    histoire.append(Paragraph("BENCHMARK NoSQL InfluxDB", styles['TitrePrincipal']))
    histoire.append(Spacer(1, 40))
    
    # Statistiques
    total_scenarios = len(SCENARIOS)
    scenarios_avec_donnees = sum(1 for s in SCENARIOS if donnees_brutes.get(s) and len(donnees_brutes[s]) > 0)
    
    info_data = [
        ["Date", datetime.now().strftime('%d %B %Y %H:%M')],
        ["Source", "InfluxDB"],
        ["Bucket", INFLUX_BUCKET],
        ["Scénarios avec données", f"{scenarios_avec_donnees}/{total_scenarios}"],
        ["Bases testées", ", ".join(BASES_DE_DONNEES)]
    ]
    
    for label, value in info_data:
        histoire.append(Paragraph(f"<b>{label}:</b> {value}", styles['TexteSimple']))
        histoire.append(Spacer(1, 8))
    
    histoire.append(PageBreak())
    
    # Analyse par scénario
    generer_analyse_scenario(histoire, styles, donnees)
    
    # Conclusion
    histoire.append(Paragraph("5. Conclusion", styles['TitreSection']))
    histoire.append(Spacer(1, 15))
    
    if scenarios_avec_donnees > 0:
        histoire.append(Paragraph(
            f"✅ Rapport généré avec {scenarios_avec_donnees} scénario(s) de données réelles depuis InfluxDB.",
            styles['Description']
        ))
    else:
        histoire.append(Paragraph(
            "⚠️ Aucune donnée réelle trouvée dans InfluxDB. Vérifiez votre configuration.",
            styles['Avertissement']
        ))
    
    histoire.append(Spacer(1, 10))
    histoire.append(Paragraph(
        "Les données présentées sont les moyennes des mesures collectées en temps réel.",
        styles['TexteSimple']
    ))
    
    # Générer le PDF
    doc.build(histoire, onFirstPage=ajouter_en_tete_pied, onLaterPages=ajouter_en_tete_pied)
    
    print(f"\n✅ Rapport PDF généré avec DONNÉES RÉELLES : {chemin_complet}")
    return chemin_complet

if __name__ == "__main__":
    try:
        fichier = generer_rapport_pdf()
        print(f"📄 Fichier PDF disponible : {fichier}")
    except Exception as e:
        print(f"❌ Erreur lors de la génération du rapport : {e}")
        import traceback
        traceback.print_exc()
    finally:
        if influx_available:
            client.close()