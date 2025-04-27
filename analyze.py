#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import re
import csv
import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import numpy as np

percentages = [0.3, 0.49, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
cpp_file = cpp_file = os.path.join(os.path.dirname(__file__), 'src/reducePC_IDLE.cpp')
build_dir = 'build'
batsim_cmd = [
    'batsim',
    '-l', os.path.join('./',build_dir, 'libreducePC_IDLE.so'),
    '0', '',
    '-p', 'assets/10machine.xml',
    '-w', 'assets/50jobs.json'
]

P_IDLE = 100.0
P_COMP = 203.12

results = []

def modify_percentage_budget(percentage):
    """Modifie le pourcentage de budget dans le code C++"""
    with open(cpp_file, 'r') as f:
        lines = f.readlines()

    pattern = r'(double\s+pourcentage_budget\s*=\s*)(\d+\.?\d*)(\s*;)'

    for i, line in enumerate(lines):
        if re.search(pattern, line):
            lines[i] = re.sub(pattern, fr'\g<1>{percentage}\g<3>', line)
            break
    
    with open(cpp_file, 'w') as f:
        f.writelines(lines)

def run_simulation(output_prefix):
    """Exécute la simulation Batsim"""
    cmd = batsim_cmd.copy()
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def parse_output(output_prefix):
    """Analyse les fichiers de sortie et calcule les métriques"""
    # Lecture du fichier schedule
    with open(f"./out/schedule.csv") as f:
        schedule = list(csv.DictReader(f))[0]
    
    makespan = float(schedule['makespan'])
    nb_machines = int(schedule['nb_computing_machines'])
    time_comp = float(schedule['time_computing'])
    time_idle = float(schedule['time_idle'])
    
    # Calcul des métriques
    utilization = 0
    if(makespan>0):
        utilization = time_comp / (nb_machines)
    max_energy = nb_machines * P_COMP
    total_energy = (time_comp * P_COMP) + (time_idle * P_IDLE)
    norm_energy = total_energy / max_energy
    
    # Lecture du fichier jobs pour BSLD
    with open(f"./out/jobs.csv") as f:
        jobs = list(csv.DictReader(f))
    
    bslds = []
    for job in jobs:
        if job['success'] == '1' and job['final_state'] == 'COMPLETED_SUCCESSFULLY':
            turnaround = float(job['turnaround_time'])
            walltime = float(job['requested_time'])
            bsld = max(turnaround / walltime, 1.0)
            bslds.append(bsld)
    
    avg_bsld = np.mean(bslds) if bslds else 0
    print(f"bsld {avg_bsld}")
    print(f"energy {norm_energy}")
    print(f"utilization {utilization}")

    
    return {
        'utilization': utilization,
        'norm_energy': norm_energy,
        'avg_bsld': avg_bsld
    }

def plot_results(results):
    """Génère les graphiques des résultats"""
    percentages = [r['percentage']*100 for r in results]
    utilizations = [r['utilization'] for r in results]
    energies = [r['norm_energy'] for r in results]
    bslds = [r['avg_bsld'] for r in results]
    
    plt.figure(figsize=(15, 5))
    
    # Graphique d'utilisation
    plt.subplot(1, 3, 1)
    plt.plot(percentages, utilizations, 'bo-')
    plt.title('Utilisation du système')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('Utilisation normalisée')
    plt.grid(True)
    
    # Graphique d'énergie
    plt.subplot(1, 3, 2)
    plt.plot(percentages, energies, 'ro-')
    plt.title('Consommation énergétique')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('Énergie normalisée')
    plt.grid(True)
    
    # Graphique de BSLD
    plt.subplot(1, 3, 3)
    plt.plot(percentages, bslds, 'go-')
    plt.title('Bounded Slowdown moyen')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('BSLD moyen')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results_plot.png')
    plt.show()

# Exécution principale
for p in percentages:
    print(f"Traitement {p*100}%...")
    try:
        modify_percentage_budget(p)
        subprocess.run(['ninja', '-C', build_dir], check=True)
        output_prefix = f"sim_{int(p*100)}"
        run_simulation(output_prefix)
        metrics = parse_output(output_prefix)
        results.append({'percentage': p, **metrics})
    except Exception as e:
        print(f"Erreur pour {p*100}%: {e}")

# Affichage des résultats
print("\nRésultats:")
print("Budget | Utilisation | Energie | BSLD")
for r in results:
    print(f"{r['percentage']*100:5.0f}% | {r['utilization']:10.3f} | {r['norm_energy']:7.3f} | {r['avg_bsld']:5.3f}")

# Génération des graphiques
plot_results(results)
