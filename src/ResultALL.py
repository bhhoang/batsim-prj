import subprocess
import re
import csv
import os
import matplotlib.pyplot as plt
import numpy as np

percentages = [0.3, 0.49, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
mach = 'assets/10machine.xml'
jobs = 'assets/50jobs.json'

cpp_file1 = os.path.join(os.path.dirname(__file__), 'EnergyBud_IDLE.cpp')
cpp_file2 = os.path.join(os.path.dirname(__file__), 'PC_IDLE.cpp')
cpp_file3 = os.path.join(os.path.dirname(__file__), 'reducePC_IDLE.cpp')
cpp_files = [cpp_file1,cpp_file2,cpp_file3]
build_dir = 'build'
batsim_cmd_1 = [
    'batsim',
    '-l', os.path.join('./',build_dir, 'libEnergyBud.so'),
    '0', '',
    '-p', mach,
    '-w', jobs
]
batsim_cmd_2 = [
    'batsim',
    '-l', os.path.join('./',build_dir, 'libPC_IDLE.so'),
    '0', '',
    '-p', mach,
    '-w', jobs
]
batsim_cmd_3 = [
    'batsim',
    '-l', os.path.join('./',build_dir, 'reducePC_IDLE.so'),
    '0', '',
    '-p', mach,
    '-w', jobs
]
batsim_cmds = [batsim_cmd_1,batsim_cmd_2,batsim_cmd_3]
P_IDLE_M = 100.0
P_COMP_M = 203.12
P_COMP_A = 190.74
P_IDLE_A = 95

results = [[] for _ in range(3)]

def modify_percentage_budget(percentage):

    """Modifie le pourcentage de budget dans le code C++"""
    for file in range(0,3):
        with open(cpp_files[file], 'r') as f:
            lines = f.readlines()
        
        pattern = r'(double\s+percentage_budget\s*=\s*)(\d+\.?\d*)(\s*;)'
        for i, line in enumerate(lines):
            if re.search(pattern, line):
                lines[i] = re.sub(pattern, fr'\g<1>{percentage}\g<3>', line)
                break
        
        with open(cpp_files[file], 'w') as f:
            f.writelines(lines)

def run_simulation(batsim_cmd):
    """Exécute la simulation Batsim"""
    cmd = batsim_cmds[batsim_cmd].copy()
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def parse_output(percent):
    """Analyse les fichiers de sortie et calcule les métriques"""
    # Lecture du fichier schedule
    with open(f"out/schedule.csv") as f:
        schedule = list(csv.DictReader(f))[0]
    
    makespan = float(schedule['makespan'])
    nb_machines = int(schedule['nb_computing_machines'])
    time_comp = float(schedule['time_computing'])
    time_idle = float(schedule['time_idle'])
    job_complete = float(schedule['nb_jobs_finished'])
    
    # Calcul des métriques
    total_energy = (time_comp * P_COMP_A) + (time_idle * P_IDLE_A)
    if(makespan==0):
        utilization = 0
        max_energy = nb_machines * P_COMP_M * percent
        norm_energy = total_energy / max_energy
    else:
        utilization = time_comp / (nb_machines * makespan)  
        max_energy = nb_machines * P_COMP_M * makespan * percent
        norm_energy = total_energy / max_energy 
    
    
    # Lecture du fichier jobs pour BSLD
    with open(f"out/jobs.csv") as f:
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
    print(f"total énergy : {total_energy} / {max_energy} = {norm_energy}")
    print(f"utilization {utilization}")
    print(f"nbr job complete {job_complete}")
    print(f"temp total {makespan}")
    print(f"temp calcul{time_comp}, temp iddle{time_idle}, conso calcul{(time_comp * P_COMP_A)}, conso iddle {(time_idle * P_IDLE_A)}, conso totale {total_energy}")
    
    return {
        'utilization': utilization,
        'norm_energy': norm_energy,
        'avg_bsld': avg_bsld
    }

def plot_results(results):
    """Génère les graphiques des résultats par fichier"""
    filenames = ["EnergyBud_IDLE", "PC_IDLE", "reducePC_IDLE"]
    colors = ['b', 'r', 'g']  # bleu, rouge, vert
    
    plt.figure(figsize=(15, 5))
    
    # Graphique d'utilisation
    plt.subplot(1, 3, 1)
    for i in range(3):
        percentages = [r['percentage']*100 for r in results[i]]
        utilizations = [r['utilization'] for r in results[i]]
        plt.plot(percentages, utilizations, f'{colors[i]}o-', label=filenames[i])
    plt.title('Utilisation du système')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('Utilisation normalisée')
    plt.legend()
    plt.grid(True)
    
    # Graphique d'énergie
    plt.subplot(1, 3, 2)
    for i in range(3):
        percentages = [r['percentage']*100 for r in results[i]]
        energies = [r['norm_energy'] for r in results[i]]
        plt.plot(percentages, energies, f'{colors[i]}o-', label=filenames[i])
    plt.title('Consommation énergétique')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('Énergie normalisée')
    plt.legend()
    plt.grid(True)
    
    # Graphique de BSLD
    plt.subplot(1, 3, 3)
    for i in range(3):
        percentages = [r['percentage']*100 for r in results[i]]
        bslds = [r['avg_bsld'] for r in results[i]]
        plt.plot(percentages, bslds, f'{colors[i]}o-', label=filenames[i])
    plt.title('Bounded Slowdown moyen')
    plt.xlabel('Budget énergétique (%)')
    plt.ylabel('BSLD moyen')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results_plot.png')
    plt.show()


# Exécution principale
for p in percentages:
    print(f"Traitement {p*100}%...")
    modify_percentage_budget(p)
    for i in range(0,3):
        print(f"Traitement {cpp_files[i]}...")
        try:
            subprocess.run(['ninja', '-C', build_dir], check=True)
            run_simulation(i)
            metrics = parse_output(p)
            results[i].append({'percentage': p,'algo':cpp_files[i], **metrics})
        except Exception as e:
            print(f"Erreur pour {p*100}%: {e}")

# Affichage des résultats
print("\n=== Résultats finaux ===")
for i in range(3):
    print(f"\nAlgorithm: {os.path.basename(cpp_files[i])}")
    print("Percentage | Utilization | Norm Energy | Avg BSLD | Jobs Completed | Makespan")
    for r in results[i]:
        print(f"{r['percentage']*100:9.1f}% | {r['utilization']:11.3f} | {r['norm_energy']:11.3f} | {r['avg_bsld']:8.3f}")

# Génération des graphiques
plot_results(results)