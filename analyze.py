import subprocess
import re
import csv
import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import numpy as np

percentages = [0.3, 0.49, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
# Define multiple algorithms to compare
algorithms = [
    {
        'name': 'reducePC_IDLE',
        'cpp_file': 'src/reducePC_IDLE.cpp',
        'lib_name': 'libreducePC_IDLE.so',
        'color': 'b' # blue
    },
    {
        'name': 'PC_IDLE',
        'cpp_file': 'src/PC_IDLE.cpp',
        'lib_name': 'libPC_IDLE.so',
        'color': 'r' # red
    },
    {
        'name': 'EnergyBud_IDLE',
        'cpp_file': 'src/EnergyBud_IDLE.cpp',
        'lib_name': 'libEnergyBud.so',
        'color': 'g' # green
    },
    # Add more algorithms as needed
]

build_dir = 'build'
base_batsim_cmd = [
    'batsim',
    '-l', '', # Will be filled with the library path
    '0', '',
    '-p', 'assets/2machine.xml',
    '-w', 'assets/50jobs.json'
]

P_IDLE_M = 100.0
P_COMP_M = 203.12
P_COMP_A = 190.74
P_IDLE_A = 95

# Dictionary to store results for all algorithms
all_results = {alg['name']: [] for alg in algorithms}

def ensure_directories():
    """Make sure necessary directories exist"""
    os.makedirs(build_dir, exist_ok=True)
    os.makedirs("out", exist_ok=True)
    os.makedirs("src", exist_ok=True)

def modify_percentage_budget(cpp_file_path, percentage):
    """Modifies the percentage budget in the C++ source file"""
    if not os.path.exists(cpp_file_path):
        raise FileNotFoundError(f"Source file not found: {cpp_file_path}")
        
    with open(cpp_file_path, 'r') as f:
        lines = f.readlines()

    pattern = r'(double\s+pourcentage_budget\s*=\s*)(\d+\.?\d*)(\s*;)'
    modified = False

    for i, line in enumerate(lines):
        if re.search(pattern, line):
            lines[i] = re.sub(pattern, fr'\g<1>{percentage}\g<3>', line)
            modified = True
            break
    
    if not modified:
        print(f"Warning: Pattern not found in {cpp_file_path}")
    
    with open(cpp_file_path, 'w') as f:
        f.writelines(lines)

def run_simulation(lib_path):
    """Executes the Batsim simulation"""
    cmd = base_batsim_cmd.copy()
    cmd[2] = os.path.join('./', lib_path)  # Set the library path
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result

def parse_output():
    """Analyzes output files and calculates metrics"""
    # Check if output files exist
    if not os.path.exists("./out/schedule.csv"):
        print("Warning: schedule.csv not found")
        return {'utilization': 0, 'norm_energy': 0, 'avg_bsld': 0}
        
    # Read schedule file
    with open("./out/schedule.csv") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            print("Warning: schedule.csv is empty")
            return {'utilization': 0, 'norm_energy': 0, 'avg_bsld': 0}
        schedule = rows[0]
    
    makespan = float(schedule['makespan'])
    nb_machines = int(schedule['nb_computing_machines'])
    time_comp = float(schedule['time_computing'])
    time_idle = float(schedule['time_idle'])
    
    # Calculate metrics
    total_energy = (time_comp * P_COMP_A) + (time_idle * P_IDLE_A)
    if(makespan==0):
        utilization = 0
        max_energy = nb_machines * P_COMP_M
        norm_energy = total_energy / max_energy
    else:
        utilization = time_comp / (nb_machines * makespan)  
        max_energy = nb_machines * P_COMP_M * makespan
        norm_energy = total_energy / max_energy 
    
    # Read jobs file for BSLD
    if not os.path.exists("./out/jobs.csv"):
        print("Warning: jobs.csv not found")
        return {'utilization': utilization, 'norm_energy': norm_energy, 'avg_bsld': 0}
        
    with open("./out/jobs.csv") as f:
        jobs = list(csv.DictReader(f))
    
    bslds = []
    for job in jobs:
        if job['success'] == '1' and job['final_state'] == 'COMPLETED_SUCCESSFULLY':
            turnaround = float(job['turnaround_time'])
            walltime = float(job['requested_time'])
            bsld = max(turnaround / walltime, 1.0)
            bslds.append(bsld)
    
    avg_bsld = np.mean(bslds) if bslds else 0
    print(f"bsld: {avg_bsld}")
    print(f"energy: {norm_energy}")
    print(f"utilization: {utilization}")
    
    return {
        'utilization': utilization,
        'norm_energy': norm_energy,
        'avg_bsld': avg_bsld
    }

def plot_comparative_results(all_results):
    """Generates comparative plots of results for all algorithms"""
    metrics = ['utilization', 'norm_energy', 'avg_bsld']
    titles = ['System Utilization', 'Energy Consumption', 'Average Bounded Slowdown']
    y_labels = ['Normalized Utilization', 'Normalized Energy', 'Average BSLD']
    
    plt.figure(figsize=(18, 6))
    
    for i, metric in enumerate(metrics):
        plt.subplot(1, 3, i+1)
        
        for alg_name, results in all_results.items():
            if not results:
                continue
                
            percentages = [r['percentage']*100 for r in results]
            values = [r[metric] for r in results]
            
            # Find the algorithm's color from the algorithms list
            color = next((a['color'] for a in algorithms if a['name'] == alg_name), 'k')
            
            plt.plot(percentages, values, f'{color}o-', label=alg_name)
        
        plt.title(titles[i])
        plt.xlabel('Energy Budget (%)')
        plt.ylabel(y_labels[i])
        plt.grid(True)
        plt.legend()
    
    plt.tight_layout()
    plt.savefig('comparative_results.png')
    print("Plot saved as 'comparative_results.png'")

# Main execution
def main():
    ensure_directories()
    
    for algorithm in algorithms:
        print(f"\n=== Processing algorithm: {algorithm['name']} ===")
        
        # Check if source file exists
        if not os.path.exists(algorithm['cpp_file']):
            print(f"Warning: Source file {algorithm['cpp_file']} not found. Skipping algorithm.")
            continue
            
        algorithm_results = []
        
        for p in percentages:
            print(f"\nProcessing {algorithm['name']} with {p*100}% budget...")
            try:
                # Modify budget percentage in source file
                modify_percentage_budget(algorithm['cpp_file'], p)
                
                # Build the library
                build_cmd = ['ninja', '-C', build_dir]
                print(f"Building with: {' '.join(build_cmd)}")
                build_result = subprocess.run(build_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Set the library path for this algorithm
                lib_path = os.path.join(build_dir, algorithm['lib_name'])
                
                # Run simulation
                run_simulation(lib_path)
                
                # Parse output
                metrics = parse_output()
                algorithm_results.append({'percentage': p, **metrics})
                
            except FileNotFoundError as e:
                print(f"File not found error: {e}")
            except subprocess.CalledProcessError as e:
                print(f"Command failed: {e}")
                print(f"stdout: {e.stdout.decode('utf-8')}")
                print(f"stderr: {e.stderr.decode('utf-8')}")
            except Exception as e:
                print(f"Error for {p*100}%: {e}")
        
        # Store results for this algorithm
        all_results[algorithm['name']] = algorithm_results
        
        # Display results for this algorithm
        print(f"\nResults for {algorithm['name']}:")
        print("Budget | Utilization | Energy | BSLD")
        for r in algorithm_results:
            print(f"{r['percentage']*100:5.0f}% | {r['utilization']:10.3f} | {r['norm_energy']:7.3f} | {r['avg_bsld']:5.3f}")
    
    # Generate comparative plots
    plot_comparative_results(all_results)

if __name__ == "__main__":
    main()
