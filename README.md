# Topic 3: Backfilling under an Energy Budget
This guide walks you through setting up the environment and running your first Batsim simulation related to energy-aware scheduling. The goal is to simulate scheduling algorithms like `energyBud_IDLE`, `PC_IDLE`, or `reducePC_IDLE` using Batsim.

Base Article: Towards Energy Budget Control in HPC. PF Dutot, Y Georgiou, D Glesser, L Lefèvre, M Poquet, I Raïs. https://hal.science/hal-01533417v1/file/towards_energy_budget_control_in_hpc.pdf

---

## Table of Contents
1. [Clone the Repository](#1-clone-the-repository)
2. [Set Up the Software Environment](#2-set-up-the-software-environment)
    - [Method 1: Using Nix (Recommended)](#method-1-using-nix-recommended)
    - [Method 2: Using Docker](#method-2-using-docker)
    - [Method 3: Manual Installation (Advanced)](#method-3-manual-installation-advanced)
3. [Running Simulations](#3-running-simulations)
    - [Option 1: Fully Automated Run with analyze.py (Recommend)](#option-1-fully-automated-run-with-analyzepy-recommend)
    - [Option 2: Manual Build and Run (run individually)](#option-1-fully-automated-run-with-analyzepy-recommend)

---

## 1. Clone the Repository
```bash
git clone https://gitlab.com/mpoquet-courses/sched-with-batsim.git
cd sched-with-batsim
```

---

## 2. Set Up the Software Environment

Before running, verify that the required software is available:
```bash
batsim --version
meson --version
ninja --version
pkg-config --version
gdb --version
cgdb --version
```

If not installed, choose **one** of the methods below:

---

### Method 1: Using Nix (Recommended)

1. Install Nix: [Nix installation guide](https://nixos.org/download/)  
   (or [nix-portable](https://github.com/DavHau/nix-portable) if no root access).
2. Enable Nix Flakes: [Enable Flakes Guide](https://nixos.wiki/wiki/Flakes).
3. (Optional) Speed up builds using binary cache:
   ```bash
   nix develop --extra-substituters 'https://capack.cachix.org' --extra-trusted-public-keys 'capack.cachix.org-1:38D+QFk3JXvMYJuhSaZ+3Nm/Qh+bZJdCrdu4pkIh5BU='
   ```
4. Enter the development shell:
   ```bash
   nix develop
   ```

---

### Method 2: Using Docker

1. Install Docker if needed.
2. Make sure your cloned folder has read/write permissions.
3. Run the container:
   ```bash
   docker run -it --read-only --volume $(pwd):/outside oarteam/batsim-getting-started:ARCH
   ```
   Replace `ARCH` with:
   - `x86_64-linux` for x86_64
   - `aarch64-linux` for AArch64

---

### Method 3: Manual Installation (Advanced)

Install the following manually:
- C++ toolchain
- Meson, Ninja, pkg-config
- Boost, nlohmann_json
- SimGrid (v3.36.0)
- intervalset
- batprotocol-cpp
- batsim (from `batprotocol` branch)

Prepare to resolve missing dependencies as needed.

## 3. Running Simulations

You have two options for running the simulations:

---

### Option 1: Fully Automated Run with `analyze.py` (Recommend)

Instead of building and running manually, you can automate everything:
```bash
./analyze.py
```
This script:
- Builds the scheduler
- Runs the simulation
- Collects and evaluates results automatically

**Recommended for faster testing!**

---

### Option 2: Manual Build and Run (this is to run each file manually)

1. Build the scheduler manually:
   ```bash
   meson setup build
   ninja -C build
   ```

2. Run a basic simulation:
   ```bash
   batsim -l ./build/libreducePC_IDLE.so 0 '' -p assets/1machine.xml -w assets/2jobs.json
   ```

Simulation outputs are stored in the `out/` folder:
- `schedule.csv`: Metrics about the generated schedule.
- `jobs.csv`: Information about each job execution.
