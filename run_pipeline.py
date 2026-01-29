"""
Run DVC pipeline based on data_mode in params.yaml
"""
import yaml
import subprocess
import sys

with open('params.yaml', 'r') as f:
    params = yaml.safe_load(f)

data_mode = params['data_mode']
print(f"Running pipeline in {data_mode.upper()} mode...")

stages = [
    f"download_data_{data_mode}",
    f"combine_data_{data_mode}",
    f"summarise_data_{data_mode}"
]
result = subprocess.run(['dvc', 'repro'] + stages)

if result.returncode == 0:
    print(f"\nPipeline complete in {data_mode.upper()} mode!")
    print(f"\nPipeline DAG for {data_mode.upper()} mode:")
    subprocess.run(['dvc', 'dag', f"summarise_data_{data_mode}"])
else:
    print(f"\n Pipeline failed!")
    sys.exit(1)