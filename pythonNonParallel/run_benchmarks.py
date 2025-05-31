#!/usr/bin/env python3
"""
Benchmarking script for comparing Hadoop, Spark, and non-parallel Python inverted index implementations.
"""

import os
import sys
import time
import json
import argparse
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime


# Configuration
HADOOP_SCRIPT = "./build_index.sh"
SPARK_SCRIPT = "./build_index_spark.sh"
PYTHON_SCRIPT = "./inverted_index_nonparallel.py"
OUTPUT_DIR = "./benchmark_results"
DATASETS_DIR = "./datasets"


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")


def run_command(cmd, description=None):
    """Run a command and capture output, errors, and execution time."""
    print(f"Running: {' '.join(cmd)}")
    if description:
        print(f"Description: {description}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        exit_code = 0
    except subprocess.CalledProcessError as e:
        result = e
        exit_code = e.returncode
        print(f"Command failed with exit code {exit_code}")
    
    elapsed = time.time() - start_time
    
    return {
        'command': ' '.join(cmd),
        'exit_code': exit_code,
        'stdout': result.stdout,
        'stderr': result.stderr,
        'execution_time': elapsed
    }


def get_memory_usage_from_log(log_file):
    """Extract peak memory usage from log file."""
    if not os.path.exists(log_file):
        return None
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if "Peak memory usage" in line:
                    return float(line.split(':')[1].strip().split()[0])
    except Exception as e:
        print(f"Error reading log file {log_file}: {e}")
    
    return None


def run_hadoop_benchmark(input_files, output_file, reducers=2, variant='combiner'):
    """Run Hadoop benchmark."""
    cmd = [HADOOP_SCRIPT, *input_files, output_file, str(reducers), variant]
    description = f"Hadoop benchmark with {reducers} reducers, variant: {variant}"
    
    result = run_command(cmd, description)
    
    # Extract memory usage from Hadoop logs
    try:
        hadoop_log = os.environ.get('HADOOP_LOG_DIR') or '/var/log/hadoop'
        latest_log = sorted(
            [f for f in os.listdir(hadoop_log) if f.endswith('.log')],
            key=lambda x: os.path.getmtime(os.path.join(hadoop_log, x))
        )[-1]
        with open(os.path.join(hadoop_log, latest_log), 'r') as f:
            log_data = f.read()
            memory_lines = [line for line in log_data.splitlines() 
                           if 'Physical memory' in line]
            if memory_lines:
                mem_line = memory_lines[-1]
                memory_mb = float(mem_line.split('(')[1].split()[0]) / 1024
                result['memory_mb'] = memory_mb
    except Exception as e:
        print(f"Failed to extract memory usage for Hadoop: {e}")
        result['memory_mb'] = None
    
    return result


def run_spark_benchmark(input_files, output_file, partitions=2):
    """Run Spark benchmark."""
    cmd = [SPARK_SCRIPT, *input_files, output_file, str(partitions)]
    description = f"Spark benchmark with {partitions} partitions"
    
    result = run_command(cmd, description)
    
    # Extract memory usage from Spark event logs
    try:
        spark_log_dir = os.environ.get('SPARK_EVENT_LOG_DIR') or '/tmp/spark-events'
        latest_log = sorted(
            [f for f in os.listdir(spark_log_dir) if f.startswith('app-')],
            key=lambda x: os.path.getmtime(os.path.join(spark_log_dir, x))
        )[-1]
        with open(os.path.join(spark_log_dir, latest_log), 'r') as f:
            for line in f:
                if '"Event":"SparkExecutorMetrics"' in line:
                    data = json.loads(line)
                    if 'peakMemoryUsed' in data['peakValues']:
                        result['memory_mb'] = data['peakValues']['peakMemoryUsed'] / (1024 * 1024)
                        break
    except Exception as e:
        print(f"Failed to extract memory usage for Spark: {e}")
        result['memory_mb'] = None
    
    return result


def run_python_benchmark(input_dir, output_file):
    """Run non-parallel Python benchmark."""
    cmd = [sys.executable, PYTHON_SCRIPT, input_dir, output_file]
    description = f"Non-parallel Python benchmark"
    
    result = run_command(cmd, description)
    
    # Extract memory usage from Python log report
    report_file = f"{os.path.splitext(output_file)[0]}_report.txt"
    memory_mb = get_memory_usage_from_log(report_file)
    result['memory_mb'] = memory_mb
    
    return result


def find_dataset_folders(base_dir):
    """Find dataset folders organized by size."""
    datasets = []
    
    for root, dirs, files in os.walk(base_dir):
        if root == base_dir:
            continue
        
        txt_files = [f for f in files if f.endswith('.txt')]
        if txt_files:
            size_bytes = sum(os.path.getsize(os.path.join(root, f)) for f in txt_files)
            size_mb = size_bytes / (1024 * 1024)
            
            datasets.append({
                'path': root,
                'name': os.path.basename(root),
                'size_mb': size_mb,
                'num_files': len(txt_files)
            })
    
    return sorted(datasets, key=lambda x: x['size_mb'])


def run_benchmarks(datasets, reducers_list, include_hadoop=True, include_spark=True, include_python=True):
    """Run benchmarks for all implementations on all datasets."""
    results = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for dataset in datasets:
        dataset_name = dataset['name']
        dataset_path = dataset['path']
        dataset_size = dataset['size_mb']
        
        print(f"\n{'='*60}")
        print(f"Benchmarking dataset: {dataset_name} ({dataset_size:.2f} MB)")
        print(f"{'='*60}")
        
        for reducers in reducers_list:
            # Hadoop benchmarks (with different variants)
            if include_hadoop:
                for variant in ['standard', 'combiner', 'imc']:
                    output_file = os.path.join(
                        OUTPUT_DIR, 
                        f"{timestamp}_{dataset_name}_hadoop_{variant}_{reducers}.txt"
                    )
                    
                    print(f"\nRunning Hadoop benchmark ({variant}, {reducers} reducers)...")
                    result = run_hadoop_benchmark(
                        [dataset_path], 
                        output_file, 
                        reducers=reducers, 
                        variant=variant
                    )
                    
                    results.append({
                        'dataset': dataset_name,
                        'size_mb': dataset_size,
                        'implementation': f'hadoop_{variant}',
                        'reducers': reducers,
                        'execution_time': result['execution_time'],
                        'memory_mb': result.get('memory_mb'),
                        'exit_code': result['exit_code'],
                        'output_file': output_file
                    })
            
            # Spark benchmarks
            if include_spark:
                output_file = os.path.join(
                    OUTPUT_DIR, 
                    f"{timestamp}_{dataset_name}_spark_{reducers}.txt"
                )
                
                print(f"\nRunning Spark benchmark ({reducers} partitions)...")
                result = run_spark_benchmark(
                    [dataset_path], 
                    output_file, 
                    partitions=reducers
                )
                
                results.append({
                    'dataset': dataset_name,
                    'size_mb': dataset_size,
                    'implementation': 'spark',
                    'reducers': reducers,
                    'execution_time': result['execution_time'],
                    'memory_mb': result.get('memory_mb'),
                    'exit_code': result['exit_code'],
                    'output_file': output_file
                })
        
        # Non-parallel Python benchmark (once per dataset, no reducers)
        if include_python:
            output_file = os.path.join(
                OUTPUT_DIR, 
                f"{timestamp}_{dataset_name}_python.txt"
            )
            
            print(f"\nRunning non-parallel Python benchmark...")
            result = run_python_benchmark(dataset_path, output_file)
            
            results.append({
                'dataset': dataset_name,
                'size_mb': dataset_size,
                'implementation': 'python',
                'reducers': None,
                'execution_time': result['execution_time'],
                'memory_mb': result.get('memory_mb'),
                'exit_code': result['exit_code'],
                'output_file': output_file
            })
    
    return results


def save_results(results, output_dir):
    """Save benchmark results to CSV and generate charts."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save raw results
    df = pd.DataFrame(results)
    csv_path = os.path.join(output_dir, f"benchmark_results_{timestamp}.csv")
    df.to_csv(csv_path, index=False)
    print(f"\nResults saved to {csv_path}")
    
    # Generate execution time chart
    df_pivot = pd.pivot_table(
        df, 
        values='execution_time', 
        index=['dataset', 'size_mb'], 
        columns=['implementation', 'reducers'],
        aggfunc='mean'
    ).reset_index()
    
    plt.figure(figsize=(14, 8))
    
    for impl, group in df.groupby('implementation'):
        if impl == 'python':
            plt.plot(
                group['size_mb'], 
                group['execution_time'], 
                'o-', 
                label=f"{impl}", 
                linewidth=2
            )
        else:
            # Find best reducer setting for each size
            best_times = group.groupby('size_mb')['execution_time'].min()
            best_df = pd.DataFrame({'size_mb': best_times.index, 'execution_time': best_times.values})
            plt.plot(
                best_df['size_mb'], 
                best_df['execution_time'], 
                'o-', 
                label=f"{impl} (best)", 
                linewidth=2
            )
    
    plt.xlabel('Dataset Size (MB)', fontsize=12)
    plt.ylabel('Execution Time (seconds)', fontsize=12)
    plt.title('Inverted Index Execution Time Comparison', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    chart_path = os.path.join(output_dir, f"execution_time_chart_{timestamp}.png")
    plt.savefig(chart_path, dpi=150)
    print(f"Execution time chart saved to {chart_path}")
    
    # Generate memory usage chart
    plt.figure(figsize=(14, 8))
    
    for impl, group in df.groupby('implementation'):
        valid_mem = group[group['memory_mb'].notna()]
        if not valid_mem.empty:
            if impl == 'python':
                plt.plot(
                    valid_mem['size_mb'], 
                    valid_mem['memory_mb'], 
                    'o-', 
                    label=f"{impl}", 
                    linewidth=2
                )
            else:
                # Find peak memory for each size
                peak_mem = valid_mem.groupby('size_mb')['memory_mb'].max()
                peak_df = pd.DataFrame({'size_mb': peak_mem.index, 'memory_mb': peak_mem.values})
                plt.plot(
                    peak_df['size_mb'], 
                    peak_df['memory_mb'], 
                    'o-', 
                    label=f"{impl} (peak)", 
                    linewidth=2
                )
    
    plt.xlabel('Dataset Size (MB)', fontsize=12)
    plt.ylabel('Memory Usage (MB)', fontsize=12)
    plt.title('Inverted Index Memory Usage Comparison', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    memory_chart_path = os.path.join(output_dir, f"memory_usage_chart_{timestamp}.png")
    plt.savefig(memory_chart_path, dpi=150)
    print(f"Memory usage chart saved to {memory_chart_path}")


def main():
    parser = argparse.ArgumentParser(description='Run inverted index benchmarks')
    parser.add_argument('--datasets-dir', default=DATASETS_DIR, help='Datasets directory')
    parser.add_argument('--output-dir', default=OUTPUT_DIR, help='Output directory for results')
    parser.add_argument('--reducers', default='1,2,4,8', help='Comma-separated list of reducer counts')
    parser.add_argument('--no-hadoop', action='store_true', help='Skip Hadoop benchmarks')
    parser.add_argument('--no-spark', action='store_true', help='Skip Spark benchmarks')
    parser.add_argument('--no-python', action='store_true', help='Skip Python benchmarks')
    args = parser.parse_args()
    
    # Ensure output directory exists
    ensure_dir(args.output_dir)
    
    # Find dataset folders
    datasets = find_dataset_folders(args.datasets_dir)
    if not datasets:
        print(f"No dataset folders found in {args.datasets_dir}")
        sys.exit(1)
    
    print(f"Found {len(datasets)} datasets:")
    for ds in datasets:
        print(f"  {ds['name']}: {ds['size_mb']:.2f} MB ({ds['num_files']} files)")
    
    # Parse reducer counts
    reducers_list = [int(r) for r in args.reducers.split(',')]
    
    # Run benchmarks
    results = run_benchmarks(
        datasets,
        reducers_list,
        include_hadoop=not args.no_hadoop,
        include_spark=not args.no_spark,
        include_python=not args.no_python
    )
    
    # Save results
    save_results(results, args.output_dir)
    
    print("\nBenchmarks completed successfully!")


if __name__ == "__main__":
    main()
