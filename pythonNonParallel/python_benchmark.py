import os
import sys
import time
import subprocess
import glob
from datetime import datetime
import json


def get_dataset_info(dataset_path):
    if not os.path.exists(dataset_path):
        return 0, 0
    
    txt_files = glob.glob(os.path.join(dataset_path, "*.txt"))
    total_size = sum(os.path.getsize(f) for f in txt_files if os.path.exists(f))
    return total_size / (1024 * 1024), len(txt_files)  # Size in MB


def run_python_implementation(dataset_path, output_file):
    
    print(f"Running Python implementation...")
    print(f"Dataset: {dataset_path}")
    print(f"Output: {output_file}")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    cmd = [sys.executable, "inverted_index_nonparallel.py", dataset_path, output_file]
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        
        execution_time = time.time() - start_time
        
        print(f"Exit code: {result.returncode}")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        performance_info = extract_performance_info(result.stdout, output_file)
        
        return {
            'success': result.returncode == 0,
            'execution_time': execution_time,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'exit_code': result.returncode,
            'output_file': output_file,
            **performance_info
        }
        
    except Exception as e:
        print(f"Error running command: {e}")
        return {
            'success': False,
            'execution_time': 0,
            'error': str(e),
            'exit_code': -1
        }


def extract_performance_info(stdout, output_file):
    info = {
        'files_processed': None,
        'unique_terms': None,
        'peak_memory_mb': None
    }
    
    lines = stdout.split('\n') if stdout else []
    
    for line in lines:
        line = line.strip()
        if 'Processed' in line and 'files' in line:
            try:
                # Look for pattern like "Processed 123 files"
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == 'Processed' and i + 1 < len(parts):
                        info['files_processed'] = int(parts[i + 1])
                        break
            except (ValueError, IndexError):
                pass
        
        elif 'unique terms' in line.lower():
            try:
                parts = line.split()
                for part in parts:
                    if part.isdigit() and int(part) > 100:  # Reasonable number of terms
                        info['unique_terms'] = int(part)
                        break
            except (ValueError, IndexError):
                pass
        
        elif 'Peak memory usage:' in line:
            try:
                # Look for pattern like "Peak memory usage: 156.23 MB"
                parts = line.split(':')[1].strip().split()
                if parts:
                    info['peak_memory_mb'] = float(parts[0])
            except (ValueError, IndexError):
                pass
    
    report_file = f"{os.path.splitext(output_file)[0]}_report.txt"
    if os.path.exists(report_file):
        try:
            with open(report_file, 'r') as f:
                content = f.read()
                for line in content.split('\n'):
                    if 'Peak memory:' in line:
                        try:
                            info['peak_memory_mb'] = float(line.split(':')[1].strip().split()[0])
                        except (ValueError, IndexError):
                            pass
        except Exception as e:
            print(f"Warning: Could not read report file: {e}")
    
    return info


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m {seconds%60:.0f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours:.0f}h {minutes:.0f}m"


def format_memory(mb):
    if mb is None:
        return "N/A"
    elif mb < 1024:
        return f"{mb:.1f} MB"
    else:
        return f"{mb/1024:.2f} GB"


def main():
    datasets_dir = "./datasets"
    output_dir = "./benchmark_results"
    
    print("Python-only Inverted Index Benchmark")
    print("=" * 50)
    
    if not os.path.exists("inverted_index_nonparallel.py"):
        print("ERROR: inverted_index_nonparallel.py not found!")
        print("Make sure the script is in the current directory.")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    if not os.path.exists(datasets_dir):
        print(f"ERROR: Datasets directory not found: {datasets_dir}")
        return
    
    datasets = []
    for item in os.listdir(datasets_dir):
        item_path = os.path.join(datasets_dir, item)
        if os.path.isdir(item_path):
            size_mb, num_files = get_dataset_info(item_path)
            if num_files > 0:
                datasets.append({
                    'name': item,
                    'path': item_path,
                    'size_mb': size_mb,
                    'num_files': num_files
                })
    
    if not datasets:
        print(f"ERROR: No datasets found in {datasets_dir}")
        print("Make sure you have subdirectories with .txt files")
        return
    
    datasets.sort(key=lambda x: x['size_mb'])
    
    print(f"Found {len(datasets)} datasets:")
    for ds in datasets:
        print(f"  {ds['name']}: {ds['size_mb']:.1f} MB ({ds['num_files']} files)")
    
    # Run benchmarks
    results = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for i, dataset in enumerate(datasets, 1):
        print(f"\n[{i}/{len(datasets)}] Processing: {dataset['name']}")
        print("-" * 40)
        
        output_file = os.path.join(output_dir, f"{timestamp}_{dataset['name']}_result.txt")
        
        result = run_python_implementation(dataset['path'], output_file)
        result.update({
            'dataset_name': dataset['name'],
            'dataset_size_mb': dataset['size_mb'],
            'dataset_num_files': dataset['num_files']
        })
        
        results.append(result)
        
        if result['success']:
            print(f"✓ SUCCESS: Completed in {format_time(result['execution_time'])}")
            if result.get('peak_memory_mb'):
                print(f"  Memory: {format_memory(result['peak_memory_mb'])}")
            if result.get('unique_terms'):
                print(f"  Terms: {result['unique_terms']:,}")
        else:
            print(f"✗ FAILED: Exit code {result['exit_code']}")
            if result.get('stderr'):
                print(f"  Error: {result['stderr'][:200]}...")
    
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    if successful:
        print(f"\n{'Dataset':<15} {'Size':<12} {'Time':<12} {'Memory':<12} {'Terms':<12} {'Status'}")
        print("-" * 75)
        
        for result in results:
            status = "SUCCESS" if result['success'] else "FAILED"
            time_str = format_time(result['execution_time']) if result['success'] else "N/A"
            memory_str = format_memory(result.get('peak_memory_mb')) if result['success'] else "N/A"
            terms_str = f"{result.get('unique_terms', 'N/A'):,}" if result.get('unique_terms') else "N/A"
            size_str = f"{result['dataset_size_mb']:.1f} MB"
            
            print(f"{result['dataset_name']:<15} {size_str:<12} {time_str:<12} {memory_str:<12} {terms_str:<12} {status}")
    
    results_file = os.path.join(output_dir, f"benchmark_summary_{timestamp}.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nDetailed results saved to: {results_file}")
    
    print(f"\nSummary:")
    print(f"  Successful runs: {len(successful)}/{len(results)}")
    print(f"  Failed runs: {len(failed)}")
    
    if failed:
        print(f"\nFailed datasets:")
        for result in failed:
            print(f"  {result['dataset_name']}: {result.get('stderr', 'Unknown error')[:100]}...")


if __name__ == "__main__":
    main()