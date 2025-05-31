import os
import sys
import time
import glob
import logging
import psutil
import argparse
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import gc
import tracemalloc
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nonparallel_invindex.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('nonparallel-invindex')


class PerformanceMonitor:

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.process = psutil.Process(os.getpid())
        self.start_memory = None
        self.max_memory = 0
        self.checkpoints = []
        tracemalloc.start()

    def start(self):
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / (1024 * 1024)
        self.max_memory = self.start_memory
        logging.info(f"Starting performance monitoring. Initial memory: {self.start_memory:.2f} MB")

    def checkpoint(self, label: str):
        current_time = time.time()
        current_memory = self.process.memory_info().rss / (1024 * 1024)
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        
        self.max_memory = max(self.max_memory, current_memory)
        elapsed = current_time - self.start_time
        
        self.checkpoints.append({
            'label': label,
            'time_elapsed': elapsed,
            'memory_usage_mb': current_memory,
            'top_memory_allocations': top_stats[:5]
        })
        
        logging.info(f"CHECKPOINT [{label}] - Time: {elapsed:.3f}s, Memory: {current_memory:.2f} MB")
        logging.debug(f"Top 5 memory allocations for [{label}]:")
        for stat in top_stats[:5]:
            logging.debug(f"  {stat}")

    def end(self):
        self.end_time = time.time()
        final_memory = self.process.memory_info().rss / (1024 * 1024)
        total_time = self.end_time - self.start_time
        
        logging.info("=" * 60)
        logging.info(f"PERFORMANCE SUMMARY:")
        logging.info(f"Total execution time: {total_time:.3f} seconds")
        logging.info(f"Initial memory: {self.start_memory:.2f} MB")
        logging.info(f"Final memory: {final_memory:.2f} MB")
        logging.info(f"Peak memory usage: {self.max_memory:.2f} MB")
        logging.info(f"Memory increase: {final_memory - self.start_memory:.2f} MB")
        logging.info("=" * 60)
        
        tracemalloc.stop()
        
        return {
            'total_time': total_time,
            'initial_memory_mb': self.start_memory,
            'final_memory_mb': final_memory,
            'peak_memory_mb': self.max_memory,
            'checkpoints': self.checkpoints
        }

    def generate_report(self, output_file: str = 'performance_report.txt'):
        with open(output_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write(f"INVERTED INDEX - NON-PARALLEL PYTHON IMPLEMENTATION\n")
            f.write(f"Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"EXECUTION SUMMARY:\n")
            f.write(f"  Total execution time: {self.end_time - self.start_time:.3f} seconds\n")
            f.write(f"  Initial memory: {self.start_memory:.2f} MB\n")
            f.write(f"  Peak memory: {self.max_memory:.2f} MB\n")
            f.write(f"  Final memory: {self.process.memory_info().rss / (1024 * 1024):.2f} MB\n\n")
            
            f.write(f"CHECKPOINT DETAILS:\n")
            for i, cp in enumerate(self.checkpoints, 1):
                f.write(f"  [{i}] {cp['label']}:\n")
                f.write(f"      Time elapsed: {cp['time_elapsed']:.3f} seconds\n")
                f.write(f"      Memory usage: {cp['memory_usage_mb']:.2f} MB\n")
            
            f.write("\n" + "=" * 80 + "\n")
        
        logging.info(f"Performance report written to {output_file}")


def normalize_text(text: str) -> List[str]:
    text = text.lower()
    # Replace non-alphanumeric with space
    chars = []
    for ch in text:
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append(' ')
    text = ''.join(chars)
    return [token for token in text.split() if token]


def load_stop_words(file_path: str = None) -> Set[str]:
    stop_words = {
        'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 'as', 'at',
        'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'cannot', 'could',
        'couldn', 'd', 'did', 'didn', 'do', 'does', 'doesn', 'doing', 'don', 'down', 'during', 'each', 'few', 'for',
        'from', 'further', 'had', 'hadn', 'has', 'hasn', 'have', 'haven', 'having', 'he', 'her', 'here', 'hers',
        'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'it', 'its', 'itself', 'just',
        'll', 'm', 'ma', 'me', 'more', 'most', 'my', 'myself', 'no', 'nor', 'not', 'now', 'o', 'of', 'off', 'on',
        'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 're', 's', 'same', 'she',
        'should', 'shouldn', 'so', 'some', 'such', 't', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves',
        'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 've', 'very',
        'was', 'wasn', 'we', 'were', 'weren', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will',
        'with', 'won', 'wouldn', 'y', 'you', 'your', 'yours', 'yourself', 'yourselves'
    }
    
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                stop_words = {line.strip().lower() for line in f if line.strip()}
            logging.info(f"Loaded {len(stop_words)} stop words from {file_path}")
        except Exception as e:
            logging.warning(f"Failed to load stop words from {file_path}: {e}")
            logging.warning(f"Using default stop words list")
    else:
        logging.info(f"Using default stop words list ({len(stop_words)} words)")
    
    return stop_words


def read_file(file_path: str) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {e}")
        return ""


def process_document(file_path: str, stop_words: Set[str]) -> Dict[str, int]:
    filename = Path(file_path).name
    content = read_file(file_path)
    
    if not content:
        logging.warning(f"Empty content in {filename}")
        return {}
    
    tokens = normalize_text(content)
    term_counts = defaultdict(int)
    
    for token in tokens:
        if token and token not in stop_words:
            term_counts[token] += 1
    
    logging.debug(f"Processed {filename}: found {len(tokens)} tokens, {len(term_counts)} unique terms")
    return dict(term_counts)


def build_inverted_index(input_dir: str, stop_words: Set[str]) -> Dict[str, Dict[str, int]]:
    inverted_index = defaultdict(dict)
    file_count = 0
    empty_files = 0
    term_count = 0
    
    # Expand pattern for all text files
    pattern = os.path.join(input_dir, '**/*.txt')
    all_files = glob.glob(pattern, recursive=True)
    
    logging.info(f"Found {len(all_files)} files to process in {input_dir}")
    
    for file_path in all_files:
        file_name = Path(file_path).name
        term_frequencies = process_document(file_path, stop_words)
        
        if not term_frequencies:
            empty_files += 1
            continue
        
        for term, count in term_frequencies.items():
            inverted_index[term][file_name] = count
        
        file_count += 1
        term_count += len(term_frequencies)
        
        if file_count % 100 == 0:
            logging.info(f"Processed {file_count}/{len(all_files)} files...")
    
    logging.info(f"Completed processing {file_count} files "
                f"({empty_files} empty or failed)")
    logging.info(f"Built inverted index with {len(inverted_index)} unique terms "
                f"and {term_count} total term occurrences")
    
    return dict(inverted_index)


def format_output(inverted_index: Dict[str, Dict[str, int]]) -> List[str]:
    output_lines = []
    
    for term, postings in sorted(inverted_index.items()):
        posting_strings = [f"{doc}:{count}" for doc, count in sorted(postings.items())]
        output_lines.append(f"{term}\t{' '.join(posting_strings)}")
    
    return output_lines


def write_output(inverted_index: Dict[str, Dict[str, int]], output_file: str):
    output_lines = format_output(inverted_index)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
        logging.info(f"Wrote {len(output_lines)} lines to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write to {output_file}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Non-parallel inverted index builder')
    parser.add_argument('input_dir', help='Input directory containing documents')
    parser.add_argument('output_file', help='Output file for inverted index')
    parser.add_argument('--stop-words', help='Path to stop words file (optional)')
    args = parser.parse_args()
    
    if not os.path.isdir(args.input_dir):
        logging.error(f"Input directory doesn't exist: {args.input_dir}")
        sys.exit(1)
    
    output_dir = os.path.dirname(args.output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")
    
    monitor = PerformanceMonitor()
    monitor.start()
    
    monitor.checkpoint("Init")
    stop_words = load_stop_words(args.stop_words)
    monitor.checkpoint("Load stop words")
    
    inverted_index = build_inverted_index(args.input_dir, stop_words)
    monitor.checkpoint("Build index")
    
    write_output(inverted_index, args.output_file)
    monitor.checkpoint("Write output")
    
    gc.collect()
    monitor.checkpoint("Final cleanup")
    stats = monitor.end()
    
    monitor.generate_report(f"{os.path.splitext(args.output_file)[0]}_report.txt")
    
    print(f"\nProcessed {len(glob.glob(os.path.join(args.input_dir, '**/*.txt'), recursive=True))} files")
    print(f"Found {len(inverted_index)} unique terms")
    print(f"Inverted index saved to {args.output_file}")
    print(f"Total execution time: {stats['total_time']:.3f} seconds")
    print(f"Peak memory usage: {stats['peak_memory_mb']:.2f} MB")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        logging.error(traceback.format_exc())
        sys.exit(2)
