import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from pathlib import Path
import re
from datetime import datetime


def extract_timestamp_from_filename(filename):
    """Extract timestamp from filename for sorting and labeling."""
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        timestamp_str = match.group(1)
        # Format as readable: YYYYMMDD_HHMMSS -> YYYY-MM-DD HH:MM:SS
        return f"{timestamp_str[:4]}-{timestamp_str[4:6]}-{timestamp_str[6:8]} {timestamp_str[9:11]}:{timestamp_str[11:13]}:{timestamp_str[13:15]}"
    return filename


def load_memory_files(directory="output/performance", sampling_interval=0.1):
    """Load all pickle files from the performance directory."""
    perf_dir = Path(directory)
    
    if not perf_dir.exists():
        print(f"Directory {directory} does not exist!")
        return []
    
    # Find all .pkl files
    pkl_files = sorted(perf_dir.glob("*.pkl"))
    
    if not pkl_files:
        print(f"No .pkl files found in {directory}")
        return []
    
    datasets = []
    for pkl_file in pkl_files:
        try:
            with open(pkl_file, "rb") as f:
                data = pickle.load(f)
            
            # Handle both old format (list) and new format (dict)
            if isinstance(data, dict):
                memory_data = data.get('memory_over_time', [])
                file_sizes = data.get('file_sizes', {})
            else:
                # Old format: just the memory list
                memory_data = data
                file_sizes = {}
                
            # Create time axis
            time_axis = [i * sampling_interval for i in range(len(memory_data))]
            
            # Extract label from filename
            label = extract_timestamp_from_filename(pkl_file.name)
            
            datasets.append({
                'time': time_axis,
                'memory': memory_data,
                'label': label,
                'filename': pkl_file.name,
                'peak': max(memory_data) if memory_data else 0,
                'avg': sum(memory_data) / len(memory_data) if memory_data else 0,
                'duration': time_axis[-1] if time_axis else 0,
                'file_sizes': file_sizes
            })
            
        except Exception as e:
            print(f"Error loading {pkl_file.name}: {e}")
    
    return datasets


def plot_memory_usage(datasets, output_file=None):
    """Plot all memory usage datasets and save to file.
    
    Args:
        datasets: List of memory data dictionaries
        output_file: Path to save the plot (defaults to output/performance/memory_comparison_TIMESTAMP.png)
    """
    if not datasets:
        print("No data to plot!")
        return
    
    # Generate default output filename if not provided
    if output_file is None:
        output_dir = Path("output/performance")
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"memory_comparison_{timestamp}.png"
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot 1: All runs overlaid
    for i, data in enumerate(datasets):
        ax1.plot(data['time'], data['memory'], label=data['label'], alpha=0.7, linewidth=1.5)
    
    ax1.set_xlabel("Time (s)", fontsize=12)
    ax1.set_ylabel("Memory Usage (MB)", fontsize=12)
    ax1.set_title("Memory Usage Over Time - All Runs", fontsize=14, fontweight='bold')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Summary statistics
    labels = [d['filename'] for d in datasets]
    peaks = [d['peak'] for d in datasets]
    avgs = [d['avg'] for d in datasets]
    
    x = range(len(datasets))
    width = 0.35
    
    ax2.bar([i - width/2 for i in x], peaks, width, label='Peak Memory', alpha=0.8)
    ax2.bar([i + width/2 for i in x], avgs, width, label='Average Memory', alpha=0.8)
    
    ax2.set_xlabel("Run", fontsize=12)
    ax2.set_ylabel("Memory (MB)", fontsize=12)
    ax2.set_title("Memory Statistics Comparison", fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save the figure
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    plt.close()
    
    # Print summary statistics
    print("\n" + "="*80)
    print("MEMORY USAGE SUMMARY")
    print("="*80)
    for data in datasets:
        print(f"\n{data['filename']}:")
        print(f"  Duration: {data['duration']:.2f}s")
        print(f"  Peak Memory: {data['peak']:.2f} MB")
        print(f"  Average Memory: {data['avg']:.2f} MB")
        
        # Print file sizes if available
        if data['file_sizes']:
            print("  Generated files:")
            for filename, size in data['file_sizes'].items():
                print(f"    - {filename}: {size:.2f} MB")
    print("="*80 + "\n")


def main():
    """Main execution function."""
    # Load all memory data files
    datasets = load_memory_files(directory="output/performance", sampling_interval=0.1)
    
    # Plot the data
    plot_memory_usage(datasets)


if __name__ == "__main__":
    main()