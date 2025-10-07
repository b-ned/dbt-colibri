import threading
import time
import psutil
import pickle
from pathlib import Path
from datetime import datetime
from dbt_colibri.report.generator import DbtColibriReportGenerator
from dbt_colibri.lineage_extractor.extractor import DbtColumnLineageExtractor


class MemoryMonitor:
    """Monitor memory usage of the current process."""
    
    def __init__(self, sampling_interval=0.1):
        self.memory_over_time = []
        self.sampling_interval = sampling_interval
        self.process = psutil.Process()
        self.monitoring = [True]
        self.monitor_thread = None
    
    def _monitor_memory(self):
        """Internal method to collect memory samples."""
        while self.monitoring[0]:
            mem = self.process.memory_info().rss / 1024 / 1024  # MB
            self.memory_over_time.append(mem)
            time.sleep(self.sampling_interval)
    
    def start(self):
        """Start memory monitoring in a separate thread."""
        self.monitoring[0] = True
        self.monitor_thread = threading.Thread(target=self._monitor_memory)
        self.monitor_thread.start()
    
    def stop(self):
        """Stop memory monitoring and wait for thread to finish."""
        self.monitoring[0] = False
        if self.monitor_thread:
            self.monitor_thread.join()
    
    def save(self, output_path, file_sizes=None):
        """Save memory data and file sizes to a pickle file."""
        data = {
            'memory_over_time': self.memory_over_time,
            'file_sizes': file_sizes or {}
        }
        with open(output_path, "wb") as f:
            pickle.dump(data, f)
        
        # Calculate statistics
        if self.memory_over_time:
            peak_mem = max(self.memory_over_time)
            avg_mem = sum(self.memory_over_time) / len(self.memory_over_time)
            print(f"Memory data saved to {output_path}")
            print(f"Peak memory usage: {peak_mem:.2f} MB")
            print(f"Average memory usage: {avg_mem:.2f} MB")
            print(f"Samples collected: {len(self.memory_over_time)}")
            
            # Print file sizes if available
            if file_sizes:
                print("\nGenerated file sizes:")
                for filename, size in file_sizes.items():
                    print(f"  {filename}: {size:.2f} MB")


def main(input_folder="dev_data"):
    """Main execution function.
    
    Args:
        input_folder: Path to folder containing manifest.json and catalog.json
    """
    # Create output directory
    output_dir = Path("output/performance")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamp for the output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"memory_data_{timestamp}.pkl"
    
    # Construct paths to manifest and catalog
    input_path = Path(input_folder)
    manifest_path = input_path / "manifest.json"
    catalog_path = input_path / "catalog.json"
    
    # Validate that files exist
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {input_folder}")
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found in {input_folder}")
    
    print(f"Using input folder: {input_folder}")
    print(f"  - manifest: {manifest_path}")
    print(f"  - catalog: {catalog_path}")
    
    # Initialize components
    extractor = DbtColumnLineageExtractor(
        manifest_path=str(manifest_path),
        catalog_path=str(catalog_path),
    )
    report_generator = DbtColibriReportGenerator(extractor)
    
    # Start memory monitoring
    monitor = MemoryMonitor(sampling_interval=0.1)
    monitor.start()
    
    file_sizes = {}
    try:
        # Run the heavy function
        print("Starting report generation...")
        start_time = time.time()
        report_generator.generate_report(output_dir="dev_output")
        elapsed_time = time.time() - start_time
        print(f"Report generation completed in {elapsed_time:.2f} seconds")
        
        # Get file sizes of generated files
        colibri_manifest = Path("dev_output/colibri-manifest.json")
        index_html = Path("dev_output/index.html")
        
        if colibri_manifest.exists():
            file_sizes['colibri-manifest.json'] = colibri_manifest.stat().st_size / 1024 / 1024  # MB
        if index_html.exists():
            file_sizes['index.html'] = index_html.stat().st_size / 1024 / 1024  # MB
            
    finally:
        # Ensure monitoring stops even if an error occurs
        monitor.stop()
        monitor.save(output_file, file_sizes=file_sizes)


if __name__ == "__main__":
    import sys
    
    # Allow input folder to be passed as command line argument
    input_folder = sys.argv[1] if len(sys.argv) > 1 else "dev_data"
    main(input_folder)