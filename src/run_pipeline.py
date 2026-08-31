import subprocess
import sys

STAGES = [
    "src/ingest_ocm.py",
    "src/ingest_weather.py",
    "src/build_silver.py",
    "src/build_gold.py",
]

def main():
    for stage in STAGES:
        print(f"\n=== Running {stage} ===")
        result = subprocess.run([sys.executable, stage])
        if result.returncode != 0:
            print(f"FAILED at {stage} — pipeline stopped")
            sys.exit(1)
    print("\nPipeline complete.")

if __name__ == "__main__":
    main()
