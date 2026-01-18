"""
Main entry point for Z-Image Data Profiling Pipeline

Usage:
    python main.py --config pipeline_config.yaml
"""

import argparse
import logging
import sys

import loaders  # noqa: F401

# Import operators, loaders, and writers to register them
import operators  # noqa: F401
import writers  # noqa: F401
from framework import Executor, PipelineConfig

# Configure logging for Ray Dashboard visibility
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

# Suppress noisy logs from third-party libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("datasets").setLevel(logging.WARNING)
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("s3fs").setLevel(logging.WARNING)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Configuration-driven data profiling pipeline for Z-Image",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python main.py --config pipeline_config.yaml
        """,
    )
    parser.add_argument(
        "--config",
        type=str,
        default="pipeline_config.yaml",
        help="Path to pipeline configuration YAML file (default: pipeline_config.yaml)",
    )
    args = parser.parse_args()

    try:
        # Load configuration
        print(f"Loading configuration from {args.config}...")
        config = PipelineConfig.from_yaml(args.config)

        # Create executor
        print("Initializing executor...")
        executor = Executor(config)

        # Execute pipeline
        print("Starting pipeline execution...")
        print(f"  - Max samples: {config.executor.max_samples or 'unlimited'}")
        print(f"  - Batch size: {config.executor.batch_size}")
        print(f"  - Stages: {len(config.stages)}")
        print()

        total_input = 0
        total_output = 0

        for input_count, output_count in executor.execute():
            total_input += input_count
            total_output += output_count
            print(f"Input: {total_input}, Output: {total_output} samples (filtered: {total_input - total_output})...")

        print("\n" + "=" * 60)
        print("Completed processing:")
        print(f"  Input samples: {total_input}")
        print(f"  Output samples: {total_output}")
        filtered = total_input - total_output
        filtered_pct = (filtered / total_input * 100) if total_input > 0 else 0.0
        print(f"  Filtered/Deduplicated: {filtered} ({filtered_pct:.1f}%)")
        print("=" * 60)

        # Print operator performance statistics
        print("\n" + "=" * 60)
        print("Operator Performance Statistics:")
        print("=" * 60)

        operator_stats = executor.get_operator_stats()
        if operator_stats:
            for stage_name, stage_ops in operator_stats.items():
                print(f"\n{stage_name}:")

                # Print stage-level summary if available
                if "_stage_summary" in stage_ops:
                    summary = stage_ops["_stage_summary"]
                    print("  [Stage Summary]")
                    print(f"    Records: {summary['total_records']}")
                    print(f"    Total time: {summary['total_time']:.2f}s")
                    print(f"    Throughput: {summary['throughput']:.2f} records/sec")
                    print()

                # Print operator-level statistics
                for op_name, op_stats in stage_ops.items():
                    if op_name == "_stage_summary":
                        continue
                    # Always print statistics, even if total_records is 0
                    print(f"  {op_name}:")
                    print(f"    Records: {op_stats.get('total_records', 0)}")
                    print(f"    Total time: {op_stats.get('total_time', 0.0):.2f}s")
                    print(f"    Avg latency: {op_stats.get('avg_latency', 0.0) * 1000:.2f}ms")
                    print(
                        f"    Min/Max latency: {op_stats.get('min_latency', 0.0) * 1000:.2f}ms / {op_stats.get('max_latency', 0.0) * 1000:.2f}ms"
                    )
                    print(
                        f"    P50/P95/P99: {op_stats.get('p50_latency', 0.0) * 1000:.2f}ms / {op_stats.get('p95_latency', 0.0) * 1000:.2f}ms / {op_stats.get('p99_latency', 0.0) * 1000:.2f}ms"
                    )
                    print(f"    Throughput: {op_stats.get('throughput', 0.0):.2f} records/sec")
        else:
            print("  No statistics available")

        print("=" * 60)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\nError: Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        if "executor" in locals():
            executor.shutdown()


if __name__ == "__main__":
    main()
