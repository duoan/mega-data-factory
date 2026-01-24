#!/usr/bin/env python
"""
Generate HTML Metrics Report

Standalone script to generate visualization reports from metrics Parquet files.
Can also publish reports to HuggingFace Spaces.

Usage:
    # Generate report locally
    python scripts/generate_metrics_report.py --metrics-path ./metrics --output report.html

    # Generate and publish to HuggingFace
    python scripts/generate_metrics_report.py --metrics-path ./metrics \\
        --huggingface-repo username/space-name \\
        --huggingface-token $HF_TOKEN
"""

import argparse
import os
import sys
from pathlib import Path


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate HTML metrics report from Parquet files")
    parser.add_argument(
        "--metrics-path",
        type=str,
        required=True,
        help="Path to metrics directory containing Parquet files",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        help="Specific run ID to generate report for (if not specified, uses latest run)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output path for HTML report (if not specified, auto-generated as report_run_{run_id}.html)",
    )
    parser.add_argument(
        "--huggingface-repo",
        type=str,
        help='HuggingFace Space repo ID for publishing (e.g., "username/space-name")',
    )
    parser.add_argument(
        "--huggingface-token",
        type=str,
        help="HuggingFace API token (defaults to HF_TOKEN env var)",
    )

    args = parser.parse_args()

    # Check if metrics path exists
    if not Path(args.metrics_path).exists():
        print(f"Error: Metrics path not found: {args.metrics_path}")
        sys.exit(1)

    # Import reporter
    try:
        from mega_data_factory.framework.metrics import MetricsReporter
    except ImportError as e:
        print(f"Error: Failed to import MetricsReporter: {e}")
        print("Make sure mega_data_factory is installed")
        sys.exit(1)

    # Create reporter
    reporter = MetricsReporter(args.metrics_path)

    # Generate single-run HTML report
    run_id_str = f" for run {args.run_id}" if args.run_id else " (latest run)"
    print(f"Generating metrics report from: {args.metrics_path}{run_id_str}")
    try:
        report_path = reporter.generate_single_run_report(
            run_id=args.run_id,  # None = use latest
            output_path=args.output,
        )
        print(f"✓ Report generated: {report_path}")
    except Exception as e:
        print(f"Error: Failed to generate report: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Publish to HuggingFace if requested
    if args.huggingface_repo:
        print(f"\nPublishing to HuggingFace Space: {args.huggingface_repo}")

        # Get token from args or environment
        token = args.huggingface_token or os.environ.get("HF_TOKEN")
        if not token:
            print("Warning: No HuggingFace token provided. Set --huggingface-token or HF_TOKEN env var")
            print("Attempting to use cached credentials...")

        try:
            space_url = reporter.publish_to_huggingface(
                report_path=report_path,
                repo_id=args.huggingface_repo,
                token=token,
            )
            print(f"✓ Report published: {space_url}")
        except Exception as e:
            print(f"Error: Failed to publish to HuggingFace: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
