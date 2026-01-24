"""
Metrics Reporter: Generate comprehensive single-run analysis reports

From a senior data analyst perspective, each run report should include:
- Data quality metrics (pass rates, data loss funnel)
- Performance bottleneck analysis (timeline, latency distribution)
- Resource utilization (worker efficiency, parallelism)
- Data flow visualization (Sankey, funnel charts)
- Anomaly detection (outliers, performance issues)
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


class MetricsReporter:
    """Generate comprehensive single-run HTML reports with advanced visualizations."""

    def __init__(self, metrics_path: str):
        """Initialize reporter.

        Args:
            metrics_path: Path to metrics directory containing Parquet files
        """
        self.metrics_path = Path(metrics_path)
        self.runs_path = self.metrics_path / "runs"
        self.stages_path = self.metrics_path / "stages"
        self.operators_path = self.metrics_path / "operators"

    def load_single_run_metrics(self, run_id: str) -> tuple[pd.Series | None, pd.DataFrame | None, pd.DataFrame | None]:
        """Load metrics for a specific run.

        Args:
            run_id: Run ID to load

        Returns:
            Tuple of (run_series, stage_df, operator_df)
        """
        # Load run metrics
        run_series = None
        run_file = self.runs_path / f"run_{run_id}.parquet"
        if run_file.exists():
            run_df = pd.read_parquet(run_file)
            run_series = run_df.iloc[0] if len(run_df) > 0 else None

        # Load stage metrics for this run
        stage_df = None
        stage_files = list(self.stages_path.glob(f"stages_run_{run_id}_*.parquet"))
        if stage_files:
            stage_df = pd.concat([pd.read_parquet(f) for f in stage_files], ignore_index=True)
            stage_df = stage_df[stage_df["run_id"] == run_id].copy()

        # Load operator metrics for this run
        operator_df = None
        operator_files = list(self.operators_path.glob(f"operators_run_{run_id}_*.parquet"))
        if operator_files:
            operator_df = pd.concat([pd.read_parquet(f) for f in operator_files], ignore_index=True)
            operator_df = operator_df[operator_df["run_id"] == run_id].copy()

        return run_series, stage_df, operator_df

    def get_latest_run_id(self) -> str | None:
        """Get the most recent run ID.

        Returns:
            Latest run ID or None if no runs found
        """
        if not self.runs_path.exists():
            return None

        run_files = list(self.runs_path.glob("run_*.parquet"))
        if not run_files:
            return None

        # Sort by modification time, get latest
        latest_file = max(run_files, key=lambda f: f.stat().st_mtime)
        # Extract run_id from filename: run_{run_id}.parquet
        run_id = latest_file.stem.replace("run_", "")
        return run_id

    def generate_single_run_report(
        self,
        run_id: str | None = None,
        output_path: str | None = None,
    ) -> str:
        """Generate comprehensive single-run HTML report.

        Args:
            run_id: Run ID to analyze (if None, uses latest run)
            output_path: Output path for HTML report (if None, auto-generated)

        Returns:
            Path to generated report
        """
        # Use latest run if not specified
        if run_id is None:
            run_id = self.get_latest_run_id()
            if run_id is None:
                raise ValueError("No runs found in metrics directory")

        # Load metrics for this run
        run_series, stage_df, operator_df = self.load_single_run_metrics(run_id)

        if run_series is None:
            raise ValueError(f"Run {run_id} not found")

        # Generate HTML
        html = self._generate_single_run_html(run_id, run_series, stage_df, operator_df)

        # Determine output path
        if output_path is None:
            output_path = self.metrics_path / f"report_run_{run_id}.html"
        else:
            output_path = Path(output_path)

        # Write to file
        output_path.write_text(html, encoding="utf-8")

        return str(output_path)

    def _generate_single_run_html(
        self,
        run_id: str,
        run_series: pd.Series,
        stage_df: pd.DataFrame | None,
        operator_df: pd.DataFrame | None,
    ) -> str:
        """Generate HTML for single run analysis."""
        try:
            import plotly.graph_objects as go  # noqa: F401
            from plotly.subplots import make_subplots  # noqa: F401
        except ImportError:
            return self._generate_simple_html(run_id, run_series, stage_df, operator_df)

        sections = []

        # 1. Executive Summary
        sections.append(self._generate_executive_summary(run_series))

        # 2. Data Quality Funnel (漏斗图)
        if stage_df is not None and len(stage_df) > 0:
            sections.append(self._generate_data_funnel(stage_df))

        # 3. Data Flow Sankey Diagram (桑基图)
        if stage_df is not None and len(stage_df) > 0:
            sections.append(self._generate_sankey_diagram(stage_df, operator_df))

        # 4. Performance Timeline (时间线)
        if stage_df is not None and len(stage_df) > 0:
            sections.append(self._generate_timeline_chart(stage_df))

        # 5. Bottleneck Analysis (瓶颈分析)
        if stage_df is not None and operator_df is not None and len(operator_df) > 0:
            sections.append(self._generate_bottleneck_analysis(stage_df, operator_df))

        # 6. Latency Distribution Heatmap (热力图)
        if operator_df is not None and len(operator_df) > 0:
            sections.append(self._generate_latency_heatmap(operator_df))

        # 7. Throughput vs Latency Scatter (气泡图)
        if operator_df is not None and len(operator_df) > 0:
            sections.append(self._generate_throughput_latency_scatter(operator_df))

        # 8. Stage Duration Waterfall (瀑布图)
        if stage_df is not None and len(stage_df) > 0:
            sections.append(self._generate_duration_waterfall(stage_df))

        # 9. Detailed Metrics Tables
        sections.append(self._generate_detailed_tables(stage_df, operator_df))

        # Combine into full HTML
        return self._wrap_html(run_id, run_series, sections)

    def _generate_executive_summary(self, run_series: pd.Series) -> str:
        """Generate executive summary with KPIs."""
        duration_str = f"{run_series['duration']:.2f}s"
        pass_rate = run_series['overall_pass_rate']
        pass_rate_color = "#27ae60" if pass_rate >= 80 else "#e67e22" if pass_rate >= 50 else "#e74c3c"

        return f"""
        <div class="section">
            <h2>📊 Executive Summary</h2>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-label">Total Input</div>
                    <div class="kpi-value">{int(run_series['total_input_records']):,}</div>
                    <div class="kpi-unit">records</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Total Output</div>
                    <div class="kpi-value">{int(run_series['total_output_records']):,}</div>
                    <div class="kpi-unit">records</div>
                </div>
                <div class="kpi-card" style="background: {pass_rate_color}; color: white;">
                    <div class="kpi-label" style="color: rgba(255,255,255,0.9);">Overall Pass Rate</div>
                    <div class="kpi-value">{pass_rate:.1f}%</div>
                    <div class="kpi-unit" style="color: rgba(255,255,255,0.9);">quality score</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Duration</div>
                    <div class="kpi-value">{duration_str}</div>
                    <div class="kpi-unit">execution time</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Throughput</div>
                    <div class="kpi-value">{run_series['avg_throughput']:.1f}</div>
                    <div class="kpi-unit">records/s</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Stages</div>
                    <div class="kpi-value">{int(run_series['num_stages'])}</div>
                    <div class="kpi-unit">pipeline stages</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Data Loss</div>
                    <div class="kpi-value">{int(run_series['total_input_records'] - run_series['total_output_records']):,}</div>
                    <div class="kpi-unit">records filtered</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Errors</div>
                    <div class="kpi-value">{int(run_series['total_errors'])}</div>
                    <div class="kpi-unit">error count</div>
                </div>
            </div>
        </div>
        """

    def _generate_data_funnel(self, stage_df: pd.DataFrame) -> str:
        """Generate data quality funnel chart showing data loss at each stage."""
        import plotly.graph_objects as go

        # Sort stages by order
        stage_df = stage_df.sort_values("stage_name")

        # Create funnel data
        stages = stage_df["stage_name"].tolist()
        input_records = stage_df["input_records"].tolist()
        output_records = stage_df["output_records"].tolist()

        # Funnel chart shows how data flows through stages
        fig = go.Figure()

        # Input funnel
        fig.add_trace(
            go.Funnel(
                name="Input",
                y=stages,
                x=input_records,
                textinfo="value+percent initial",
                marker={"color": "#3498db"},
            )
        )

        # Output funnel
        fig.add_trace(
            go.Funnel(
                name="Output",
                y=stages,
                x=output_records,
                textinfo="value+percent previous",
                marker={"color": "#2ecc71"},
            )
        )

        fig.update_layout(
            title="Data Flow Funnel - Record Counts Through Pipeline",
            height=400,
            template="plotly_white",
        )

        return f'<div class="section"><h2>🔻 Data Quality Funnel</h2><div id="funnel-chart"></div></div>\n<script>Plotly.newPlot("funnel-chart", {fig.to_json()});</script>'

    def _generate_sankey_diagram(self, stage_df: pd.DataFrame, operator_df: pd.DataFrame | None) -> str:
        """Generate Sankey diagram showing data flow between stages and operators."""
        import plotly.graph_objects as go

        # Sort stages
        stage_df = stage_df.sort_values("stage_name")

        # Build nodes
        nodes = ["INPUT"]  # Start node
        node_idx = {"INPUT": 0}

        # Add stage nodes
        for stage in stage_df["stage_name"]:
            nodes.append(stage)
            node_idx[stage] = len(nodes) - 1

        nodes.append("OUTPUT")  # End node
        node_idx["OUTPUT"] = len(nodes) - 1

        # Build links
        source = []
        target = []
        value = []
        colors = []

        # INPUT -> first stage
        first_stage = stage_df.iloc[0]
        source.append(node_idx["INPUT"])
        target.append(node_idx[first_stage["stage_name"]])
        value.append(first_stage["input_records"])
        colors.append("rgba(52, 152, 219, 0.4)")

        # Stage to stage
        for i in range(len(stage_df) - 1):
            curr_stage = stage_df.iloc[i]
            next_stage = stage_df.iloc[i + 1]
            source.append(node_idx[curr_stage["stage_name"]])
            target.append(node_idx[next_stage["stage_name"]])
            value.append(curr_stage["output_records"])
            # Color based on pass rate
            pass_rate = curr_stage["pass_rate"]
            if pass_rate >= 80:
                colors.append("rgba(46, 204, 113, 0.4)")  # Green
            elif pass_rate >= 50:
                colors.append("rgba(230, 126, 34, 0.4)")  # Orange
            else:
                colors.append("rgba(231, 76, 60, 0.4)")  # Red

        # Last stage -> OUTPUT
        last_stage = stage_df.iloc[-1]
        source.append(node_idx[last_stage["stage_name"]])
        target.append(node_idx["OUTPUT"])
        value.append(last_stage["output_records"])
        colors.append("rgba(46, 204, 113, 0.4)")

        fig = go.Figure(
            data=[
                go.Sankey(
                    node={
                        "pad": 15,
                        "thickness": 20,
                        "line": {"color": "black", "width": 0.5},
                        "label": nodes,
                        "color": "#3498db",
                    },
                    link={
                        "source": source,
                        "target": target,
                        "value": value,
                        "color": colors,
                    },
                )
            ]
        )

        fig.update_layout(
            title="Data Flow Sankey Diagram - Record Movement",
            font_size=12,
            height=500,
            template="plotly_white",
        )

        return f'<div class="section"><h2>🌊 Data Flow Sankey</h2><div id="sankey-chart"></div></div>\n<script>Plotly.newPlot("sankey-chart", {fig.to_json()});</script>'

    def _generate_timeline_chart(self, stage_df: pd.DataFrame) -> str:
        """Generate timeline chart showing stage execution durations."""
        import plotly.graph_objects as go

        stage_df = stage_df.sort_values("stage_name")

        # Calculate start times (cumulative)
        start_times = [0]
        for i in range(len(stage_df) - 1):
            start_times.append(start_times[-1] + stage_df.iloc[i]["duration"])

        fig = go.Figure()

        for i, (_idx, row) in enumerate(stage_df.iterrows()):
            fig.add_trace(
                go.Bar(
                    name=row["stage_name"],
                    x=[row["duration"]],
                    y=[row["stage_name"]],
                    orientation="h",
                    marker={
                        "color": f"rgba({50 + i * 40}, {100 + i * 30}, {200 - i * 20}, 0.8)",
                    },
                    text=[f"{row['duration']:.2f}s"],
                    textposition="inside",
                )
            )

        fig.update_layout(
            title="Stage Execution Timeline",
            xaxis_title="Duration (seconds)",
            yaxis_title="Stage",
            barmode="stack",
            height=400,
            template="plotly_white",
            showlegend=False,
        )

        return f'<div class="section"><h2>⏱️ Performance Timeline</h2><div id="timeline-chart"></div></div>\n<script>Plotly.newPlot("timeline-chart", {fig.to_json()});</script>'

    def _generate_bottleneck_analysis(self, stage_df: pd.DataFrame, operator_df: pd.DataFrame) -> str:
        """Analyze and visualize performance bottlenecks."""
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        # Find slowest stage
        slowest_stage = stage_df.loc[stage_df["duration"].idxmax()]

        # Find operator with lowest throughput
        slowest_operator = operator_df.loc[operator_df["throughput"].idxmin()]

        # Find operators with highest latency
        operator_summary = operator_df.groupby("operator_name").agg({"avg_latency": "mean", "throughput": "mean"}).reset_index()

        fig = make_subplots(
            rows=1,
            cols=2,
            subplot_titles=("Stage Duration (Identify Bottleneck)", "Operator Throughput (records/s)"),
            specs=[[{"type": "bar"}, {"type": "bar"}]],
        )

        # Stage durations
        fig.add_trace(
            go.Bar(
                x=stage_df["stage_name"],
                y=stage_df["duration"],
                name="Duration",
                marker_color=["#e74c3c" if name == slowest_stage["stage_name"] else "#3498db" for name in stage_df["stage_name"]],
                text=[f"{d:.2f}s" for d in stage_df["duration"]],
                textposition="outside",
            ),
            row=1,
            col=1,
        )

        # Operator throughput
        fig.add_trace(
            go.Bar(
                x=operator_summary["operator_name"],
                y=operator_summary["throughput"],
                name="Throughput",
                marker_color=["#e74c3c" if name == slowest_operator["operator_name"] else "#2ecc71" for name in operator_summary["operator_name"]],
                text=[f"{t:.1f}" for t in operator_summary["throughput"]],
                textposition="outside",
            ),
            row=1,
            col=2,
        )

        fig.update_xaxes(title_text="Stage", row=1, col=1)
        fig.update_xaxes(title_text="Operator", row=1, col=2)
        fig.update_yaxes(title_text="Duration (s)", row=1, col=1)
        fig.update_yaxes(title_text="Throughput (rec/s)", row=1, col=2)

        fig.update_layout(height=500, template="plotly_white", showlegend=False)

        bottleneck_info = f"""
        <div class="alert alert-warning">
            <strong>⚠️ Bottleneck Detected:</strong><br>
            Slowest Stage: <strong>{slowest_stage['stage_name']}</strong> ({slowest_stage['duration']:.2f}s, {slowest_stage['avg_throughput']:.1f} rec/s)<br>
            Slowest Operator: <strong>{slowest_operator['operator_name']}</strong> ({slowest_operator['throughput']:.1f} rec/s, {slowest_operator['avg_latency']:.3f}s latency)
        </div>
        """

        return f'<div class="section"><h2>🔍 Bottleneck Analysis</h2>{bottleneck_info}<div id="bottleneck-chart"></div></div>\n<script>Plotly.newPlot("bottleneck-chart", {fig.to_json()});</script>'

    def _generate_latency_heatmap(self, operator_df: pd.DataFrame) -> str:
        """Generate heatmap of latency percentiles."""
        import plotly.graph_objects as go

        # Prepare data for heatmap
        operators = operator_df["operator_name"].unique()

        data_matrix = []
        for op in operators:
            op_data = operator_df[operator_df["operator_name"] == op].iloc[0]
            data_matrix.append([
                op_data["min_latency"],
                op_data["p50_latency"],
                op_data["p95_latency"],
                op_data["p99_latency"],
                op_data["max_latency"],
            ])

        fig = go.Figure(
            data=go.Heatmap(
                z=data_matrix,
                x=["Min", "P50", "P95", "P99", "Max"],
                y=list(operators),
                colorscale="RdYlGn_r",
                text=[[f"{v:.3f}s" for v in row] for row in data_matrix],
                texttemplate="%{text}",
                textfont={"size": 10},
                colorbar={"title": "Latency (s)"},
            )
        )

        fig.update_layout(
            title="Latency Heatmap - Percentile Distribution",
            xaxis_title="Percentile",
            yaxis_title="Operator",
            height=400,
            template="plotly_white",
        )

        return f'<div class="section"><h2>🌡️ Latency Heatmap</h2><div id="heatmap-chart"></div></div>\n<script>Plotly.newPlot("heatmap-chart", {fig.to_json()});</script>'

    def _generate_throughput_latency_scatter(self, operator_df: pd.DataFrame) -> str:
        """Generate scatter plot of throughput vs latency with bubble size = input_records."""
        import plotly.graph_objects as go

        operator_summary = (
            operator_df.groupby("operator_name")
            .agg({"throughput": "mean", "avg_latency": "mean", "input_records": "sum", "error_count": "sum"})
            .reset_index()
        )

        fig = go.Figure(
            data=go.Scatter(
                x=operator_summary["avg_latency"],
                y=operator_summary["throughput"],
                mode="markers+text",
                marker={
                    "size": operator_summary["input_records"] / operator_summary["input_records"].max() * 100,
                    "color": operator_summary["error_count"],
                    "colorscale": "Reds",
                    "showscale": True,
                    "colorbar": {"title": "Errors"},
                    "line": {"width": 1, "color": "white"},
                },
                text=operator_summary["operator_name"],
                textposition="top center",
                textfont={"size": 10},
            )
        )

        fig.update_layout(
            title="Throughput vs Latency Scatter (bubble size = input records, color = errors)",
            xaxis_title="Average Latency (seconds)",
            yaxis_title="Throughput (records/s)",
            height=500,
            template="plotly_white",
        )

        return f'<div class="section"><h2>💡 Throughput vs Latency Analysis</h2><div id="scatter-chart"></div></div>\n<script>Plotly.newPlot("scatter-chart", {fig.to_json()});</script>'

    def _generate_duration_waterfall(self, stage_df: pd.DataFrame) -> str:
        """Generate waterfall chart showing cumulative stage durations."""
        import plotly.graph_objects as go

        stage_df = stage_df.sort_values("stage_name")

        # Waterfall data
        x = list(stage_df["stage_name"]) + ["Total"]
        y = list(stage_df["duration"]) + [stage_df["duration"].sum()]

        # Build measure list
        measure = ["relative"] * len(stage_df) + ["total"]

        fig = go.Figure(
            go.Waterfall(
                x=x,
                y=y,
                measure=measure,
                text=[f"{v:.2f}s" for v in y],
                textposition="outside",
                connector={"line": {"color": "rgb(63, 63, 63)"}},
            )
        )

        fig.update_layout(
            title="Duration Waterfall - Cumulative Stage Time",
            xaxis_title="Stage",
            yaxis_title="Duration (seconds)",
            height=400,
            template="plotly_white",
        )

        return f'<div class="section"><h2>📊 Duration Waterfall</h2><div id="waterfall-chart"></div></div>\n<script>Plotly.newPlot("waterfall-chart", {fig.to_json()});</script>'

    def _generate_detailed_tables(self, stage_df: pd.DataFrame | None, operator_df: pd.DataFrame | None) -> str:
        """Generate detailed data tables."""
        html = '<div class="section"><h2>📋 Detailed Metrics</h2>'

        if stage_df is not None and len(stage_df) > 0:
            html += "<h3>Stage Metrics</h3>"
            html += stage_df.to_html(index=False, classes="data-table")

        if operator_df is not None and len(operator_df) > 0:
            html += "<h3>Operator Metrics</h3>"
            html += operator_df.to_html(index=False, classes="data-table")

        html += "</div>"
        return html

    def _wrap_html(self, run_id: str, run_series: pd.Series, sections: list[str]) -> str:
        """Wrap sections in complete HTML document."""
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Run {run_id} - Pipeline Metrics Report</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #2c3e50;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        .header h1 {{ font-size: 36px; margin-bottom: 10px; }}
        .header .run-id {{ font-size: 18px; opacity: 0.9; }}
        .header .timestamp {{ font-size: 14px; opacity: 0.7; margin-top: 10px; }}
        .content {{ padding: 40px; }}
        .section {{
            margin-bottom: 60px;
            padding-bottom: 40px;
            border-bottom: 2px solid #ecf0f1;
        }}
        .section:last-child {{ border-bottom: none; }}
        .section h2 {{
            font-size: 28px;
            color: #2c3e50;
            margin-bottom: 30px;
            padding-bottom: 15px;
            border-bottom: 3px solid #667eea;
        }}
        .section h3 {{
            font-size: 20px;
            color: #34495e;
            margin: 30px 0 15px 0;
        }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .kpi-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }}
        .kpi-card:hover {{ transform: translateY(-5px); }}
        .kpi-label {{
            font-size: 14px;
            opacity: 0.9;
            margin-bottom: 10px;
            font-weight: 500;
        }}
        .kpi-value {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .kpi-unit {{
            font-size: 13px;
            opacity: 0.8;
        }}
        .alert {{
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border-left: 4px solid;
        }}
        .alert-warning {{
            background: #fff3cd;
            border-color: #f39c12;
            color: #856404;
        }}
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        .data-table th {{
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        .data-table td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        .data-table tr:hover {{ background: #f8f9fa; }}
        .footer {{
            text-align: center;
            padding: 30px;
            background: #f8f9fa;
            color: #7f8c8d;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Pipeline Metrics Report</h1>
            <div class="run-id">Run ID: <strong>{run_id}</strong></div>
            <div class="timestamp">Started: {run_series['start_time']} | Ended: {run_series['end_time']}</div>
            <div class="timestamp">Generated: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}</div>
        </div>
        <div class="content">
            {''.join(sections)}
        </div>
        <div class="footer">
            <p><strong>Mega Data Factory</strong> - Data Pipeline Metrics & Analytics</p>
            <p style="margin-top: 10px;">Senior Data Analyst Report | Run-Level Analysis</p>
        </div>
    </div>
</body>
</html>"""

    def _generate_simple_html(
        self,
        run_id: str,
        run_series: pd.Series,
        stage_df: pd.DataFrame | None,
        operator_df: pd.DataFrame | None,
    ) -> str:
        """Generate simple HTML without plotly (fallback)."""
        sections = []

        sections.append(f"""
        <h2>Run Summary</h2>
        <table border="1">
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Run ID</td><td>{run_id}</td></tr>
            <tr><td>Duration</td><td>{run_series['duration']:.2f}s</td></tr>
            <tr><td>Input Records</td><td>{int(run_series['total_input_records']):,}</td></tr>
            <tr><td>Output Records</td><td>{int(run_series['total_output_records']):,}</td></tr>
            <tr><td>Pass Rate</td><td>{run_series['overall_pass_rate']:.2f}%</td></tr>
        </table>
        """)

        if stage_df is not None:
            sections.append(f"<h2>Stage Metrics</h2>{stage_df.to_html(index=False)}")

        if operator_df is not None:
            sections.append(f"<h2>Operator Metrics</h2>{operator_df.to_html(index=False)}")

        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Run {run_id} Report</title></head>
<body style="font-family: Arial; padding: 20px;">
<h1>Pipeline Metrics Report - Run {run_id}</h1>
{''.join(sections)}
</body></html>"""

    def publish_to_huggingface(
        self,
        report_path: str,
        repo_id: str,
        token: str | None = None,
        commit_message: str | None = None,
    ) -> str:
        """Publish HTML report to HuggingFace Space.

        Args:
            report_path: Path to HTML report file
            repo_id: HuggingFace repo ID (e.g., "username/space-name")
            token: HuggingFace API token (if None, uses HF_TOKEN env var)
            commit_message: Commit message for the upload

        Returns:
            URL of the published space
        """
        try:
            from huggingface_hub import HfApi, create_repo
        except ImportError as e:
            raise ImportError(
                "huggingface_hub is required for publishing to HuggingFace. "
                "Install it with: pip install huggingface_hub"
            ) from e

        api = HfApi(token=token)

        # Create repo if it doesn't exist (Space type)
        try:
            create_repo(
                repo_id=repo_id,
                repo_type="space",
                space_sdk="static",
                exist_ok=True,
                token=token,
            )
        except Exception as e:
            print(f"Warning: Could not create repo (may already exist): {e}")

        # Upload the report as index.html
        if commit_message is None:
            commit_message = f"Update metrics report - {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}"

        api.upload_file(
            path_or_fileobj=report_path,
            path_in_repo="index.html",
            repo_id=repo_id,
            repo_type="space",
            commit_message=commit_message,
            token=token,
        )

        space_url = f"https://huggingface.co/spaces/{repo_id}"
        print(f"Report published to: {space_url}")

        return space_url
