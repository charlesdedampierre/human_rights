from flask import Flask, render_template_string, jsonify
import json
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)

# Paths
STATUS_FILE = Path(__file__).parent.parent / "wikidata_sparql_scripts/instance_properties/output/status.json"
LOG_FILE = Path(__file__).parent.parent / "wikidata_sparql_scripts/instance_properties/output/extraction_stdout.log"

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Extraction Monitor</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect fill='%231a1a2e' width='100' height='100' rx='15'/><path fill='%2300d4ff' d='M20 70V40l15 15 15-25 15 20 15-30v50H20z'/><circle fill='%2300ff88' cx='35' cy='55' r='4'/><circle fill='%2300ff88' cx='50' cy='30' r='4'/><circle fill='%2300ff88' cx='65' cy='45' r='4'/><circle fill='%2300ff88' cx='80' cy='25' r='4'/></svg>">
    <meta http-equiv="refresh" content="30">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, sans-serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 15px;
            background: #1a1a2e;
            color: #eee;
        }
        h1 { color: #00d4ff; font-size: 1.5em; text-align: center; }
        h2 { color: #00d4ff; font-size: 1.1em; margin-top: 20px; }
        .card { background: #16213e; border-radius: 12px; padding: 15px; margin: 15px 0; }
        .status { font-size: 1.3em; margin-bottom: 15px; text-align: center; }
        .status.running { color: #00ff88; }
        .status.completed { color: #00d4ff; }
        .status.failed { color: #ff4757; }
        .progress-bar { background: #0f3460; border-radius: 10px; height: 25px; overflow: hidden; }
        .progress-fill { background: linear-gradient(90deg, #00d4ff, #00ff88); height: 100%; transition: width 0.5s; }
        .stats { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-top: 15px; }
        .stat { background: #0f3460; padding: 12px; border-radius: 8px; text-align: center; }
        .stat-value { font-size: 1.4em; font-weight: bold; color: #00d4ff; }
        .stat-label { color: #888; font-size: 0.75em; margin-top: 3px; }
        .time-remaining {
            font-size: 2em;
            color: #00ff88;
            text-align: center;
            margin: 20px 0;
            font-weight: bold;
        }
        .time-label { font-size: 0.5em; color: #888; display: block; }
        .system { margin-top: 15px; font-size: 0.8em; color: #666; text-align: center; }
        .refresh-note { color: #444; font-size: 0.7em; text-align: center; margin-top: 10px; }
        .chart-container {
            background: #0f3460;
            border-radius: 8px;
            padding: 15px;
            margin-top: 15px;
            height: 250px;
        }

        @media (min-width: 600px) {
            .stats { grid-template-columns: repeat(4, 1fr); }
            .stat-value { font-size: 1.8em; }
            h1 { font-size: 2em; }
        }
    </style>
</head>
<body>
    <h1>Wikidata Extraction</h1>
    {% if status %}
    <div class="card">
        <div class="status {{ status.status }}">{{ status.status.upper() }}</div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: {{ status.progress.percent_complete }}%"></div>
        </div>

        <div class="time-remaining">
            {{ time_remaining }}
            <span class="time-label">remaining</span>
        </div>

        <div class="stats">
            <div class="stat">
                <div class="stat-value">{{ "{:,}".format(status.progress.completed_items) }}</div>
                <div class="stat-label">Extracted</div>
            </div>
            <div class="stat">
                <div class="stat-value">{{ status.progress.percent_complete }}%</div>
                <div class="stat-label">Complete</div>
            </div>
            <div class="stat">
                <div class="stat-value">{{ status.performance.items_per_second or status.performance.average_items_per_second or 0 }}</div>
                <div class="stat-label">Items/sec</div>
            </div>
            <div class="stat">
                <div class="stat-value">{{ total_timeouts }}</div>
                <div class="stat-label">Timeouts</div>
            </div>
        </div>

        <h2>Speed & Timeouts over time</h2>
        <div class="chart-container" style="height: 300px;">
            <canvas id="mainChart"></canvas>
        </div>

        {% if status.system %}
        <div class="system">
            Disk: {{ status.system.disk_free_gb }} GB free
        </div>
        {% endif %}
    </div>
    {% else %}
    <div class="card">
        <p>No status file found. Extraction may not have started.</p>
    </div>
    {% endif %}
    <p class="refresh-note">Auto-refreshes every 30 seconds</p>

    <script>
        const chartData = {{ chart_data | safe }};

        new Chart(document.getElementById('mainChart'), {
            type: 'line',
            data: {
                datasets: [
                    {
                        label: 'Speed (items/sec)',
                        data: chartData.speed_points,
                        borderColor: '#6b9e8a',
                        backgroundColor: 'rgba(107, 158, 138, 0.1)',
                        fill: true,
                        tension: 0.2,
                        pointRadius: 0,
                        borderWidth: 1.5,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Timeouts',
                        data: chartData.timeout_points,
                        borderColor: '#a65d5d',
                        backgroundColor: 'rgba(166, 93, 93, 0.3)',
                        fill: true,
                        tension: 0.2,
                        pointRadius: 0,
                        borderWidth: 1.5,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: { color: '#888', boxWidth: 12 }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'minute',
                            stepSize: 30,
                            displayFormats: { minute: 'HH:mm' },
                            tooltipFormat: 'MMM d, HH:mm'
                        },
                        ticks: { color: '#888', maxTicksLimit: 15 },
                        grid: { color: '#252545' }
                    },
                    y: {
                        type: 'linear',
                        position: 'left',
                        ticks: { color: '#6b9e8a' },
                        grid: { color: '#252545' },
                        beginAtZero: true,
                        title: { display: true, text: 'items/sec', color: '#6b9e8a' }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        ticks: { color: '#a65d5d' },
                        grid: { drawOnChartArea: false },
                        beginAtZero: true,
                        title: { display: true, text: 'timeouts', color: '#a65d5d' }
                    }
                }
            }
        });
    </script>
</body>
</html>
'''

def fetch_status():
    """Fetch status.json from local file."""
    try:
        if STATUS_FILE.exists():
            with open(STATUS_FILE) as f:
                return json.load(f)
    except Exception as e:
        print(f"Error fetching status: {e}")
    return None

def parse_logs():
    """Parse log file for speed and timeout data, aggregated by 3-minute intervals."""
    speed_by_interval = defaultdict(list)  # {interval: [speeds]}
    timeout_counts = defaultdict(int)  # {interval: count}
    total_timeouts = 0

    try:
        if LOG_FILE.exists():
            with open(LOG_FILE, 'r') as f:
                for line in f:
                    # Parse speed from PROGRESS lines
                    # Format: 2026-01-24 10:51:28,155 - INFO - PROGRESS: ... | 14.4 items/s | ...
                    if 'items/s' in line:
                        match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}):(\d{2}).*?(\d+\.?\d*) items/s', line)
                        if match:
                            date_hour = match.group(1)
                            minute = int(match.group(2))
                            speed = float(match.group(3))
                            # Round to 3-minute interval
                            interval_min = (minute // 3) * 3
                            interval_key = f"{date_hour}:{interval_min:02d}"
                            speed_by_interval[interval_key].append(speed)

                    # Parse timeouts
                    if 'Timeout' in line or 'timed out' in line.lower() or 'Rate limited' in line:
                        match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}):(\d{2})', line)
                        if match:
                            date_hour = match.group(1)
                            minute = int(match.group(2))
                            interval_min = (minute // 3) * 3
                            interval_key = f"{date_hour}:{interval_min:02d}"
                            timeout_counts[interval_key] += 1
                            total_timeouts += 1
    except Exception as e:
        print(f"Error parsing logs: {e}")

    # Average speed per interval
    speed_data = {k: sum(v)/len(v) for k, v in speed_by_interval.items()}

    return speed_data, dict(timeout_counts), total_timeouts

def calc_time_remaining(eta_str):
    """Calculate hours and minutes remaining from ETA."""
    try:
        eta = datetime.fromisoformat(eta_str)
        now = datetime.now()
        diff = eta - now

        if diff.total_seconds() < 0:
            return "Done!"

        hours = int(diff.total_seconds() // 3600)
        minutes = int((diff.total_seconds() % 3600) // 60)

        if hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    except:
        return "calculating..."

@app.route('/')
def index():
    status = fetch_status()
    time_remaining = "..."

    if status and status.get('eta'):
        time_remaining = calc_time_remaining(status['eta'])

    # Parse logs for charts
    speed_hourly, timeout_counts, total_timeouts = parse_logs()

    # Prepare speed data points (one per 3-min interval)
    speed_points = []
    for interval_key in sorted(speed_hourly.keys()):
        # interval_key is like "2026-01-24 13:06"
        speed_points.append({
            'x': interval_key.replace(' ', 'T'),
            'y': round(speed_hourly[interval_key], 1)
        })

    # Prepare timeout data points (one per 3-min interval)
    timeout_points = []
    for interval_key in sorted(timeout_counts.keys()):
        timeout_points.append({
            'x': interval_key.replace(' ', 'T'),
            'y': timeout_counts[interval_key]
        })

    chart_data = json.dumps({
        'speed_points': speed_points,
        'timeout_points': timeout_points
    })

    return render_template_string(HTML,
                                  status=status,
                                  time_remaining=time_remaining,
                                  total_timeouts=total_timeouts,
                                  chart_data=chart_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
