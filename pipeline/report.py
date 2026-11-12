import html
import json
from collections import defaultdict


def write_report(run, metrics):
    """Небольшой автономный HTML: ссылки на настоящие GX/dbt reports и времена этапов."""
    durations = defaultdict(float)
    for line in (run / "events.jsonl").read_text().splitlines():
        entry = json.loads(line)
        if entry["event"] == "stage_finished":
            durations[entry["stage"]] += entry["seconds"]
    maximum = max(durations.values(), default=1)
    bars = "".join(
        f'<text x="0" y="{i * 32 + 20}">{html.escape(stage)}</text>'
        f'<rect x="110" y="{i * 32 + 5}" width="{seconds / maximum * 420:.1f}" '
        f'height="22" fill="#1976d2"/>'
        f'<text x="540" y="{i * 32 + 20}">{seconds:.3f} s</text>'
        for i, (stage, seconds) in enumerate(durations.items())
    )
    links = [f'<li><a href="{p.relative_to(run)}">{html.escape(p.name)}</a></li>'
             for p in run.rglob("*.html") if p.name in {"index.html", "static_index.html"}]
    (run / "report.html").write_text(
        '<!doctype html><meta charset="utf-8"><title>Taxi ELT</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:40px auto}svg{width:100%}</style>'
        '<h1>Taxi ELT — результаты запуска</h1><pre>'
        + html.escape(json.dumps(metrics, indent=2, ensure_ascii=False))
        + '</pre><h2>Время успешных этапов</h2>'
        + f'<svg viewBox="0 0 650 {len(durations) * 32}">{bars}</svg>'
        + '<h2>Great Expectations / DBT Docs</h2><ul>' + ''.join(links) + '</ul>'
    )
