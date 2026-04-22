from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_HTML = ROOT / "dashboard-concept.html"
DATA_JSON = ROOT / "dashboard_data.json"
OUTPUT_HTML = ROOT / "dashboard-standalone.html"


def main() -> None:
    if not SOURCE_HTML.exists():
        raise SystemExit(f"Arquivo base nao encontrado: {SOURCE_HTML}")
    if not DATA_JSON.exists():
        raise SystemExit(
            f"Arquivo de dados nao encontrado: {DATA_JSON}. Rode primeiro o gerador de dados."
        )

    html = SOURCE_HTML.read_text(encoding="utf-8")
    payload = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    inline_script = f"<script>window.DASHBOARD_DATA = {json.dumps(payload, ensure_ascii=True)};</script>"

    target = '<script src="./dashboard_data.js"></script>'
    if target not in html:
        raise SystemExit("Nao foi encontrado o ponto de injecao do script de dados no HTML.")

    html = html.replace(target, inline_script)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"Standalone dashboard written to {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
