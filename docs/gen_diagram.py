import base64
import requests
from pathlib import Path

def run():
    # Betiğin bulunduğu klasörü baz al
    base_dir = Path(__file__).parent
    src = base_dir / "architecture.mmd"
    dest = base_dir / "architecture.svg"

    if not src.exists():
        print(f"Error: {src} not found.")
        return

    content = src.read_text(encoding="utf-8")
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    url = f"https://mermaid.ink/svg/{encoded}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dest.write_bytes(response.content)
        print(f"Success: {dest} updated.")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    run()
