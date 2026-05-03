import sys
from pathlib import Path

folder = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("notebooks")

for file in folder.rglob("*"):
    if file.suffix in [".ipynb", ".py"]:
        text = file.read_text(errors="ignore")
        blocked = ["C:\\", "/Users/", "/home/", "password", "secret", "token"]

        for item in blocked:
            if item.lower() in text.lower():
                print(f"Potential hardcoded value found in {file}: {item}")
                sys.exit(1)

print("No hardcoded paths or secrets found.")
