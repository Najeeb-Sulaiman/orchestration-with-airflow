# from steps.extract import extract
# from steps.load import load
# from steps.transform import transform
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "steps"))


from extract import extract  # noqa: E402
from load import load  # noqa: E402
from transform import transform  # noqa: E402
from notify import notify  # noqa: E402


def main():
    print("Pipeline run started")

    extract()
    load()
    transform()
    notify()

    print("Pipeline run completed")


if __name__ == "__main__":
    main()
