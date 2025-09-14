import sys
import subprocess


def main():
    # Run pytest in the tests directory
    res = subprocess.run([sys.executable, "-m", "pytest", "-q", "tests"], cwd=".")
    raise SystemExit(res.returncode)


if __name__ == "__main__":
    main()
