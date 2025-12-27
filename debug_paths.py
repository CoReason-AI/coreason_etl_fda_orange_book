import os
from pathlib import Path

def check_paths(base_dir, filename):
    print(f"--- Checking {filename} relative to {base_dir} ---")

    # Setup
    base_path = Path(base_dir).resolve()
    target_path = base_path / filename

    # Pathlib approach
    try:
        resolved_pathlib = target_path.resolve()
        is_relative = resolved_pathlib.is_relative_to(base_path)
        print(f"Pathlib: {resolved_pathlib} | Relative? {is_relative}")
    except Exception as e:
        print(f"Pathlib error: {e}")

    # os.path approach
    real_base = os.path.realpath(base_dir)
    real_target = os.path.realpath(os.path.join(real_base, filename))

    # check with commonpath
    try:
        common = os.path.commonpath([real_base, real_target])
        is_safe = common == real_base
        print(f"os.path: {real_target} | Common base? {common} | Safe? {is_safe}")
    except Exception as e:
        print(f"os.path error: {e}")

if __name__ == "__main__":
    os.makedirs("test_sandbox/safe", exist_ok=True)
    base = "test_sandbox/safe"

    check_paths(base, "good.txt")
    check_paths(base, "../evil.txt")
    check_paths(base, "nested/../good.txt")
    # check_paths(base, "/etc/passwd") # Absolute path might be sanitized by zipfile join but let's see os.path.join behavior
