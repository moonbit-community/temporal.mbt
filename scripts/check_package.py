"""Verify distribution contents and test the extracted MoonBit module on Linux."""
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile

root = Path(__file__).resolve().parent.parent
subprocess.run(["moon", "package"], cwd=root, check=True)
archives = list((root / "_build/publish").glob("*.zip"))
assert len(archives) == 1, archives
vendored = subprocess.check_output(
    ["git", "ls-files", "vendor/temporal-sdk-core"], cwd=root, text=True
).splitlines()
required = vendored + ["moon.mod", "moon.pkg", "temporal_mbt_bridge.c", "core_test.mbt"]
with tempfile.TemporaryDirectory(prefix="temporal-package-") as temp:
    extracted = Path(temp)
    with zipfile.ZipFile(archives[0]) as archive:
        names = archive.namelist()
        assert all(not Path(n).is_absolute() and ".." not in Path(n).parts for n in names)
        assert not any("/target/" in n or n.startswith("_build/") for n in names)
        for name in required:
            assert archive.read(name) == (root / name).read_bytes(), name
        archive.extractall(extracted)
    # The clean Linux checkout already built these byte-identical vendored Rust
    # sources. Reuse only that static-library output; compile and test MoonBit
    # and its C stub from the extracted distribution.
    vendor = Path("vendor/temporal-sdk-core")
    (extracted / vendor / "target").symlink_to(root / vendor / "target", target_is_directory=True)
    stamp = vendor / "VENDOR.stamp"
    stat = (root / stamp).stat()
    os.utime(extracted / stamp, (stat.st_atime, stat.st_mtime))
    subprocess.run(["moon", "check", "--target", "native"], cwd=extracted, check=True)
    subprocess.run(["moon", "test", "--target", "native"], cwd=extracted, check=True)
print(f"Verified {len(names)} archive members, including all {len(vendored)} vendored Rust files.")
