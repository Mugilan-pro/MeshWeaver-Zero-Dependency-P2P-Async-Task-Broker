"""
MeshWeaver Master Test Runner
Discovers and executes all unit test suites across the entire repository.
Compatible with Python 3.7+ through 3.13+.
"""

import sys
import unittest
import os

# Ensure repository root is on Python path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

def run_suite():
    print("=" * 70)
    print("   MESHWEAVER: RUNNING MASTER AUTOMATED TEST SUITE")
    print("=" * 70)

    tests_dir = os.path.dirname(__file__)
    loader = unittest.TestLoader()
    suite = loader.discover(tests_dir, pattern="test_*.py", top_level_dir=REPO_ROOT)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 70)
    print(f"   TEST SUMMARY: {result.testsRun} tests executed")
    print(f"   Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   Failures: {len(result.failures)} | Errors: {len(result.errors)}")
    print("=" * 70)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_suite())
