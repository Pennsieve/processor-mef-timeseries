import os
import sys

# Modules under processor/ import each other by bare name (e.g. `from config
# import Config`), so the package dir has to be importable directly.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processor"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
