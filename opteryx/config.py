import os
from pathlib import Path

import dotenv

try:
    env_path = Path(".") / ".env"
    dotenv.load_dotenv(dotenv_path=env_path)
except:
    pass  # fail quietly

# The maximum input frame size for JOINs
INTERNAL_BATCH_SIZE: int = int(os.environ.get("INTERNAL_BATCH_SIZE", 500))
# The maximum number of records to create in a CROSS JOIN frame
MAX_JOIN_SIZE: int = int(os.environ.get("MAX_JOIN_SIZE", 1000000))
