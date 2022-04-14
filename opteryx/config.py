import os
import dotenv 
from pathlib import Path

try:
    env_path = Path(".") / ".env"
    dotenv.load_dotenv(dotenv_path=env_path)
except:
    pass # fail quietly

INTERNAL_BATCH_SIZE: int = int(os.environ.get("INTERNAL_BATCH_SIZE", 500))

