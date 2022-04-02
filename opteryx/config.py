import os
import dotenv 
from pathlib import Path

env_path = Path(".") / ".env"
dotenv.load_dotenv(dotenv_path=env_path)


INTERNAL_BATCH_SIZE: int = int(os.environ.get("INTERNAL_BATCH_SIZE", 100))

