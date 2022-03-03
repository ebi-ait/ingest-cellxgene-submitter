import os
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

wd = Path(os.environ.get('TMP_DIR'), str(uuid.uuid4()))
wd.mkdir(parents=True)

# Just a global dict to access runtime context
# Can use something like Context Locals if we support multithreading in the future
# https://werkzeug.palletsprojects.com/en/2.0.x/local/
context = {
    'wd': wd
}
