import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import kafkamysql  # noqa # pylint: disable=unused-import, wrong-import-position
from kafkamysql import utils

