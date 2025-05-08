import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

start_time = datetime.now()

# Simulate work
import time
time.sleep(1.64)

end_time = datetime.now()
duration = end_time - start_time
elapsed_seconds = duration.total_seconds()

# Correct logging
logger.info(f"Script completed successfully in {elapsed_seconds:.2f} seconds")
