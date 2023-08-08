import os
import json
from utils.logger import log_message


class CacheManager:
    def __init__(self, cache_path) -> None:
        self.default_content = {
            "start_date": "",
            "end_date": ""
        }

        self.cache_path = cache_path

    def reinitialize_cache(self):
        """
        Reinitialize the cache by overwriting it with the default content.

        Args:
            cache_path (str): The path to the cache JSON file.

        Returns:
            None
        """
        log_message("reinitialize_cache() called.")

        with open(self.cache_path, 'w') as json_file:
            json.dump(self.default_content, json_file, indent=4)

        log_message(f"Cache reinitialized. It can be found at: {self.cache_path}")



def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
