from modelscope.hub.api import HubApi
import os

class MSManager:
    def __init__(self, token, dataset_id):
        self.api = HubApi()
        self.api.login(token)
        self.dataset_id = dataset_id
        
    def upload_folder(self, local_path, path_in_repo):
        self.api.upload_dataset_file(
            dataset_id=self.dataset_id,
            file_or_folder=local_path,
            file_path=path_in_repo
        )
