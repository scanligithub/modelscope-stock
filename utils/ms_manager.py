from modelscope.hub.api import HubApi
import os

class MSManager:
    def __init__(self, token, dataset_id):
        self.api = HubApi()
        self.api.login(token)
        self.dataset_id = dataset_id
        
    def upload_file(self, local_path, path_in_repo):
        print(f"🚀 Uploading {local_path} to ModelScope Dataset: {path_in_repo}...")
        try:
            # 【核心修复】：ModelScope SDK 参数名使用 dataset_id
            self.api.upload_dataset_file(
                dataset_id=self.dataset_id,
                file_or_folder=local_path,
                file_path=path_in_repo
            )
        except Exception as e:
            print(f"❌ Upload Failed: {e}")
