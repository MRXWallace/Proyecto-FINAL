# ____________________________________________________________________________ #
# In order to use the Kaggle’s public API, you must first authenticate using an 
# API token
#  api.dataset_download_file --> gets one file
#  api.dataset_download_files --> gets the full Dataset
# https://www.kaggle.com/docs/api
# https://www.kaggle.com/code/donkeys/kaggle-python-api/notebook
# ____________________________________________________________________________ #
import kaggle

from kaggle.api.kaggle_api_extended import KaggleApi

def get_kaggle(dataset):
    # Directory to download kaggle files
    path = './downloads'

    try:
        api = KaggleApi()
        api.authenticate()

        api.dataset_download_files(dataset, path=path, quiet=False, unzip=True)
    except :
        print("An exception ocurred")

