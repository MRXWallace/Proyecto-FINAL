from load_kaggle import get_kaggle

# Include in the list the Dataset to download
list_download_kaggle = ['usdot/flight-delays', 'bingecode/us-national-flight-data-2015-2020' ]


for dataset in list_download_kaggle:
    get_kaggle(dataset)


