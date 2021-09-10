import json
import boto3
import pandas as pd
from pandas.io.json import json_normalize
import timeit
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
import warnings
warnings.filterwarnings("ignore")
import datamapper


def lambda_handler(event, context):

    '''
    Here event is a data dump as file in S3 or listen to database for a certain number of batch

    '''

    data = "fetch data from S3/database"

    canonical_list = "fetch from common source to update or create one"



    data = datamapper.prepare_canonical_dataframe(data)
    (or)
    canonical_data = datamapper.update_canonical_data(date)


    mapped_data = datamapper.map_data_to_canonical_data(data)
    (or)
    update_mappings_to_data = datamapper.map_data_to_canonical_data(data,already_mapped=True)


    datamapper.upload_updated_canonical_info_to_s3()

    
