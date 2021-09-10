import re
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import warnings
warnings.filterwarnings("ignore")



'''
    The functions are defined to match the requirements of analysis notebook 
    and these functions are what will be used for production deployment.

    After making an object out of this class and by supplying relavant data, it can directly be used as mapping in production 
    by pickling the object.

    Params:
    Canonical_df : if already exists {optional} or else it will create from raw_data
    raw_data : the data which needs to have it mapped with canonical_df


'''



def clean_data(data,for_canonical_list=False):
    '''
    Cleans available data into a cleaner format for comparision and mapping to canonical list

    params :

    data : dataframe to clean
    for_canonical_list (Bool) : additional cleaning which include dropping rows 

    '''
    print('Begin data cleaning')

    #Remove columns with only one unique value which is useless:
    for column in data.columns:
        if (data[column].nunique() == 1) :
            data.drop([column], axis = 1, inplace = True)   # Removes country column
    #data.drop(['country'], axis = 1, inplace = True)   

    if data.shape[0]==0:
        return data    

    else:
        number_map = {
            'one': '1',
            'two': '2',
            'three': '3',
            'four': '4',
            'five': '5',
            'six': '6',
            'seven': '7',
            'eight': '8',
            'nine': '9',
            'zero' : '0'
        }

        number_map = {r"\b{}\b".format(k): v for k, v in number_map.items()}

        non_numeric_columns = data.dtypes[data.dtypes != 'int64'][data.dtypes != 'float64'].index
        # Lower case entire data :
        for column in non_numeric_columns:
            data[column] = data[column].str.strip().str.lower()
        
        # Print unique values in each column to examine:
        for column in data.columns:
            print("The " + column  + " has following unique elements :" + \
                str(data[column].nunique()) + " out of 30000 entries")

        #Handle non numeric columns (EXCLUDING WEB ADDRESSES):

        #remove special charecters in text
        data[['name','address_line','zip','city','state']]=data[['name','address_line','zip','city','state']].replace('[^A-Za-z0-9]+',' ',regex=True)

        for column in non_numeric_columns:
            #Lower case all the columns to standardize casing
            data[column] = data[column].str.strip().str.lower()
            #replace numbers and numerical words to corresponding wording
            data[column] = data[column].replace(number_map, regex=True)
            #Fill any blank spaces with nulls.
            data[column] = data[column].replace(r'^\s*$', np.NaN, regex=True)
        
        #Remove rows where name is null.
        if for_canonical_list:
            data = data[~data.name.isnull()]
            if data.shape[0]==0:
                return data


        ## derive standard 5 digit zip code from different formats to seperate column
        data['zip_short']  = data.zip.apply(lambda x: str(x).split(' ')[0] if len(str(x))>5 else x)

        #Remove any zip codes and keep it to null which is malformed
        data.zip = data.zip.apply(lambda x: x if str(x).replace(' ','').isdigit() else np.nan)
        data.zip_short = data.zip_short.apply(lambda x: x if str(x).replace(' ','').isdigit() else np.nan)

        #Remove zips which are nulls :
        if for_canonical_list:
            data = data[~data.zip.isnull()]
            if data.shape[0]==0:
                return data

        # Generalize web addresses to appropriate format, (www.example.com) .
        data.web = data.web.str.strip().str.replace('https://','').str.replace('http://','')
        data.web = data.web.apply(lambda x: str(x).split('/')[0] if x is not None else np.nan)
        data.web = data.web.replace('nan',np.nan)

        #Remove any duplicates in whole data before proceeding.
        if for_canonical_list:
            data = data.drop_duplicates(subset = data.columns, keep = 'first')

        #This function can further be refined based on standards for imprving data quality which can be done along as time permits.
        print("data cleaned")

        return data


def prepare_canonical_dataframe(data,from_raw_data=True):
        '''
            This function is to extract canonical list from the cleaned data, this can be merged in above function itself,
            but seperating inorder to have a common cleaning functionality and reduce code redundancy.

            Note : Only cleaned data needs to be sent for better results.

            This function consolidates series of overlapping data to produce a refined data by 
            using different elements in columns as combnation of primary keys

            The compression is done step by step based on count of unique elements in one of the targets and consilodated based on
            the benefit of doubt that everything convey similar info


            Params:
            data : dataframe from which canonical data needs to be prepared
            from_raw_data (bool): for using when creating a canonical list rather than updating
        '''

        #To consolidate entries based on base meta which is name, address,city,state,zip

        data = clean_data(data,for_canonical_list=True)

        if data.shape[0]==0:
            return "no proper data"
        else:
            data['meta_data'] = data['name'].fillna(' ') + ',' + data['address_line'].fillna(' ') + ',' + data['city'].fillna(' ')\
                                +data['state'].fillna(' ') + ',' + data['zip_short'].fillna(' ')

            data=data.groupby(['meta_data']).first().reset_index()


            #To consolidate based on name and address
            data['dealer_addresses'] = data['name'] + ',' + data['address_line']
            data=data.groupby(['dealer_addresses']).first().reset_index()

            #To consolidate based on dealer_contact. hand nothing much to refine in data given, but can be usefu in new data that can come.
            data['dealer_contact'] = data['name'].fillna(' ') + ',' + data['address_line'].fillna(' ') +  ',' + data['phone'].fillna(' ')
            data=data.groupby(['dealer_contact']).first().reset_index()


            #Consolidate based on internet presence :
            data['internet_presence'] = data['name'].fillna(' ') + ',' + data['phone'].fillna(' ') + ',' + data['zip_short'].fillna(' ') + ',' \
                                        + data['web'].fillna(' ') + ',' + data['google_url'].fillna(' ') + ',' + data['facebook_url'].fillna(' ')\
                                        + data['cars_url'].fillna(' ')
            data=data.groupby(['internet_presence']).first().reset_index()


            #Consolidate based on nane city and zip_short :
            data =data.groupby(['name','city','zip_short']).first().reset_index()

            print("data reduced to "+ str(data.shape[0]))

            ## Assign ID to canonical list assuming this is the final list.
            #This can actually be refined with much better quality given the time taking multiple combinations as primary keys to fix overlappings.

            canonical_df =data[['name','city','zip_short','address_line','state','zip','phone','web','google_url','facebook_url','cars_url']]

            canonical_df['meta_data'] = canonical_df['name'].fillna(' ') + ',' + canonical_df['address_line'].fillna(' ') + ',' + canonical_df['city'].fillna(' ')\
                                        +canonical_df['state'].fillna(' ') + ',' + canonical_df['zip_short'].fillna(' ') + ',' + canonical_df['web'].fillna(' ') \
                                        + ',' + canonical_df['phone'].fillna(' ')
            if from_raw_data:
                canonical_df['canonical_id'] = canonical_df.index + 1


            return canonical_df



def map_data_to_canonical_data(data,canonical_data,threshold = 50,already_mapped=False):
    '''
        To map latter and new entries w.r.t canonical data as well as append new values to canonical data when
        encountered based on threshold of confidence.
    '''
    if already_mapped :
        mapped_data = data[data.mapping_canonical_id!='Not Applicable']
        data = data[data.mapping_canonical_id=='Not Applicable']

    pure_data = clean_data(data)
    pure_data['meta_data'] = pure_data['name'].fillna(' ') + ',' + pure_data['address_line'].fillna(' ') + ',' + pure_data['city'].fillna(' ')\
                                +pure_data['state'].fillna(' ') + ',' + pure_data['zip_short'].fillna(' ') + ',' + pure_data['web'].fillna(' ') \
                                + ',' + pure_data['phone'].fillna(' ')
    
    # Map a canonical ID and add confidence columns to new data:
    mapping_table = pd.DataFrame(columns = ['confidence','mapping_canonical_id'])
    for index,row in pure_data.iterrows():
        confidence,mapping_canonical_id = fetch_mapping_id(row,canonical_data)
        if confidence<threshold:
            mapping_canonical_id = 'Not Applicable'
        mapping_table = mapping_table.append({'confidence':confidence,'mapping_canonical_id':mapping_canonical_id},ignore_index=True)
    print(mapping_table.shape)
    data = pd.concat([data, mapping_table], axis=1)

    if already_mapped:
        data = data.append(mapped_data)
        
    return data

        

    #data = data['meta_data'].apply(lambda x : fetch_mapping_id(x,canonical_data,threshold=50))

    return data


def fetch_mapping_id(row,canonical_data,threshold = 50):
    similarity_extraction = process.extract(row.meta_data,canonical_data.meta_data,scorer=fuzz.token_sort_ratio)[0]
    #print(similarity_extraction)
    confidence = similarity_extraction[len(similarity_extraction)-2]
    mapping_canonical_id = similarity_extraction[len(similarity_extraction)-1] + 1
    return confidence,mapping_canonical_id



def update_canonical_data(mapped_data,canonical_list):
    '''
    To update canonical data based on "not applicables" in mapped_data
    '''
    mapped_data = mapped_data[mapped_data['mapping_canonical_id']=='Not Applicable']
    updates = prepare_canonical_dataframe(mapped_data,from_raw_data=False)
    if updates == "no proper data":
        return canonical_list
    else:
        start_id = canonical_list.index + 1
        updates['canonical_id'] = start_id + updates.index
        canonical_list = canonical_list.append(updates)
        return canonical_list





def upload_updated_canonical_info_to_s3():
    '''
    Open an AWS session and send the updated canonical list to common storage in S3 for having changes in place.
    '''
    # Nothing to do as of now.... {part of further improvements}

    












