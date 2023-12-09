import os
import pickle
import pandas as pd
import json
import os
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
GOOGLE_SECRETS = 'client_secrets.json'

load_dotenv()

def get_authentication(scopes):

    with open(GOOGLE_SECRETS, 'r') as json_file:
        data = json.load(json_file)

    data['web']['project_id'] = os.environ['GOOGLE_PROJECT_ID']
    data['web']['client_id'] = os.environ['GOOGLE_CLIENT_ID']
    data['web']['client_secret'] = os.environ['GOOGLE_CLIENT_SECRET']
    data['web']['redirect_uris'] = os.environ['GOOGLE_REDIRECT_URI']

    with open(GOOGLE_SECRETS, 'w') as json_file:
        json.dump(data, json_file, indent=2)
    
    credentials = None

    # STORES USER'S CREDENTIALS FROM PREVIOUSLY SUCCESSFUL LOGINS
    if os.path.exists('token.pkl'):
        print('Loading credentials from file')
        with open('token.pkl', 'rb') as token_file:
            credentials = pickle.load(token_file)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print('Refreshing access token')
            credentials.refresh(Request())
        else:
            print('Fetching new tokens')

            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json',
                scopes=scopes
                )
            
            flow.run_local_server(port=8080, prompt='consent', 
                                authorization_prompt_message='')
            
            credentials = flow.credentials
            
            with open('token.pkl', 'wb') as token_file:
                pickle.dump(credentials, token_file)
    
    # CREATES YOUTUBE OBJECT
    youtube = build('youtube', 'v3', credentials=credentials)
    return youtube


def get_all_subscriptions(youtube):
    dfs = []
    next_page_token = None

    while True:
        # GET 50 SUBSCRIPTIONS PER TIME
        request = youtube.subscriptions().list(part='snippet', mine=True, 
                                            maxResults=50, pageToken=next_page_token)
        response = request.execute()
        df = pd.json_normalize(response['items'])
        dfs.append(df)
        # CHECK IF THERE IS A PAGE MORE
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    # AGGREGATE DATA
    subscriptions = pd.concat(dfs)
    subscriptions.reset_index(inplace=True)
    subscriptions.to_csv('./data/youtube_subscriptions.csv', index=False)


def treat_category_data(dataframe, column):
    # FORMATTING CATEGORY TOPICS
    dataframe[column].fillna('', inplace=True)
    
    dataframe_exploded = dataframe.explode(column)
    
    dataframe_exploded[column] = dataframe_exploded[column].apply(
        lambda x: x.replace('https://en.wikipedia.org/wiki/', ''))
    
    return dataframe_exploded


def get_topic_categories(youtube, channels_file, cataegories_file):
    channels = pd.read_csv(channels_file)
    ids = channels['snippet.resourceId.channelId'].to_list()
    start = 0
    stop = 50
    ids_to_retrieve = ids[start:stop]

    dfs = []
    
    # ITERATES TO RETRIEVE DATA FROM ALL CHANNELS DUE TO RESQUEST LIMIT
    while len(ids_to_retrieve) > 0:
        request = youtube.channels().list(part='topicDetails', id=ids_to_retrieve,
                                        maxResults=50)
        response = request.execute()

        categories_dict = {'id':[], 
                        'categories':[]
                        }
        # EXTRACT RELEVANT DATA
        for element in response['items']:
            id = element['id']
            try:
                categories = element.get('topicDetails').get('topicCategories')
            except:
                categories = None
        
            categories_dict['id'].append(id)
            categories_dict['categories'].append(categories)

        df = pd.DataFrame(categories_dict)
        dfs.append(df)
            
        start += 50
        stop += 50
        ids_to_retrieve = ids[start:stop]

    # AGGREGATES DATA AND STORES IT
    channels_categories = pd.concat(dfs)
    channels_categories.reset_index(inplace=True)

    channels_categories = treat_category_data(
        dataframe=channels_categories, 
        column='categories'
    )

    channels_categories.to_csv(cataegories_file, index=False)
    

if __name__ == '__main__':  
    youtube = get_authentication(scopes=SCOPES)

    get_all_subscriptions(youtube=youtube)

    get_topic_categories(youtube=youtube, channels_file='./data/youtube_subscriptions.csv',
                        cataegories_file='./data/channels_categories.csv')