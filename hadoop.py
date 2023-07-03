# Instalar dependencias
import os
import json
import tweepy
import pandas as pd

# Definir credenciales
consumer_key = ''
consumer_secret = ''
baerer_token = "
access_token = ''
access_token_secret = ''

# Autenticar
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

cliente = tweepy.Client(baerer_token, consumer_key, consumer_secret,
                        access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(
    consumer_key, consumer_secret, access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# Crear objeto API

# Buscar tweets
tweets = cliente.search_recent_tweets(query="colombia -RT ", tweet_fields=[
    'author_id', 'public_metrics', 'created_at', 'source'],
    user_fields=[
    "name", "username", "location", "description"],
    max_results=100, expansions='author_id')

# Almacenar los tweets
# data = [for tweet in tweets]
# js_data = json.dumps(data)
tweet_info_ls = []
tlt = {}
# Transformacion
for tweet, user in zip(tweets.data, tweets.includes['users']):
    tweet_info = {
        'author_id': tweet.author_id,
        # 'created_at': tweet.created_at,
        'text': tweet.text,
        'source': tweet.source,
        'name': user.name,
        'username': user.username,
        'location': user.location,
        'description': user.description
    }
    tweet_info_ls.append(tweet_info)
    tweets_df = pd.DataFrame(tweet_info_ls)
    result = tweets_df.to_json(orient="split")
    tweets_df.head()

data = tweet_info_ls
print(data)

# Subir los tweets a Hadoop
os.system("hdfs dfs -mkdir -p /apache_hadoop/twitter/hdfs/miller")
os.system("hdfs dfs -put -f /data/tweets.json /apache_hadoop/twitter/hdfs/miller")

# Guardar los tweets en un archivo
with open('/data/tweets.json', 'w', encoding='utf-8') as outfile:
    outfile.write(json.dumps(data, indent=4))

print('Tweets almacenados satisfactoriamente')

