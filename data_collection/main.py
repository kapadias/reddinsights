# import dependent packages
import praw
import pandas as pd
import json
from google.cloud import storage
import time
import multiprocessing.pool as mpool


def _establish_connection():
    """establish reddit API connection using credentials from
    config.text file

    :return: reddit object
    """
    with open('config.text') as json_file:
        data = json.load(json_file)

    # reddit API connection
    reddit = praw.Reddit(client_id=data['client_id'],
                         client_secret=data['client_secret'],
                         user_agent=data['user_agent'],
                         username=data['username'],
                         password=data['password'])
    return reddit


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket

    :param bucket_name: STRING
    :param source_file_name: STRING
    :param destination_blob_name: STRING
    :return: None
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def _stream_content(reddit, subreddit):
    """stream the posting from the input subreddit

    :param reddit:
    :param subreddit:
    :return:
    """
    topics_dict = {'submission_id': [],
                   'user_id': [],
                   'submission_timestamp': [],
                   'title': [],
                   'body': []}

    # assume you have a Reddit instance bound to variable `reddit`
    subreddit = reddit.subreddit(subreddit)

    # initialize the counter
    count = 0
    try:
        for submission in subreddit.stream.submissions():
            topics_dict['submission_id'].append(submission.id)
            topics_dict['user_id'].append(submission.author)
            topics_dict['submission_timestamp'].append(submission.created_utc)
            topics_dict['title'].append(submission.title.encode('utf-8').strip())
            topics_dict['body'].append(submission.selftext.encode('utf-8').strip())

            # save blob of 50 posts at a time
            if count == 50:
                pd.DataFrame(topics_dict).to_csv('./temp_blob_' + subreddit + '.csv',
                                                 index=False, doublequote=True, sep='|')
                upload_blob('raw_dataset', 'temp_blob.csv',
                            'reddit/raw_posts/reddit_' + subreddit + '_post_stream_' + str(int(time.time())) + '.csv')
                count = 0

            count += 1
    except:
        pass


def launch(sub_reddits):
    """

    :return:
    """
    # get the reddit object
    reddit = _establish_connection()

    # initialize and execute parallel processes for each subreddit
    pool = mpool.ThreadPool(3)
    for sub_reddit in sub_reddits:
        pool.apply_async(_stream_content,
                         args=(reddit, sub_reddit))
    pool.close()
    pool.join()


if __name__ == '__main__':

    sub_reddits = ['AskReddit', 'datascience', 'galaxys10']
    launch(sub_reddits)
