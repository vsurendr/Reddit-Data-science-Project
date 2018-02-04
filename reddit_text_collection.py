
"""
reddit_text_collection.py
Collects Reddit comments text using the Reddit API wrapper PRAW 
and stores data in MongoDB database.
"""

import praw
import pickle
import pymongo
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.reddit_database
collection_top = db.reddit_top
collection_contro = db.reddit_contro

r = praw.Reddit(user_agent="Chrome:Reddit Project (by /u/vsurendr)")

# Collect flattened tree of Reddit comments labeled as 'top'.
count_top = 0
submissions_top = r.get_top(limit=1000)
for post in submissions_top:
    count_top += 1
    
    # Flatten the comment tree
    post.replace_more_comments(limit=20, threshold=1)
    
    # Sort collected posts by 'score' or number of upvotes minus downvotes
    comment_list = []
    for comment in sorted(post.comments, key=lambda x: x.score, reverse=True):
        comment_list.append([comment.body, comment.score])  

    # Add comment text and metadata in MongoDB database
    document = {'title': post.title,
                'url': post.url, 
                'comments': comment_list}
    collection_top.insert(document)                  


# Collect flattened tree of Reddit comments labeled as 'controversial'.
count_contro = 0
submissions_contro = r.get_controversial(limit=210)
for post in submissions_contro:
    count_contro += 1
    
    # Flatten the comment tree
    post.replace_more_comments(limit=20, threshold=1)
    
    # Sort collected comments by 'score' or number of upvotes minus downvotes
    comment_list = []
    for comment in sorted(post.comments, key=lambda x: x.score, reverse=True):
        comment_list.append([comment.body, comment.score])  

    # Add comment text and metadata in MongoDB database
    document = {'title': post.title,
                'url': post.url, 
                'comments': comment_list}
    collection_contro.insert(document) 

