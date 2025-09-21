import pandas as pd
import requests
import time
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googlesearch import search

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
VIDEO_IDS = os.getenv("VIDEO_IDS", "dQw4w9WgXcQ").split(",")
TOP_N = int(os.getenv("TOP_N", "10"))

youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

def get_video_comments(video_id):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response['items']:
            snippet = item['snippet']['topLevelComment']['snippet']
            commenter = {
                'YouTube DisplayName': snippet.get('authorDisplayName', 'Unknown'),
                'Channel URL': f"https://www.youtube.com/channel/{snippet['authorChannelId']['value']}",
                'Comment(s)': snippet.get('textDisplay', ''),
                'Comment Likes': snippet.get('likeCount', 0)
            }
            comments.append(commenter)
    except HttpError as e:
        print(f"⚠️ Error fetching comments for {video_id}: {e}")
    return comments

print("🔍 Fetching YouTube comments...")
all_comments = []
for vid in VIDEO_IDS:
    vid = vid.strip()
    print(f" → Processing video: {vid}")
    all_comments.extend(get_video_comments(vid))
    time.sleep(1)

if not all_comments:
    print("❌ No comments fetched. Check API key or video IDs.")
    exit(1)

df_raw = pd.DataFrame(all_comments)
df_raw.to_csv("youtube_commenters_raw.csv", index=False, encoding='utf-8')
print(f"✅ Saved {len(df_raw)} raw comments")

df_ranked = df_raw.groupby(['YouTube DisplayName', 'Channel URL']).agg({
    'Comment(s)': lambda x: ' || '.join(x),
    'Comment Likes': 'sum'
}).reset_index()

df_ranked['Total Comments'] = df_raw.groupby(['YouTube DisplayName']).size().values
df_ranked = df_ranked.sort_values(['Total Comments', 'Comment Likes'], ascending=False)
df_ranked.to_csv("youtube_commenters_ranked.csv", index=False, encoding='utf-8')
print(f"✅ Saved {len(df_ranked)} ranked commenters")

def find_instagram_profile(display_name):
    if not display_name or display_name == "Unknown":
        return "", "Invalid name"
    query = f'site:instagram.com "{display_name}"'
    try:
        for url in search(query, num_results=3, sleep_interval=2):
            if "instagram.com" in url and "/p/" not in url and "/tv/" not in url and "/reel/" not in url:
                return url, "Google site: search"
    except Exception as e:
        print(f"⚠️ Search error for {display_name}: {e}")
    return "", "Not found"

print("🕵️ Searching Instagram profiles...")
top_candidates = df_ranked.head(TOP_N).copy()
results = []

for _, row in top_candidates.iterrows():
    print(f" → Searching IG for: {row['YouTube DisplayName']}")
    insta_url, method = find_instagram_profile(row['YouTube DisplayName'])
    results.append({
        'YouTube DisplayName': row['YouTube DisplayName'],
        'Channel URL': row['Channel URL'],
        'Comment(s)': row['Comment(s)'],
        'Instagram Profile': insta_url,
        'Verification method': method
    })
    time.sleep(2)

df_instagram = pd.DataFrame(results)
df_instagram.to_csv("top_candidates_with_instagram.csv", index=False, encoding='utf-8')
print(f"✅ Saved {len(df_instagram)} top candidates with Instagram")

print("\n🎉 PROTOTYPE COMPLETE!")
