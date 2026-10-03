import os
import json
import datetime
from googleapiclient.discovery import build
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential
from deep_translator import GoogleTranslator  # Added: Translation library

# ==========================================
# Load environment variables from GitHub Secrets
# ==========================================
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET")       # Example: youtube_data
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID")
BRAND_NAME = os.environ.get("BRAND_NAME")       # 'UMAMUSUME' or 'JAL'

# Dynamically determine the table name
BQ_TABLE = f"raw_comments_{BRAND_NAME.lower()}"

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

# Added: Initialize translation instance
translator = GoogleTranslator(source='auto', target='en')

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_recent_videos(channel_id, max_videos=50):
    uploads_playlist_id = "UU" + channel_id[2:]
    videos = []
    request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=uploads_playlist_id, maxResults=max_videos)
    response = request.execute()
    for item in response.get("items", []):
        raw_title = item["snippet"]["title"]
        
        # English translation process for video titles
        try:
            en_title = translator.translate(raw_title) if raw_title else ""
        except Exception as e:
            print(f"⚠️ Title translation error: {e}")
            en_title = raw_title

        videos.append({"video_id": item["contentDetails"]["videoId"], "video_title": en_title})
    return videos

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_comments_for_video(video_info, channel_id, scraped_at):
    video_id = video_info["video_id"]
    comments = []
    next_page_token = None
    while True:
        try:
            # ★Safety mechanism: Catch errors here so the process doesn't stop
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=100, textFormat="plainText", pageToken=next_page_token)
            response = request.execute()
            for item in response.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                raw_text = snippet.get("textDisplay", "")
                
                # English translation process for comment text
                try:
                    en_text = translator.translate(raw_text) if raw_text.strip() else ""
                except Exception as e:
                    print(f"⚠️ Comment translation error: {e}")
                    en_text = raw_text

                comment_data = {
                    "comment_id": item["id"],
                    "video_id": video_id,
                    "video_title": video_info["video_title"],
                    "channel_id": channel_id,
                    "brand": BRAND_NAME,
                    "author_channel_id": snippet.get("authorChannelId", {}).get("value", "UNKNOWN"),
                    "text_english": en_text,  # Changed text_original to text_english
                    "like_count": snippet["likeCount"],
                    "reply_count": item["snippet"]["totalReplyCount"],
                    "published_at": snippet["publishedAt"],
                    "scraped_at": scraped_at
                }
                comments.append(comment_data)
            next_page_token = response.get("nextPageToken")
            if not next_page_token: break
            
        except Exception as e:
            print(f"⚠️ Skipped comment extraction for video {video_id}: {e}")
            break # If an error occurs, give up on this video and proceed to the next
            
    return comments

def load_local_file_to_bigquery(file_path, dataset_id, table_id):
    table_ref = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    with open(file_path, "rb") as source_file:
        bq_client.load_table_from_file(source_file, table_ref, job_config=job_config).result()
    print(f"✅ Successfully loaded into {table_ref}")

def run_bigquery_queries():
    print("⏳ Starting data sorting (cleansing) in BigQuery...")
    
    # Changed text_original to text_english in query_a and removed Japanese filters
    query_a = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_comments_{BRAND_NAME.lower()}` AS
    SELECT
      comment_id, video_id, video_title, author_channel_id, brand,
      LOWER(REGEXP_REPLACE(text_english, r'<[^>]+>', '')) AS text_normalized,
      text_english, TIMESTAMP(published_at) AS published_at,
      MIN(TIMESTAMP(scraped_at)) OVER(PARTITION BY comment_id) AS first_scraped_at
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE text_english IS NOT NULL AND TRIM(text_english) != ''
    QUALIFY ROW_NUMBER() OVER(PARTITION BY comment_id ORDER BY scraped_at DESC) = 1;
    """
    
    query_b = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.fct_like_history_{BRAND_NAME.lower()}` AS
    SELECT
      TIMESTAMP(scraped_at) AS checked_at, comment_id, brand,
      CAST(like_count AS INT64) AS like_count, CAST(reply_count AS INT64) AS reply_count
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE comment_id IS NOT NULL;
    """
    
    bq_client.query(query_a).result()
    print(f"✅ Creation of Query A (dim_comments_{BRAND_NAME.lower()}) completed")
    bq_client.query(query_b).result()
    print(f"✅ Creation of Query B (fct_like_history_{BRAND_NAME.lower()}) completed")

def main():
    scraped_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    video_infos = get_recent_videos(TARGET_CHANNEL_ID, max_videos=50)
    all_comments = []
    
    for idx, v_info in enumerate(video_infos, 1):
        print(f"[{idx}/{len(video_infos)}] Processing video {v_info['video_id']}...")
        all_comments.extend(get_comments_for_video(v_info, TARGET_CHANNEL_ID, scraped_at))
    
    if all_comments:
        temp_file = "temp_comments.jsonl"
        with open(temp_file, "w", encoding="utf-8") as f:
            for record in all_comments: f.write(json.dumps(record, ensure_ascii=False) + "\n")
        load_local_file_to_bigquery(temp_file, BQ_DATASET, BQ_TABLE)
        os.remove(temp_file)
        
        # 🌟 Key point: Perform cleansing process only when there are comments!
        run_bigquery_queries()
        print("🎉 All batch processes completed successfully.")
    else:
        print("⚠️ 0 comments were fetched this time. Skipping saving to BigQuery and organizing, then exiting.")

if __name__ == "__main__":
    main()
