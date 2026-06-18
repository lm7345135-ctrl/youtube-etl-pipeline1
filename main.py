import os
import json
import datetime
from googleapiclient.discovery import build
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

# ==========================================
# GitHub Secretsから環境変数を読み込み
# ==========================================
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET")       # 例: youtube_data
BQ_TABLE = os.environ.get("BQ_TABLE")           # 例: raw_comments
TARGET_CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_recent_videos(channel_id, max_videos=50):
    """チャンネルの隠しプレイリスト(Uploads)から最新50本の動画IDとタイトルを取得"""
    uploads_playlist_id = "UU" + channel_id[2:]
    videos = []
    
    # ページ送りをせず、最新の max_videos 件のみを取得
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=uploads_playlist_id,
        maxResults=max_videos
    )
    response = request.execute()
    
    for item in response.get("items", []):
        videos.append({
            "video_id": item["contentDetails"]["videoId"],
            "video_title": item["snippet"]["title"]
        })
        
    print(f"✅ 最新の対象動画を {len(videos)} 件特定しました。")
    return videos

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_comments_for_video(video_info, channel_id, scraped_at):
    """指定動画の親コメントをページ送りで全件取得"""
    video_id = video_info["video_id"]
    video_title = video_info["video_title"]
    
    comments = []
    next_page_token = None
    
    while True: # ページ送りの無限ループ
        try:
            # commentThreads APIで親コメントのみをターゲットにする
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                textFormat="plainText",
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                comment_data = {
                    "comment_id": item["id"],
                    "video_id": video_id,
                    "video_title": video_title,
                    "channel_id": channel_id,
                    "author_channel_id": snippet.get("authorChannelId", {}).get("value", "UNKNOWN"),
                    "text_original": snippet["textDisplay"],
                    "like_count": snippet["likeCount"],
                    "reply_count": item["snippet"]["totalReplyCount"],
                    "published_at": snippet["publishedAt"],
                    "scraped_at": scraped_at
                }
                comments.append(comment_data)
                
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break # 続きの切符がなければ全件取得完了としてループを抜ける
                
        except Exception as e:
            print(f"⚠️ 動画 {video_id} のコメント取得をスキップしました: {e}")
            break
            
    return comments

def load_local_file_to_bigquery(file_path, dataset_id, table_id):
    """ローカルのJSONLファイルをBigQueryへ直接ロード"""
    table_ref = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    print(f"⏳ BigQueryへのロードを開始します: {table_ref}")
    with open(file_path, "rb") as source_file:
        load_job = bq_client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
    load_job.result()
    
    table = bq_client.get_table(table_ref)
    print(f"✅ BigQueryへのロード完了。現在のテーブル総件数: {table.num_rows} 件")

def run_bigquery_queries():
    print("⏳ BigQuery内でデータの仕分け(クレンジング)を開始します...")
    
    # クエリA：コメントマスターの再作成 (★ORDER BY scraped_at DESC に修正済)
    query_a = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_comments` AS
    SELECT
      comment_id, video_id, video_title, author_channel_id,
      LOWER(REGEXP_REPLACE(text_original, r'<[^>]+>', '')) AS text_normalized,
      text_original, TIMESTAMP(published_at) AS published_at,
      MIN(TIMESTAMP(scraped_at)) OVER(PARTITION BY comment_id) AS first_scraped_at
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE text_original IS NOT NULL AND TRIM(text_original) != ''
      AND REGEXP_CONTAINS(text_original, r'[ぁ-んァ-ヶ一-龠a-zA-Z0-9!?！？。、ｗｗwW]')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY comment_id ORDER BY scraped_at DESC) = 1;
    """
    
    # クエリB：いいね履歴の再作成
    query_b = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.fct_like_history` AS
    SELECT
      TIMESTAMP(scraped_at) AS checked_at, comment_id,
      CAST(like_count AS INT64) AS like_count, CAST(reply_count AS INT64) AS reply_count
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE comment_id IS NOT NULL;
    """
    
    bq_client.query(query_a).result()
    print("✅ クエリA (dim_comments) の作成完了")
    bq_client.query(query_b).result()
    print("✅ クエリB (fct_like_history) の作成完了")

def main():
    print("🚀 YouTube ETL パイプライン (最新50本・親コメント全件版) を起動します...")
    scraped_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    # 最新50本をターゲットに設定
    video_infos = get_recent_videos(TARGET_CHANNEL_ID, max_videos=50)
    
    all_comments = []
    for idx, v_info in enumerate(video_infos, 1):
        print(f"[{idx}/{len(video_infos)}] 動画 {v_info['video_id']} のコメントを取得中...")
        comments = get_comments_for_video(v_info, TARGET_CHANNEL_ID, scraped_at)
        all_comments.extend(comments)
        
    print(f"📊 合計 {len(all_comments)} 件の親コメントを抽出しました。")
    
    if all_comments:
        temp_file_path = "temp_comments.jsonl"
        with open(temp_file_path, "w", encoding="utf-8") as f:
            for record in all_comments:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        load_local_file_to_bigquery(temp_file_path, BQ_DATASET, BQ_TABLE)
        os.remove(temp_file_path)
    
    # データ保存が終わった直後に仕分けを実行する
    run_bigquery_queries()
            
    print("🎉 全てのバッチ処理が正常に完了しました。")

if __name__ == "__main__":
    main()
