from dotenv import load_dotenv
import os
from googleapiclient.discovery import build
import json
from datetime import datetime


def fetch_trending_videos(region_code: str, max_results: int, target_file_path: str):
    """Fetches trending videos from YouTube for a specific region.

    Args:
        region_code: A string representing the ISO 3166-1 alpha-2 country code for the desired region.
        max_results: An integer representing the maximum number of results to fetch.
        target_file_path: A string representing the path to the file to be written.
    """

    # Load API key from .env file
    load_dotenv("dags/.env")
    api_key = os.environ.get("YOUTUBE_API_KEY")

    # Create YouTube API client
    youtube = build("youtube", "v3", developerKey=api_key)

    # Fetch videos until max_results is reached or there are no more results
    videos_list = []
    next_page_token = ""
    while len(videos_list) < max_results and next_page_token is not None:
        # Make API request for videos
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=50,
            pageToken=next_page_token,
        )
        response = request.execute()

        # Extract videos from response
        videos = response.get("items", [])

        # Update next_page_token for the next API request
        next_page_token = response.get("nextPageToken", None)

        # Extract relevant video details and append to list
        infos = {
            "snippet": [
                "title",
                "publishedAt",
                "channelId",
                "channelTitle",
                "description",
                "tags",
                "thumbnails",
                "categoryId",
                "defaultAudioLanguage",
            ],
            "contentDetails": ["duration", "caption"],
            "statistics": ["viewCount", "likeCount", "commentCount"],
        }
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        for video in videos:
            video_details = {"videoId": video["id"], "trendingAt": now}

            for k in infos.keys():
                for info in infos[k]:
                    # use try-except to handle missing info
                    try:
                        video_details[info] = video[k][info]
                    except KeyError:
                        video_details[info] = None

            videos_list.append(video_details)

    # Write fetched videos data to a json file
    with open(target_file_path, "w") as f:
        json.dump(videos_list, f)


fetch_trending_videos("ID", 200, "tmp_file.json")
