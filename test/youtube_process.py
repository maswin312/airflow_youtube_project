import json
import isodate


def data_processing(source_file_path: str, target_file_path: str):
    """Processes the raw data fetched from YouTube.

    Args:
        source_file_path: A string representing the path to the file to be processed.
        target_file_path: A string representing the path to the file to be written.
    """
    # Load the fetched videos data from the json file
    with open(source_file_path, "r") as f:
        videos_list = json.load(f)

    # Load the categories dictionary from the json file
    with open("dags/categories.json", "r") as f:
        categories = json.load(f)

    # Process the fetched videos data
    for video in videos_list:
        # Convert ISO 8601 duration to seconds
        video["durationSec"] = (
            int(isodate.parse_duration(video["duration"]).total_seconds())
            if video["duration"] is not None
            else None
        )
        del video["duration"]

        # Convert tags list to string
        video["tags"] = ", ".join(video["tags"]) if video["tags"] is not None else None

        # Convert categoryId to category based on categories dictionary
        video["category"] = (
            categories.get(video["categoryId"], None)
            if video["categoryId"] is not None
            else None
        )
        del video["categoryId"]

        # Parse the thumbnail url
        video["thumbnailUrl"] = (
            video["thumbnails"].get("standard", {}).get("url", None)
            if video["thumbnails"] is not None
            else None
        )
        del video["thumbnails"]

        # Convert viewCount, likeCount, and commentCount to integer
        video["viewCount"] = (
            int(video["viewCount"]) if video["viewCount"] is not None else None
        )
        video["likeCount"] = (
            int(video["likeCount"]) if video["likeCount"] is not None else None
        )
        video["commentCount"] = (
            int(video["commentCount"]) if video["commentCount"] is not None else None
        )

        # Convert caption to boolean
        video["caption"] = (
            True
            if video["caption"] == "true"
            else False
            if video["caption"] == "false"
            else None
        )

    # Save the processed videos data to a new file
    with open(target_file_path, "w") as f:
        json.dump(videos_list, f)


data_processing("tmp_file.json", "tmp_file_processed.json")
