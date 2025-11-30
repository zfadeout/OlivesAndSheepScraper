# youtube_api.py
# Flask API server to serve YouTube video metadata from scraped data

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional

from flask import Flask, jsonify, request
from flask_cors import CORS

# ------------ Config -------------
OUTPUT_ROOT_DIR = Path("output/youtube_videos")
VIDEOS_INDEX_FILE = OUTPUT_ROOT_DIR / "VIDEOS_INDEX.json"
API_HOST = "127.0.0.1"
API_PORT = 5001

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)

# ------------ Data Loading Functions -------------
def load_videos_index() -> Dict[str, Dict]:
    """Load all videos from index"""
    if VIDEOS_INDEX_FILE.exists():
        try:
            with open(VIDEOS_INDEX_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load videos index: {e}")
            return {}
    return {}

def load_video_metadata(video_id: str) -> Optional[Dict]:
    """Load detailed metadata for a specific video"""
    video_dir = OUTPUT_ROOT_DIR / video_id
    
    try:
        if video_dir.exists():
            # Find the first .json file in the directory (should be the titled one)
            json_files = list(video_dir.glob("*.json"))
            if json_files:
                metadata_file = json_files[0]  # Get the first json file
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load metadata for video {video_id}: {e}")
    
    return None

def format_video_for_api(video_id: str, index_data: Dict) -> Dict:
    """Format video data for API response"""
    # Load full metadata
    full_metadata = load_video_metadata(video_id)
    
    video = {
        "id": video_id,
        "title": index_data.get("title", "Unknown"),
        "url": index_data.get("url", ""),
        "date": index_data.get("date") or "Not Available",
        "place": index_data.get("place") or "Not Applicable",
        "topic": index_data.get("topic") or "Not Applicable",
        "scrapedAt": index_data.get("scraped_at", "")
    }
    
    # Add full description if available
    if full_metadata:
        video["description"] = full_metadata.get("extracted_metadata", {}).get("description", "Not Applicable")
        video["basicInfo"] = full_metadata.get("basic_info", {})
    else:
        video["description"] = "Not Applicable"
        video["basicInfo"] = {}
    
    return video

# ------------ API Routes -------------

@app.route('/api/videos', methods=['GET'])
def get_videos():
    """Get all videos with optional filtering"""
    try:
        videos_index = load_videos_index()
        
        # Apply filters from query params
        search_query = request.args.get('search', '').lower()
        place_filter = request.args.get('place', '').lower()
        topic_filter = request.args.get('topic', '').lower()
        limit = request.args.get('limit', '1000', type=int)
        
        # Format all videos
        formatted_videos = []
        for video_id, video_data in videos_index.items():
            formatted_video = format_video_for_api(video_id, video_data)
            formatted_videos.append(formatted_video)
        
        # Apply filters
        filtered_videos = formatted_videos
        
        if search_query:
            filtered_videos = [
                v for v in filtered_videos
                if search_query in v['title'].lower() or search_query in v.get('description', '').lower()
            ]
        
        if place_filter:
            filtered_videos = [
                v for v in filtered_videos
                if place_filter in v.get('place', '').lower()
            ]
        
        if topic_filter:
            filtered_videos = [
                v for v in filtered_videos
                if topic_filter in v.get('topic', '').lower()
            ]
        
        # Apply limit
        filtered_videos = filtered_videos[:limit]
        
        return jsonify({
            "success": True,
            "count": len(filtered_videos),
            "total": len(formatted_videos),
            "videos": filtered_videos
        })
        
    except Exception as e:
        logging.error(f"Error in get_videos: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/videos/<video_id>', methods=['GET'])
def get_video(video_id):
    """Get detailed metadata for a specific video"""
    try:
        videos_index = load_videos_index()
        
        if video_id not in videos_index:
            return jsonify({
                "success": False,
                "error": f"Video with ID {video_id} not found"
            }), 404
        
        full_metadata = load_video_metadata(video_id)
        
        if not full_metadata:
            return jsonify({
                "success": False,
                "error": f"Metadata for video {video_id} not found"
            }), 404
        
        video = {
            "id": video_id,
            "videoIdentification": full_metadata.get("video_identification", {}),
            "basicInfo": full_metadata.get("basic_info", {}),
            "extractedMetadata": full_metadata.get("extracted_metadata", {}),
            "metadata": full_metadata.get("metadata", {})
        }
        
        return jsonify({
            "success": True,
            "video": video
        })
        
    except Exception as e:
        logging.error(f"Error in get_video: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/search', methods=['GET'])
def search_videos():
    """Search videos by title, description, place, or topic"""
    try:
        query = request.args.get('q', '').lower()
        field = request.args.get('field', 'all')  # all, title, description, place, topic
        limit = request.args.get('limit', '50', type=int)
        
        if not query:
            return jsonify({
                "success": False,
                "error": "Search query required"
            }), 400
        
        videos_index = load_videos_index()
        results = []
        
        for video_id, video_data in videos_index.items():
            formatted_video = format_video_for_api(video_id, video_data)
            
            match = False
            
            if field == 'all':
                match = (
                    query in formatted_video['title'].lower() or
                    query in formatted_video.get('description', '').lower() or
                    query in formatted_video.get('place', '').lower() or
                    query in formatted_video.get('topic', '').lower()
                )
            elif field == 'title':
                match = query in formatted_video['title'].lower()
            elif field == 'description':
                match = query in formatted_video.get('description', '').lower()
            elif field == 'place':
                match = query in formatted_video.get('place', '').lower()
            elif field == 'topic':
                match = query in formatted_video.get('topic', '').lower()
            
            if match:
                results.append(formatted_video)
        
        results = results[:limit]
        
        return jsonify({
            "success": True,
            "query": query,
            "field": field,
            "count": len(results),
            "results": results
        })
        
    except Exception as e:
        logging.error(f"Error in search_videos: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        videos_index = load_videos_index()
        
        # Calculate statistics
        places = {}
        topics = {}
        total_videos = len(videos_index)
        
        for video_id, video_data in videos_index.items():
            place = video_data.get('place', 'Unknown')
            topic = video_data.get('topic', 'Unknown')
            
            if place and place != 'Not Applicable':
                places[place] = places.get(place, 0) + 1
            
            if topic and topic != 'Not Applicable':
                topics[topic] = topics.get(topic, 0) + 1
        
        return jsonify({
            "success": True,
            "stats": {
                "totalVideos": total_videos,
                "byPlace": places,
                "byTopic": topics,
                "uniquePlaces": len(places),
                "uniqueTopics": len(topics)
            }
        })
        
    except Exception as e:
        logging.error(f"Error in get_stats: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/places', methods=['GET'])
def get_places():
    """Get list of all places mentioned in videos"""
    try:
        videos_index = load_videos_index()
        
        places = set()
        for video_data in videos_index.values():
            place = video_data.get('place')
            if place and place != 'Not Applicable':
                places.add(place)
        
        places = sorted(list(places))
        
        return jsonify({
            "success": True,
            "count": len(places),
            "places": places
        })
        
    except Exception as e:
        logging.error(f"Error in get_places: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/topics', methods=['GET'])
def get_topics():
    """Get list of all topics mentioned in videos"""
    try:
        videos_index = load_videos_index()
        
        topics = set()
        for video_data in videos_index.values():
            topic = video_data.get('topic')
            if topic and topic != 'Not Applicable':
                topics.add(topic)
        
        topics = sorted(list(topics))
        
        return jsonify({
            "success": True,
            "count": len(topics),
            "topics": topics
        })
        
    except Exception as e:
        logging.error(f"Error in get_topics: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    videos_index = load_videos_index()
    
    return jsonify({
        "success": True,
        "status": "healthy",
        "message": "YouTube Videos API is running",
        "videosLoaded": len(videos_index)
    })

# ------------ Main -------------
if __name__ == '__main__':
    logging.info("Starting YouTube Videos API Server...")
    logging.info(f"Loading videos from: {OUTPUT_ROOT_DIR.absolute()}")
    
    # Test load videos on startup
    videos_index = load_videos_index()
    logging.info(f"API ready with {len(videos_index)} videos")
    
    app.run(
        host=API_HOST,
        port=API_PORT,
        debug=True
    )