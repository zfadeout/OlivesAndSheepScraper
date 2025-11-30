# youtube_scraper.py
# Advanced YouTube scraper for specific channels with video metadata extraction
# Extracts: Title, Date, Place, Topic, Description from YouTube videos
# IMPROVEMENTS: Better description scraping, more comprehensive topic/location detection

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ------------ Config -------------
YOUTUBE_BASE_URL = "https://www.youtube.com"

# Target channels to scrape - Regular videos
TARGET_CHANNELS = [
    "https://www.youtube.com/@greenolivetours5614/videos",
    "https://www.youtube.com/@rabbisforhumanrights4516/videos",
    "https://www.youtube.com/@Zochrot/videos",
    "https://www.youtube.com/@PalestineLandSociety/videos",
    "https://www.youtube.com/@combatantsforpeace5017/videos",
]

# Target channels to scrape - Shorts
TARGET_SHORTS_CHANNELS = [
    "https://www.youtube.com/@rabbisforhumanrights4516/shorts",
    "https://www.youtube.com/@Zochrot/shorts",
    "https://www.youtube.com/@greenolivetours5614/shorts",
]

# Search query for additional content
SEARCH_QUERIES = [
    "https://www.youtube.com/results?search_query=jordan+valley+activists",
]

OUTPUT_ROOT_DIR = Path("output/youtube_videos")
VIDEOS_INDEX_FILE = OUTPUT_ROOT_DIR / "VIDEOS_INDEX.json"
LOGS_DIR = Path("output/logs")

USER_AGENT_STRING = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

PAGE_TIMEOUT_MS = 60_000
DELAY_BETWEEN_VIDEOS_SEC = 0.2
MAX_VIDEOS_PER_CHANNEL = 999999  # Scrape ALL videos
MAX_RETRIES = 3

# ------------ Logging Setup -------------
def setup_logging() -> Path:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file_path = LOGS_DIR / f"youtube_scraper-{timestamp}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("Logging to %s", log_file_path)
    return log_file_path

# ------------ Index helpers -------------
def load_videos_index() -> Dict[str, dict]:
    """Load existing videos index from file"""
    if VIDEOS_INDEX_FILE.exists():
        try:
            return json.loads(VIDEOS_INDEX_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logging.warning("Failed to read index (%s). Starting fresh.", e)
    return {}

def save_videos_index(index_data: Dict[str, dict]) -> None:
    """Save videos index to file"""
    VIDEOS_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    VIDEOS_INDEX_FILE.write_text(
        json.dumps(index_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def save_video_metadata(video_id: str, metadata: Dict) -> Path:
    """Save individual video metadata to JSON file using title as filename"""
    # Use title to create filename, fall back to video_id if no title
    title = metadata.get('video_identification', {}).get('title', video_id)
    filename = sanitize_filename(title)
    
    # Ensure filename isn't too long and doesn't conflict
    if len(filename) > 100:
        filename = filename[:100]
    
    # Determine content type for folder organization
    content_type = metadata.get('video_identification', {}).get('content_type', 'video')
    
    # Create directory structure: OUTPUT_ROOT_DIR / {content_type}s / {video_id}
    if content_type == 'shorts':
        type_folder = OUTPUT_ROOT_DIR / "shorts"
    else:
        type_folder = OUTPUT_ROOT_DIR / "videos"
    
    video_dir = type_folder / video_id
    video_dir.mkdir(parents=True, exist_ok=True)
    
    metadata_file = video_dir / f"{filename}.json"
    
    # If file already exists with different naming, remove old ones
    for old_file in video_dir.glob("*.json"):
        if old_file != metadata_file:
            try:
                old_file.unlink()
            except:
                pass
    
    metadata_file.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    return metadata_file

# ------------ Extraction helpers -------------
def extract_video_id_from_href(href: str) -> Optional[str]:
    """Extract video ID from YouTube href"""
    match = re.search(r'v=([a-zA-Z0-9_-]{11})', href)
    if match:
        return match.group(1)
    return None

def extract_video_id_from_url(url: str) -> Optional[str]:
    """Extract video ID from full YouTube URL"""
    patterns = [
        r'(?:youtube\.com\/watch\?v=|youtu\.be\/)([a-zA-Z0-9_-]{11})',
        r'v=([a-zA-Z0-9_-]{11})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def sanitize_filename(name: str) -> str:
    """Sanitize filename for filesystem"""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    return name[:100]

def clean_text(text: str) -> str:
    """Clean extracted text"""
    if not text:
        return ""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_date_from_page_metadata(page) -> Optional[str]:
    """Extract publish date from YouTube page metadata (from watch-info-text or dateText)"""
    try:
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Look for yt-formatted-string with date pattern in watch-info-text
        watch_info = soup.find('ytd-watch-info-text')
        if watch_info:
            formatted_strings = watch_info.find_all('yt-formatted-string')
            for fs in formatted_strings:
                text = fs.get_text(strip=True)
                date_match = extract_date_from_text(text)
                if date_match:
                    return date_match
        
        # Method 2: Look in meta tags
        publish_date_meta = soup.find('meta', {'itemprop': 'uploadDate'})
        if publish_date_meta:
            content = publish_date_meta.get('content', '')
            return content[:10] if content else None
        
        # Method 3: Look for dateText element
        date_text = soup.find('span', {'id': 'date-text'})
        if date_text:
            text = date_text.get_text(strip=True)
            date_match = extract_date_from_text(text)
            if date_match:
                return date_match
        
        return None
    except Exception as e:
        logging.debug(f"Error extracting date from page metadata: {e}")
        return None

def extract_date_from_page_metadata(page) -> Optional[str]:
    """Extract publish date from YouTube page metadata (from watch-info-text or dateText)"""
    try:
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Look for yt-formatted-string with date pattern in watch-info-text
        watch_info = soup.find('ytd-watch-info-text')
        if watch_info:
            formatted_strings = watch_info.find_all('yt-formatted-string')
            for fs in formatted_strings:
                text = fs.get_text(strip=True)
                date_match = extract_date_from_text(text)
                if date_match:
                    return date_match
        
        # Method 2: Look in meta tags
        publish_date_meta = soup.find('meta', {'itemprop': 'uploadDate'})
        if publish_date_meta:
            content = publish_date_meta.get('content', '')
            return content[:10] if content else None
        
        # Method 3: Look for dateText element
        date_text = soup.find('span', {'id': 'date-text'})
        if date_text:
            text = date_text.get_text(strip=True)
            date_match = extract_date_from_text(text)
            if date_match:
                return date_match
        
        return None
    except Exception as e:
        logging.debug(f"Error extracting date from page metadata: {e}")
        return None

def extract_date_from_text(text: str) -> Optional[str]:
    """Try to extract date from text using common patterns"""
    if not text:
        return None
    
    # Common YouTube date patterns
    patterns = [
        r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'(\d{1,2}/\d{1,2}/\d{4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None

def extract_location_from_text(text: str) -> Optional[str]:
    """Extract location from text - improved with priority for Palestinian locations"""
    if not text:
        return None
    
    # Predefined Palestinian and Middle Eastern locations (check first for precision)
    specific_locations = {
        r'\bWest Bank\b': 'West Bank',
        r'\bGaza\s+(?:Strip)?\b': 'Gaza Strip',
        r'\bEast Jerusalem\b': 'East Jerusalem',
        r'\bHebron\b': 'Hebron',
        r'\bRamallah\b': 'Ramallah',
        r'\bBethlehem\b': 'Bethlehem',
        r'\bNablus\b': 'Nablus',
        r'\bJenin\b': 'Jenin',
        r'\bTulkarem\b': 'Tulkarem',
        r'\bQalqilya\b': 'Qalqilya',
        r'\bSalfit\b': 'Salfit',
        r'\bJericho\b': 'Jericho',
        r'\bJordan Valley\b': 'Jordan Valley',
        r'\bArea [AC]\b': 'Area A/C',
        r'\bArea [BC]\b': 'Area B/C',
        r'\bIsrael\b': 'Israel',
        r'\bPalestine\b': 'Palestine',
        r'\bJordan\b': 'Jordan',
    }
    
    # First check for specific locations (highest priority)
    for pattern, location_name in specific_locations.items():
        if re.search(pattern, text, re.IGNORECASE):
            return location_name
    
    # Comprehensive location patterns for other locations
    location_patterns = [
        r'(?:in|from|at|location:?|place:?|vicinity\s+of)\s+([A-Z][a-zA-Z\s]+?)(?:\s*[-–]|\s+(?:where|area|region|valley|district))',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*(?:[A-Z]{2}|Palestine|Israel|Jordan)',
        r'(?:in|from|at)\s+([A-Z][a-z]+(?:\s+(?:Valley|Region|Area|District|Zone|Governorate))?)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Valley|Region|Area|District|Zone|Governorate)',
    ]
    
    for pattern in location_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            location = match.group(1) if match.lastindex else match.group(0)
            location = location.strip()
            if len(location) > 2 and len(location) < 100:
                # Filter out common non-location words
                non_locations = {'said', 'according', 'report', 'video', 'interview', 'footage', 'shows', 'reveals', 'documentary', 'depicts', 'describes', 'documents'}
                if location.lower() not in non_locations:
                    return location
    
    return None

def extract_topic_from_text(text: str) -> Optional[str]:
    """Extract topic with comprehensive keyword coverage - improved for documentaries"""
    if not text:
        return None
    
    # Comprehensive topic patterns with excellent coverage
    topic_keywords = {
        r'(?:settlement|settlement.*expansion|outpost|illegal.*settlement|settler|colonization)': 'Settlement Expansion',
        r'(?:demolition|home.*demolish|building.*demolish|razed|destroyed homes)': 'Home Demolition',
        r'(?:checkpoint|military.*checkpoint|roadblock|access restriction)': 'Checkpoints & Access',
        r'(?:wall|barrier|separation.*wall|concrete wall|perimeter)': 'Separation Wall/Barrier',
        r'(?:protest|demonstration|march|rally|activism|resistance)': 'Protest/Demonstration',
        r'(?:arrest|detention|prisoner|incarcerated|captured|held)': 'Arrest/Detention',
        r'(?:violence|attack|assault|confrontation|clash|shooting|injured)': 'Violence/Confrontation',
        r'(?:refugee|camp|displacement|eviction|expelled|displaced)': 'Refugees/Displacement',
        r'(?:water.*rights?|water.*dispute|water.*scarcity)': 'Water Rights',
        r'(?:land.*dispute|land.*claim|land.*grab|expropriation)': 'Land Dispute',
        r'(?:human.*right|right.*violation|abuse|torture|mistreatment)': 'Human Rights',
        r'(?:documentation|footage|evidence|investigation|documented)': 'Documentation/Investigation',
        r'(?:interview|testimony|account|witness|first-hand)': 'Interview/Testimony',
        r'(?:activist|activism|advocacy|humanitarian)': 'Activism/Advocacy',
        r'(?:palestinian|israeli|middle.*east|palestinian-israeli|conflict)': 'Palestinian-Israeli Conflict',
        r'(?:documentary|doc|expose|investigative|report)': 'Documentary/Investigation',
        r'(?:apartheid|discrimination|segregation|racism)': 'Discrimination/Apartheid',
        r'(?:occupation|military.*occupation|occupied.*territory)': 'Occupation',
        r'(?:tunnel|underground|construction|infrastructure)': 'Infrastructure/Construction',
        r'(?:cultural|heritage|ancient|archaeological|historical.*site)': 'Cultural Heritage',
    }
    
    text_lower = text.lower()
    
    # Find all matching topics
    matched_topics = []
    for pattern, topic in topic_keywords.items():
        if re.search(pattern, text_lower):
            matched_topics.append(topic)
    
    # Return first unique match (already ordered by importance)
    if matched_topics:
        seen = set()
        for topic in matched_topics:
            if topic not in seen:
                return topic
            seen.add(topic)
    
    # Fallback: generic extraction
    topic_patterns = [
        r'(?:topic:?|subject:?|about:?)\s+([^.!?]+)',
        r'^([a-zA-Z\s]+?)(?:\s+(?:in|from|at|where|footage|video|documentary))',
        r'(?:documenting|showing|featuring|depicting)\s+([^.!?]+)',
    ]
    
    for pattern in topic_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            topic = match.group(1).strip()
            if len(topic) > 3 and len(topic) < 150:
                return topic
    
    return None

# ------------ Scraping functions -------------
def is_english_title(text: str) -> bool:
    """Check if title is primarily in English"""
    if not text:
        return False
    
    # Count ASCII characters (English letters, numbers, common punctuation)
    ascii_count = sum(1 for c in text if ord(c) < 128)
    total_count = len(text)
    
    # If more than 60% ASCII characters, consider it English
    return (ascii_count / total_count) > 0.6

def extract_channel_name_from_url(channel_url: str) -> str:
    """Extract channel name from channel URL"""
    try:
        # Pattern: https://www.youtube.com/@channelname/videos or /shorts
        match = re.search(r'@([a-zA-Z0-9_-]+)', channel_url)
        if match:
            return match.group(1)
    except:
        pass
    return "Unknown"

def extract_video_metadata(video_element, browser_page=None, allow_non_english=False, channel_url: str = None) -> Optional[Dict]:
    """Extract metadata from a video element on the videos page"""
    try:
        # Extract title
        title_element = video_element.select_one('a#video-title, h3 yt-formatted-string')
        title = None
        video_href = None
        
        if title_element:
            title = clean_text(title_element.get_text())
            # Try to get video link
            link_elem = video_element.select_one('a[href*="/watch?v="]')
            if link_elem and link_elem.get('href'):
                video_href = link_elem.get('href')
        
        if not title or not video_href:
            return None
        
        # Filter for English titles only (unless allow_non_english is True)
        if not allow_non_english and not is_english_title(title):
            logging.debug(f"Skipping non-English title: {title[:50]}")
            return None
        
        video_id = extract_video_id_from_href(video_href)
        if not video_id:
            return None
        
        # Extract date (from relative time like "2 weeks ago")
        date_element = video_element.select_one('span.inline-metadata-item')
        date_str = None
        if date_element:
            date_str = clean_text(date_element.get_text())
        
        # Extract view count
        view_count = None
        view_elements = video_element.select('span.inline-metadata-item')
        if view_elements:
            view_count = clean_text(view_elements[0].get_text()) if view_elements else None
        
        # Extract channel name from URL
        channel_name = extract_channel_name_from_url(channel_url) if channel_url else "Unknown"
        
        # Build metadata object
        metadata = {
            "video_identification": {
                "video_id": video_id,
                "title": title,
                "href": video_href,
                "full_url": urljoin(YOUTUBE_BASE_URL, video_href) if video_href.startswith('/') else video_href,
                "content_type": "video"
            },
            "basic_info": {
                "date": date_str or "Not Available",
                "view_count": view_count or "Not Available",
                "channel": channel_name
            },
            "extracted_metadata": {
                "title": title,
                "date": None,
                "place": None,
                "topic": None,
                "description": None,
                "channel": channel_name
            },
            "metadata": {
                "source_url": YOUTUBE_BASE_URL,
                "scraped_at": datetime.now().isoformat(),
                "extraction_method": "Playlist Video Page"
            }
        }
        
        return metadata
        
    except Exception as e:
        logging.error(f"Error extracting video metadata: {e}")
        return None

def scrape_video_description(page, video_url: str) -> Optional[str]:
    """Scrape full video description from YouTube's description-inline-expander div"""
    try:
        logging.debug(f"Fetching video page: {video_url}")
        try:
            page.goto(video_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        except Exception as e:
            logging.debug(f"Failed to navigate to {video_url}: {e}")
            return None
        
        try:
            page.wait_for_selector('ytd-watch-metadata, ytd-text-inline-expander', timeout=10000)
        except:
            logging.debug("Description selectors not found, skipping")
            return None
        
        # Try to click "Show more" button
        try:
            page.evaluate("""
                const expandBtn = document.querySelector('#expand');
                if (expandBtn && !expandBtn.hidden) {
                    expandBtn.click();
                }
            """)
            time.sleep(1)
        except:
            pass
        
        try:
            html_content = page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            description = None
            
            # PRIMARY: Read from ytd-text-inline-expander
            logging.debug("Attempting to extract from description-inline-expander")
            description_container = soup.find('ytd-text-inline-expander', {'id': 'description-inline-expander'})
            if description_container:
                logging.debug("Found description-inline-expander container")
                expanded_div = description_container.find('div', {'id': 'expanded'})
                if expanded_div:
                    logging.debug("Found expanded div")
                    attr_string = expanded_div.find('yt-attributed-string', {'user-input': ''})
                    if attr_string:
                        span = attr_string.find('span', {'class': 'yt-core-attributed-string'})
                        if span:
                            description = clean_text(span.get_text())
                            logging.debug(f"Extracted from yt-attributed-string span: {len(description)} chars")
                        else:
                            span = attr_string.find('span')
                            if span:
                                description = clean_text(span.get_text())
                                logging.debug(f"Extracted from generic span: {len(description)} chars")
                            else:
                                description = clean_text(attr_string.get_text())
                                logging.debug(f"Extracted from attr_string text: {len(description)} chars")
            
            # SECONDARY: Try other selectors if primary fails
            if not description or len(description) < 20:
                logging.debug("Primary extraction failed, trying secondary selectors")
                description_selectors = [
                    ('ytd-structured-description-content-renderer', 'Extracted from structured description'),
                    ('yt-attributed-string[user-input]', 'Extracted from attributed string'),
                ]
                
                for selector, desc in description_selectors:
                    if description and len(description) > 20:
                        break
                    try:
                        elements = soup.select(selector)
                        for elem in elements:
                            text = clean_text(elem.get_text())
                            if (text and len(text) > 30 and 
                                'Subscribe' not in text[:50] and 
                                'Share' not in text[:50] and
                                'Videos' not in text[:50] and
                                'About' not in text[:50]):
                                description = text
                                logging.debug(f"{desc}: {len(description)} chars")
                                break
                    except Exception as e:
                        logging.debug(f"Secondary selector failed: {e}")
                        continue
            
            # TERTIARY: Extract from JSON-LD metadata
            if not description or len(description) < 20:
                logging.debug("Secondary extraction failed, trying JSON-LD")
                try:
                    scripts = soup.find_all('script', {'type': 'application/ld+json'})
                    for script in scripts:
                        try:
                            if script.string:
                                data = json.loads(script.string)
                                if isinstance(data, list):
                                    data = data[0] if data else {}
                                if isinstance(data, dict) and 'description' in data:
                                    desc_value = data['description']
                                    if isinstance(desc_value, str):
                                        description = clean_text(desc_value)
                                        if description and len(description) > 20:
                                            logging.debug(f"Extracted from JSON-LD: {len(description)} chars")
                                            break
                        except (json.JSONDecodeError, TypeError, IndexError, AttributeError):
                            continue
                except Exception as e:
                    logging.debug(f"JSON-LD extraction failed: {e}")
            
            # Clean up description
            if description:
                noise_patterns = [
                    r'Transcript.*?Show transcript.*?Show less',
                    r'Follow along using.*?\n',
                    r'Green Olive Tours.*?subscribers',
                    r'Videos.*?About',
                ]
                for pattern in noise_patterns:
                    description = re.sub(pattern, '', description, flags=re.DOTALL | re.IGNORECASE)
                
                description = clean_text(description)
                
                if len(description) < 20:
                    description = None
            
            if description and len(description) > 15:
                logging.info(f"✓ Description extracted: {description[:80]}...")
                return description
            
            logging.debug(f"Description extraction failed - no suitable content found")
            return None
            
        except Exception as e:
            logging.debug(f"Error during description extraction: {e}")
            return None
        
    except PWTimeoutError:
        logging.debug(f"Timeout fetching description for {video_url}")
        return None
    except Exception as e:
        logging.debug(f"Skipping description - error occurred: {e}")
        return None
        return None

def scrape_channel_videos(page, channel_url: str) -> List[Dict]:
    """Scrape all videos from a YouTube channel - aggressive scrolling for all content"""
    videos = []
    
    try:
        logging.info(f"Scraping channel: {channel_url}")
        page.goto(channel_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        
        # Wait for videos to load
        page.wait_for_selector('ytd-rich-item-renderer', timeout=15000)
        page.wait_for_timeout(2000)
        
        # Aggressive scrolling to load ALL videos
        last_height = page.evaluate("document.documentElement.scrollHeight")
        scroll_count = 0
        max_scrolls = 100
        
        while scroll_count < max_scrolls:
            page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
            page.wait_for_timeout(1500)
            
            new_height = page.evaluate("document.documentElement.scrollHeight")
            if new_height == last_height:
                logging.info(f"Reached end of channel after {scroll_count} scrolls")
                break
            
            last_height = new_height
            scroll_count += 1
            if scroll_count % 10 == 0:
                logging.debug(f"Scroll progress: {scroll_count} scrolls completed")
        
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all video items
        video_elements = soup.select('ytd-rich-item-renderer')
        logging.info(f"Found {len(video_elements)} total video elements")
        
        for idx, video_element in enumerate(video_elements[:MAX_VIDEOS_PER_CHANNEL]):
            if idx > 0:
                time.sleep(DELAY_BETWEEN_VIDEOS_SEC)
            
            metadata = extract_video_metadata(video_element, channel_url=channel_url)
            if metadata:
                videos.append(metadata)
                logging.debug(f"Extracted video {idx+1}: {metadata['video_identification']['title'][:50]}")
        
        logging.info(f"Successfully scraped {len(videos)} videos from channel")
        return videos
        
    except Exception as e:
        logging.error(f"Error scraping channel {channel_url}: {e}")
        return videos

def extract_shorts_video_id_from_url(url: str) -> Optional[str]:
    """Extract shorts video ID from YouTube URL"""
    # Pattern for /shorts/VIDEO_ID
    match = re.search(r'/shorts/([a-zA-Z0-9_-]{11})', url)
    if match:
        return match.group(1)
    return None

def extract_shorts_metadata(shorts_element, channel_url: str = None) -> Optional[Dict]:
    """Extract metadata from a shorts video element in the grid"""
    try:
        # The shorts elements have class shortsLockupViewModelHost
        # Find the video title link
        title_link = shorts_element.find('a', {'class': lambda x: x and 'shortsLockupViewModelHostEndpoint' in x})
        
        if not title_link:
            return None
        
        href = title_link.get('href', '')
        if not href or '/shorts/' not in href:
            return None
        
        # Extract video ID from href
        video_id = extract_shorts_video_id_from_url(href)
        if not video_id:
            return None
        
        # Get title from the link title attribute or aria-label
        title = title_link.get('title', '') or title_link.get('aria-label', '')
        if not title:
            title_span = shorts_element.find('span', {'role': 'text'})
            if title_span:
                title = title_span.get_text(strip=True)
        
        if not title:
            return None
        
        # Filter for English titles only
        if not is_english_title(title):
            return None
        
        # Extract channel name from URL
        channel_name = extract_channel_name_from_url(channel_url) if channel_url else "Unknown"
        
        # Build full YouTube URL
        full_url = f"https://www.youtube.com/shorts/{video_id}"
        
        # Create basic metadata
        metadata = {
            "video_identification": {
                "video_id": video_id,
                "title": clean_text(title),
                "full_url": full_url,
                "content_type": "shorts"
            },
            "basic_info": {
                "duration": "Short Form",
                "channel": channel_name,
                "channel_url": channel_url or "Unknown"
            },
            "extracted_metadata": {
                "date": "Not Available",
                "place": "Not Applicable",
                "topic": "Not Applicable",
                "description": "Not Applicable",
                "channel": channel_name
            },
            "metadata": {
                "scraped_at": datetime.now().isoformat()
            }
        }
        
        return metadata
        
    except Exception as e:
        logging.debug(f"Error extracting shorts metadata: {e}")
        return None

def scrape_shorts_channel(page, channel_url: str, max_shorts: int = None) -> List[Dict]:
    """Scrape ALL shorts from a channel's shorts page with aggressive scrolling"""
    if max_shorts is None:
        max_shorts = MAX_VIDEOS_PER_CHANNEL
    
    shorts_list = []
    collected_ids = set()
    
    try:
        logging.info(f"Navigating to shorts channel: {channel_url}")
        page.goto(channel_url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT_MS)
        page.wait_for_timeout(3000)
        
        # Wait for shorts to load - try multiple selectors
        loaded = False
        selectors = [
            'ytm-shorts-lockup-view-model',
            'a[href*="/shorts/"]',
            'div[class*="shorts"]',
        ]
        
        for selector in selectors:
            try:
                page.wait_for_selector(selector, timeout=5000)
                logging.info(f"Found shorts using selector: {selector}")
                loaded = True
                break
            except:
                continue
        
        if not loaded:
            logging.warning(f"No shorts found for {channel_url} - trying with scroll anyway")
        
        # Aggressive scrolling to load ALL shorts
        last_height = page.evaluate("document.documentElement.scrollHeight")
        scroll_count = 0
        max_scrolls = 100
        no_new_content = 0
        
        while scroll_count < max_scrolls and no_new_content < 3:
            page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
            page.wait_for_timeout(1500)
            
            new_height = page.evaluate("document.documentElement.scrollHeight")
            if new_height == last_height:
                no_new_content += 1
                if no_new_content >= 3:
                    logging.info(f"Reached end of shorts after {scroll_count} scrolls")
                    break
            else:
                no_new_content = 0
            
            last_height = new_height
            scroll_count += 1
            if scroll_count % 10 == 0:
                logging.debug(f"Shorts scroll progress: {scroll_count} scrolls")
        
        # Extract all shorts from final page
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Try multiple selectors to find shorts
        shorts_elements = []
        
        # Try method 1: ytm-shorts-lockup-view-model
        shorts_elements = soup.find_all('ytm-shorts-lockup-view-model')
        logging.debug(f"Method 1 (ytm-shorts-lockup-view-model): Found {len(shorts_elements)} elements")
        
        # Try method 2: Look for /shorts/ links directly
        if len(shorts_elements) == 0:
            shorts_links = soup.find_all('a', {'href': re.compile(r'/shorts/')})
            logging.debug(f"Method 2 (direct links): Found {len(shorts_links)} shorts links")
            shorts_elements = shorts_links
        
        logging.info(f"Found {len(shorts_elements)} total shorts elements")
        
        for idx, shorts_elem in enumerate(shorts_elements):
            if len(shorts_list) >= max_shorts:
                break
            
            # Handle both ytm-shorts-lockup-view-model and direct link extraction
            if shorts_elem.name == 'ytm-shorts-lockup-view-model':
                metadata = extract_shorts_metadata(shorts_elem, channel_url=channel_url)
            else:
                # Direct link extraction
                href = shorts_elem.get('href', '')
                if '/shorts/' in href:
                    video_id = extract_shorts_video_id_from_url(href)
                    if video_id:
                        title = shorts_elem.get('title') or shorts_elem.get_text(strip=True)[:50]
                        if title and is_english_title(title):
                            channel_name = extract_channel_name_from_url(channel_url) if channel_url else "Unknown"
                            metadata = {
                                "video_identification": {
                                    "video_id": video_id,
                                    "title": clean_text(title),
                                    "full_url": f"https://www.youtube.com/shorts/{video_id}",
                                    "content_type": "shorts"
                                },
                                "basic_info": {
                                    "duration": "Short Form",
                                    "channel": channel_name,
                                    "channel_url": channel_url or "Unknown"
                                },
                                "extracted_metadata": {
                                    "date": "Not Available",
                                    "place": "Not Applicable",
                                    "topic": "Not Applicable",
                                    "description": "Not Applicable",
                                    "channel": channel_name
                                },
                                "metadata": {
                                    "scraped_at": datetime.now().isoformat()
                                }
                            }
                        else:
                            metadata = None
                    else:
                        metadata = None
                else:
                    metadata = None
            
            if metadata:
                video_id = metadata['video_identification']['video_id']
                if video_id not in collected_ids:
                    shorts_list.append(metadata)
                    collected_ids.add(video_id)
                    logging.debug(f"Extracted shorts {len(shorts_list)}: {metadata['video_identification']['title'][:50]}")
        
        logging.info(f"✓ Scraped {len(shorts_list)} shorts from {channel_url}")
        return shorts_list
        
    except Exception as e:
        logging.error(f"Error scraping shorts channel {channel_url}: {e}")
        return shorts_list

def scrape_search_results(page, search_url: str) -> List[Dict]:
    """Scrape videos from YouTube search results"""
    videos = []
    skipped_non_english = 0
    
    try:
        logging.info(f"Scraping search results: {search_url}")
        page.goto(search_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        
        # Wait for results to load
        page.wait_for_selector('ytd-video-renderer, ytd-rich-item-renderer', timeout=15000)
        
        # Scroll to load more results
        for _ in range(2):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.5)
        
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all video items
        video_elements = soup.select('ytd-video-renderer, ytd-rich-item-renderer')
        logging.info(f"Found {len(video_elements)} search result elements")
        
        for idx, video_element in enumerate(video_elements[:MAX_VIDEOS_PER_CHANNEL]):
            if idx > 0:
                time.sleep(DELAY_BETWEEN_VIDEOS_SEC)
            
            metadata = extract_video_metadata(video_element)
            if metadata:
                videos.append(metadata)
                logging.debug(f"Extracted search result {idx+1}: {metadata['video_identification']['title'][:50]}")
            else:
                skipped_non_english += 1
        
        if skipped_non_english > 0:
            logging.info(f"Skipped {skipped_non_english} non-English videos from search")
        
        return videos
        
    except Exception as e:
        logging.error(f"Error scraping search results {search_url}: {e}")
        return videos

def extract_shorts_date_from_page(page) -> Optional[str]:
    """Extract publish date from shorts page metadata"""
    try:
        html_content = page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Look for meta tag with uploadDate
        publish_date_meta = soup.find('meta', {'itemprop': 'uploadDate'})
        if publish_date_meta:
            content = publish_date_meta.get('content', '')
            if content:
                return content[:10]
        
        # Method 2: Look for the date text in the metadata area
        # Try finding in ytd-expandable-video-description-body-renderer
        desc_body = soup.find('ytd-expandable-video-description-body-renderer')
        if desc_body:
            # Look for date patterns in the metadata section
            yt_formatted_strings = desc_body.find_all('yt-formatted-string')
            for fs in yt_formatted_strings:
                text = fs.get_text(strip=True)
                date_match = extract_date_from_text(text)
                if date_match:
                    return date_match
        
        # Method 3: Look in general yt-formatted-string elements
        all_formatted = soup.find_all('yt-formatted-string')
        for fs in all_formatted[:20]:  # Check first 20
            text = fs.get_text(strip=True)
            if len(text) < 50 and ('ago' in text.lower() or 'year' in text.lower() or 'month' in text.lower() or 'week' in text.lower() or 'day' in text.lower()):
                date_match = extract_date_from_text(text)
                if date_match:
                    return date_match
        
        return None
    except Exception as e:
        logging.debug(f"Error extracting shorts date from page: {e}")
        return None

def enrich_shorts_metadata(page, metadata: Dict) -> Dict:
    """Enrich shorts metadata with description and extracted date/location/topic"""
    try:
        video_url = metadata['video_identification']['full_url']
        title = metadata['video_identification']['title']
        
        # Navigate to shorts video - with error handling
        try:
            page.goto(video_url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT_MS)
            page.wait_for_timeout(1500)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.debug(f"Failed to navigate to shorts {video_url}: {e}")
            # Use fallback enrichment
            title_date = extract_date_from_text(title)
            title_place = extract_location_from_text(title)
            title_topic = extract_topic_from_text(title)
            
            metadata['extracted_metadata']['description'] = "Not Applicable"
            metadata['extracted_metadata']['date'] = title_date or "Date Not Available"
            metadata['extracted_metadata']['place'] = title_place or "Location Not Specified"
            metadata['extracted_metadata']['topic'] = title_topic or "General Documentation"
            logging.info(f"✓ Enriched shorts (nav fallback): {title[:40]}")
            return metadata
        
        # Get page content
        try:
            html_content = page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            logging.debug(f"Failed to get page content for shorts: {e}")
            html_content = ""
            soup = BeautifulSoup("", 'html.parser')
        
        # Try to extract description from the description panel
        description = ""
        try:
            desc_body = soup.find('ytd-expandable-video-description-body-renderer')
            if desc_body:
                text_inline = desc_body.find('ytd-text-inline-expander')
                if text_inline:
                    snippet_text = text_inline.find('span', {'id': 'plain-snippet-text'})
                    if snippet_text:
                        description = clean_text(snippet_text.get_text())
            
            if not description:
                desc_content = soup.find('ytd-expandable-video-description-body-renderer')
                if desc_content:
                    description = clean_text(desc_content.get_text())
        except Exception as e:
            logging.debug(f"Error extracting description: {e}")
        
        metadata['extracted_metadata']['description'] = description or "Not Applicable"
        
        # Extract date from page first (shorts have date in metadata)
        page_date = None
        try:
            page_date = extract_shorts_date_from_page(page)
        except Exception as e:
            logging.debug(f"Error extracting shorts date from page: {e}")
        
        # Extract date, place, topic from title and description
        try:
            title_date = extract_date_from_text(title)
            title_place = extract_location_from_text(title)
            title_topic = extract_topic_from_text(title)
        except Exception as e:
            logging.debug(f"Error extracting from title: {e}")
            title_date = title_place = title_topic = None
        
        desc_date = None
        desc_place = None
        desc_topic = None
        
        if description:
            try:
                desc_date = extract_date_from_text(description)
                desc_place = extract_location_from_text(description)
                desc_topic = extract_topic_from_text(description)
            except Exception as e:
                logging.debug(f"Error extracting from description: {e}")
        
        # Combine: page metadata > description > title > defaults
        metadata['extracted_metadata']['date'] = page_date or desc_date or title_date or "Date Not Available"
        metadata['extracted_metadata']['place'] = desc_place or title_place or "Location Not Specified"
        metadata['extracted_metadata']['topic'] = desc_topic or title_topic or "General Documentation"
        
        logging.info(f"✓ Enriched shorts: {title[:40]} | Date: {metadata['extracted_metadata']['date']}")
        
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.debug(f"Error enriching shorts metadata (using fallback): {e}")
        title = metadata['video_identification']['title']
        
        # Fallback enrichment using title only
        try:
            metadata['extracted_metadata']['description'] = "Not Applicable"
            title_date = extract_date_from_text(title)
            title_place = extract_location_from_text(title)
            title_topic = extract_topic_from_text(title)
            
            metadata['extracted_metadata']['date'] = title_date or "Date Not Available"
            metadata['extracted_metadata']['place'] = title_place or "Location Not Specified"
            metadata['extracted_metadata']['topic'] = title_topic or "General Documentation"
        except Exception as fallback_e:
            logging.debug(f"Fallback enrichment also failed: {fallback_e}")
            metadata['extracted_metadata']['date'] = "Date Not Available"
            metadata['extracted_metadata']['place'] = "Location Not Specified"
            metadata['extracted_metadata']['topic'] = "General Documentation"
        
        logging.info(f"✓ Enriched shorts (fallback): {title[:40]}")
    
    return metadata

def enrich_video_metadata(page, metadata: Dict) -> Dict:
    """Enrich metadata with description and inferred metadata"""
    try:
        video_url = metadata['video_identification']['full_url']
        title = metadata['video_identification']['title']
        
        # Start with title analysis
        title_date = extract_date_from_text(title)
        title_place = extract_location_from_text(title)
        title_topic = extract_topic_from_text(title)
        
        # Fetch description - with comprehensive error handling
        description = None
        try:
            description = scrape_video_description(page, video_url)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.debug(f"Error fetching description for {video_url}: {e}")
            description = None
        
        metadata['extracted_metadata']['description'] = description or "Not Applicable"
        
        # Extract from description
        desc_date = None
        desc_place = None
        desc_topic = None
        
        if description:
            try:
                desc_date = extract_date_from_text(description)
                desc_place = extract_location_from_text(description)
                desc_topic = extract_topic_from_text(description)
            except Exception as e:
                logging.debug(f"Error extracting metadata from description: {e}")
        
        # Try to get date from YouTube page metadata
        page_date = None
        try:
            page_date = extract_date_from_page_metadata(page)
        except Exception as e:
            logging.debug(f"Error extracting page metadata: {e}")
        
        # Combine: page metadata > description > title > defaults
        metadata['extracted_metadata']['date'] = page_date or desc_date or title_date or "Date Not Available"
        metadata['extracted_metadata']['place'] = desc_place or title_place or "Location Not Specified"
        metadata['extracted_metadata']['topic'] = desc_topic or title_topic or "General Documentation"
        
        # If no real description was found, create meaningful fallback from title/topic
        if not description or description == "Not Applicable":
            topic = metadata['extracted_metadata']['topic']
            place = metadata['extracted_metadata']['place']
            metadata['extracted_metadata']['description'] = f"{topic} - {place}"
        
        logging.info(f"✓ Enriched: {title[:40]} | Place: {metadata['extracted_metadata']['place']} | Topic: {metadata['extracted_metadata']['topic']}")
        
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.debug(f"Error enriching metadata (using fallback): {e}")
        title = metadata['video_identification']['title']
        metadata['extracted_metadata']['description'] = f"Video: {title}"
        metadata['extracted_metadata']['date'] = "Date Not Available"
        metadata['extracted_metadata']['place'] = "Location Not Specified"
        metadata['extracted_metadata']['topic'] = extract_topic_from_text(title) or "General Documentation"
        logging.info(f"✓ Enriched (fallback): {title[:40]} | Topic: {metadata['extracted_metadata']['topic']}")
    
    return metadata

# ------------ Main scraper function -------------
def run_scraper():
    """Main scraper function"""
    log_file = setup_logging()
    logging.info("=" * 80)
    logging.info("YouTube Scraper Started")
    logging.info("=" * 80)
    
    OUTPUT_ROOT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create shorts and videos folders upfront
    (OUTPUT_ROOT_DIR / "videos").mkdir(parents=True, exist_ok=True)
    (OUTPUT_ROOT_DIR / "shorts").mkdir(parents=True, exist_ok=True)
    logging.info(f"Created output directories: {OUTPUT_ROOT_DIR / 'videos'} and {OUTPUT_ROOT_DIR / 'shorts'}")
    
    # Load existing index
    videos_index = load_videos_index()
    existing_video_ids: Set[str] = set(videos_index.keys())
    logging.info(f"Loaded {len(existing_video_ids)} existing videos from index")
    
    all_videos = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT_STRING)
            page = context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)
            
            # Scrape shorts channels FIRST
            logging.info(f"\n{'='*80}")
            logging.info("Starting Shorts Scraping (FIRST)")
            logging.info(f"{'='*80}")
            
            for shorts_url in TARGET_SHORTS_CHANNELS:
                try:
                    logging.info(f"\n--- Processing Shorts Channel: {shorts_url} ---")
                    shorts_videos = scrape_shorts_channel(page, shorts_url)
                    logging.info(f"Found {len(shorts_videos)} shorts to process")
                    
                    for shorts_metadata in shorts_videos:
                        video_id = shorts_metadata['video_identification']['video_id']
                        
                        if video_id not in existing_video_ids:
                            try:
                                # Enrich with description
                                try:
                                    shorts_metadata = enrich_shorts_metadata(page, shorts_metadata)
                                except KeyboardInterrupt:
                                    raise
                                except Exception as enrichment_error:
                                    logging.debug(f"Error enriching shorts {video_id}: {enrichment_error}, using basic metadata")
                                
                                # Save individual metadata file
                                try:
                                    save_video_metadata(video_id, shorts_metadata)
                                except Exception as save_error:
                                    logging.error(f"Error saving shorts {video_id}: {save_error}")
                                    continue
                                
                                # Add to index
                                videos_index[video_id] = {
                                    "title": shorts_metadata['video_identification']['title'],
                                    "url": shorts_metadata['video_identification']['full_url'],
                                    "date": shorts_metadata['extracted_metadata']['date'],
                                    "place": shorts_metadata['extracted_metadata'].get('place'),
                                    "topic": shorts_metadata['extracted_metadata'].get('topic'),
                                    "scraped_at": shorts_metadata['metadata']['scraped_at']
                                }
                                
                                all_videos.append(shorts_metadata)
                                existing_video_ids.add(video_id)
                                logging.info(f"✓ Saved new shorts: {video_id}")
                            except KeyboardInterrupt:
                                raise
                            except Exception as e:
                                logging.debug(f"Error processing shorts {video_id}: {e}")
                                # Still save with whatever metadata we have
                                try:
                                    save_video_metadata(video_id, shorts_metadata)
                                    existing_video_ids.add(video_id)
                                    logging.info(f"✓ Saved shorts (fallback): {video_id}")
                                except Exception as fallback_error:
                                    logging.error(f"Failed to save shorts {video_id}: {fallback_error}")
                        else:
                            logging.debug(f"Skipping existing shorts: {video_id}")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error processing shorts channel {shorts_url}: {e}")
                    continue
            
            # Scrape channels - REGULAR VIDEOS AFTER SHORTS
            for channel_url in TARGET_CHANNELS:
                try:
                    logging.info(f"\n--- Processing Channel (Regular Videos) ---")
                    channel_videos = scrape_channel_videos(page, channel_url)
                    
                    for video_metadata in channel_videos:
                        video_id = video_metadata['video_identification']['video_id']
                        
                        if video_id not in existing_video_ids:
                            try:
                                # Enrich with full description
                                try:
                                    video_metadata = enrich_video_metadata(page, video_metadata)
                                except KeyboardInterrupt:
                                    raise
                                except Exception as enrichment_error:
                                    logging.debug(f"Error enriching video {video_id}: {enrichment_error}, using basic metadata")
                                
                                # Save individual metadata file
                                try:
                                    save_video_metadata(video_id, video_metadata)
                                except Exception as save_error:
                                    logging.error(f"Error saving video {video_id}: {save_error}")
                                    continue
                                
                                # Add to index
                                videos_index[video_id] = {
                                    "title": video_metadata['video_identification']['title'],
                                    "url": video_metadata['video_identification']['full_url'],
                                    "date": video_metadata['extracted_metadata']['date'],
                                    "place": video_metadata['extracted_metadata'].get('place'),
                                    "topic": video_metadata['extracted_metadata'].get('topic'),
                                    "scraped_at": video_metadata['metadata']['scraped_at']
                                }
                                
                                all_videos.append(video_metadata)
                                existing_video_ids.add(video_id)
                                logging.info(f"✓ Saved new video: {video_id}")
                            except KeyboardInterrupt:
                                raise
                            except Exception as e:
                                logging.debug(f"Error processing video {video_id}: {e}")
                                # Still save with whatever metadata we have
                                try:
                                    save_video_metadata(video_id, video_metadata)
                                    existing_video_ids.add(video_id)
                                    logging.info(f"✓ Saved video (fallback): {video_id}")
                                except Exception as fallback_error:
                                    logging.error(f"Failed to save video {video_id}: {fallback_error}")
                        else:
                            logging.debug(f"Skipping existing video: {video_id}")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error processing channel {channel_url}: {e}")
                    continue
            
            # Scrape search results
            for search_url in SEARCH_QUERIES:
                try:
                    logging.info(f"\n--- Processing Search Results ---")
                    search_videos = scrape_search_results(page, search_url)
                    
                    for video_metadata in search_videos:
                        video_id = video_metadata['video_identification']['video_id']
                        
                        if video_id not in existing_video_ids:
                            try:
                                # Enrich with full description
                                try:
                                    video_metadata = enrich_video_metadata(page, video_metadata)
                                except KeyboardInterrupt:
                                    raise
                                except Exception as enrichment_error:
                                    logging.debug(f"Error enriching search video {video_id}: {enrichment_error}, using basic metadata")
                                
                                # Save individual metadata file
                                try:
                                    save_video_metadata(video_id, video_metadata)
                                except Exception as save_error:
                                    logging.error(f"Error saving search video {video_id}: {save_error}")
                                    continue
                                
                                # Add to index
                                videos_index[video_id] = {
                                    "title": video_metadata['video_identification']['title'],
                                    "url": video_metadata['video_identification']['full_url'],
                                    "date": video_metadata['extracted_metadata']['date'],
                                    "place": video_metadata['extracted_metadata'].get('place'),
                                    "topic": video_metadata['extracted_metadata'].get('topic'),
                                    "scraped_at": video_metadata['metadata']['scraped_at']
                                }
                                
                                all_videos.append(video_metadata)
                                existing_video_ids.add(video_id)
                                logging.info(f"✓ Saved new search video: {video_id}")
                            except KeyboardInterrupt:
                                raise
                            except Exception as e:
                                logging.debug(f"Error processing search video {video_id}: {e}")
                                # Still save with whatever metadata we have
                                try:
                                    save_video_metadata(video_id, video_metadata)
                                    existing_video_ids.add(video_id)
                                    logging.info(f"✓ Saved search video (fallback): {video_id}")
                                except Exception as fallback_error:
                                    logging.error(f"Failed to save search video {video_id}: {fallback_error}")
                        else:
                            logging.debug(f"Skipping existing search video: {video_id}")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    logging.error(f"Error processing search {search_url}: {e}")
                    continue
            
            browser.close()
        
        # Save updated index
        save_videos_index(videos_index)
        
        logging.info("\n" + "=" * 80)
        logging.info(f"Scraping Complete!")
        logging.info(f"Total videos scraped: {len(all_videos)}")
        logging.info(f"Total videos in index: {len(videos_index)}")
        logging.info(f"Output directory: {OUTPUT_ROOT_DIR.absolute()}")
        logging.info(f"Index file: {VIDEOS_INDEX_FILE.absolute()}")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"Fatal error during scraping: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    run_scraper()