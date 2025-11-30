#!/usr/bin/env python3
"""
youtube_test.py
Test script to verify YouTube scraper and API functionality

Usage:
    python youtube_test.py --scraper      # Test the scraper
    python youtube_test.py --api          # Test the API
    python youtube_test.py --full         # Full test suite
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple

# Configuration
OUTPUT_DIR = Path("output/youtube_videos")
LOGS_DIR = Path("output/logs")
API_URL = "http://127.0.0.1:5001"


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'


def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.RESET}")


def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.RESET}")


def print_info(msg: str):
    print(f"{Colors.BLUE}ℹ {msg}{Colors.RESET}")


def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.RESET}")


def test_dependencies() -> bool:
    """Test if all required dependencies are installed"""
    print(f"\n{Colors.BLUE}Testing Dependencies...{Colors.RESET}")
    
    dependencies = [
        ("playwright", "playwright"),
        ("bs4", "beautifulsoup4"),
        ("flask", "flask"),
        ("flask_cors", "flask-cors"),
    ]
    
    all_ok = True
    for module_name, package_name in dependencies:
        try:
            __import__(module_name)
            print_success(f"{package_name} installed")
        except ImportError:
            print_error(f"{package_name} NOT installed")
            print(f"   Install with: pip install {package_name}")
            all_ok = False
    
    return all_ok


def test_output_structure() -> bool:
    """Test if output directory structure exists"""
    print(f"\n{Colors.BLUE}Testing Output Structure...{Colors.RESET}")
    
    if OUTPUT_DIR.exists():
        print_success(f"Output directory exists: {OUTPUT_DIR}")
    else:
        print_warning(f"Output directory doesn't exist yet (will be created on first run)")
        return True
    
    # Check for index file
    index_file = OUTPUT_DIR / "VIDEOS_INDEX.json"
    if index_file.exists():
        try:
            with open(index_file) as f:
                index = json.load(f)
            count = len(index)
            print_success(f"VIDEOS_INDEX.json found with {count} videos")
            return True
        except json.JSONDecodeError:
            print_error(f"VIDEOS_INDEX.json is corrupted")
            return False
    else:
        print_warning(f"VIDEOS_INDEX.json not found (will be created on first scrape)")
        return True


def test_sample_metadata() -> bool:
    """Test if we can read a sample metadata file"""
    print(f"\n{Colors.BLUE}Testing Sample Metadata...{Colors.RESET}")
    
    if not OUTPUT_DIR.exists():
        print_warning("Output directory doesn't exist yet")
        return True
    
    metadata_files = list(OUTPUT_DIR.glob("*/*.json"))
    if not metadata_files:
        print_warning("No metadata files found (will be created on first scrape)")
        return True
    
    try:
        with open(metadata_files[0]) as f:
            metadata = json.load(f)
        
        # Check required fields
        required_fields = [
            "video_identification",
            "basic_info",
            "extracted_metadata",
            "metadata"
        ]
        
        for field in required_fields:
            if field in metadata:
                print_success(f"Metadata field '{field}' present")
            else:
                print_error(f"Metadata field '{field}' missing")
                return False
        
        # Check extracted metadata
        extracted = metadata.get("extracted_metadata", {})
        required_extracted = ["title", "date", "place", "topic", "description"]
        
        for field in required_extracted:
            if field in extracted:
                value = extracted[field]
                if value:
                    print_success(f"Extracted field '{field}': {str(value)[:50]}...")
                else:
                    print_warning(f"Extracted field '{field}': (empty)")
            else:
                print_error(f"Extracted field '{field}' missing")
        
        print_success(f"File name: {metadata_files[0].name}")
        
        return True
        
    except Exception as e:
        print_error(f"Error reading metadata: {e}")
        return False


def test_api_endpoints() -> bool:
    """Test API endpoints"""
    print(f"\n{Colors.BLUE}Testing API Endpoints...{Colors.RESET}")
    
    try:
        import requests
    except ImportError:
        print_warning("requests library not installed, skipping API tests")
        print("   Install with: pip install requests")
        return True
    
    endpoints = [
        ("/api/health", "Health check"),
        ("/api/stats", "Statistics"),
        ("/api/videos", "Videos list"),
        ("/api/places", "Places"),
        ("/api/topics", "Topics"),
    ]
    
    all_ok = True
    for endpoint, description in endpoints:
        try:
            response = requests.get(f"{API_URL}{endpoint}", timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    print_success(f"{description}: {endpoint}")
                else:
                    print_warning(f"{description}: returned but not successful")
                    all_ok = False
            else:
                print_error(f"{description}: HTTP {response.status_code}")
                all_ok = False
        except requests.ConnectionError:
            print_error(f"Cannot connect to API at {API_URL}")
            print("   Make sure API server is running: python youtube_api.py")
            return False
        except Exception as e:
            print_error(f"{description}: {str(e)}")
            all_ok = False
    
    return all_ok


def test_api_search() -> bool:
    """Test API search functionality"""
    print(f"\n{Colors.BLUE}Testing API Search...{Colors.RESET}")
    
    try:
        import requests
    except ImportError:
        print_warning("requests library not installed, skipping search test")
        return True
    
    try:
        # Test search with a simple query
        response = requests.get(f"{API_URL}/api/search?q=test&limit=5", timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                count = data.get("count", 0)
                print_success(f"Search working: found {count} results")
                return True
            else:
                print_warning(f"Search endpoint returned error: {data.get('error')}")
                return True
        else:
            print_error(f"Search failed: HTTP {response.status_code}")
            return False
            
    except requests.ConnectionError:
        print_error("Cannot connect to API")
        return False
    except Exception as e:
        print_error(f"Search test failed: {e}")
        return False


def test_logs() -> bool:
    """Test if logs are being created"""
    print(f"\n{Colors.BLUE}Testing Logs...{Colors.RESET}")
    
    if LOGS_DIR.exists():
        log_files = list(LOGS_DIR.glob("*.log"))
        if log_files:
            latest_log = max(log_files, key=lambda p: p.stat().st_mtime)
            print_success(f"Log file found: {latest_log.name}")
            
            # Check log content
            try:
                with open(latest_log) as f:
                    lines = f.readlines()
                print_success(f"Log contains {len(lines)} entries")
                return True
            except Exception as e:
                print_error(f"Cannot read log: {e}")
                return False
        else:
            print_warning("No log files found (will be created on first run)")
            return True
    else:
        print_warning("Logs directory doesn't exist yet")
        return True


def test_scraper_config() -> bool:
    """Test scraper configuration"""
    print(f"\n{Colors.BLUE}Testing Scraper Configuration...{Colors.RESET}")
    
    try:
        # Try to import and check config
        import youtube_scraper
        
        configs = [
            ("TARGET_CHANNELS", youtube_scraper.TARGET_CHANNELS),
            ("SEARCH_QUERIES", youtube_scraper.SEARCH_QUERIES),
            ("MAX_VIDEOS_PER_CHANNEL", youtube_scraper.MAX_VIDEOS_PER_CHANNEL),
            ("DELAY_BETWEEN_VIDEOS_SEC", youtube_scraper.DELAY_BETWEEN_VIDEOS_SEC),
        ]
        
        for name, value in configs:
            if isinstance(value, list):
                print_success(f"{name}: {len(value)} items configured")
            else:
                print_success(f"{name}: {value}")
        
        return True
        
    except ImportError:
        print_error("Cannot import youtube_scraper module")
        return False
    except Exception as e:
        print_error(f"Error checking config: {e}")
        return False


def test_json_validity() -> bool:
    """Test if all JSON files are valid"""
    print(f"\n{Colors.BLUE}Testing JSON Validity...{Colors.RESET}")
    
    if not OUTPUT_DIR.exists():
        print_warning("Output directory doesn't exist yet")
        return True
    
    json_files = list(OUTPUT_DIR.glob("*/METADATA.json")) + [OUTPUT_DIR / "VIDEOS_INDEX.json"]
    
    if not json_files:
        print_warning("No JSON files found yet")
        return True
    
    all_ok = True
    for json_file in json_files:
        if not json_file.exists():
            continue
        
        try:
            with open(json_file) as f:
                json.load(f)
            print_success(f"Valid JSON: {json_file.name}")
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON: {json_file.name} - {e}")
            all_ok = False
        except Exception as e:
            print_error(f"Error reading {json_file.name}: {e}")
            all_ok = False
    
    return all_ok


def run_tests(test_type: str = "all") -> None:
    """Run test suite"""
    print(f"\n{Colors.BLUE}{'='*60}")
    print("YouTube Scraper - Test Suite")
    print(f"{'='*60}{Colors.RESET}\n")
    
    results: List[Tuple[str, bool]] = []
    
    # Basic tests (always run)
    if test_type in ["all", "basic", "scraper", "full"]:
        results.append(("Dependencies", test_dependencies()))
        results.append(("Output Structure", test_output_structure()))
        results.append(("Scraper Configuration", test_scraper_config()))
        results.append(("Logs", test_logs()))
        results.append(("JSON Validity", test_json_validity()))
        results.append(("Sample Metadata", test_sample_metadata()))
    
    # API tests
    if test_type in ["all", "api", "full"]:
        results.append(("API Endpoints", test_api_endpoints()))
        results.append(("API Search", test_api_search()))
    
    # Summary
    print(f"\n{Colors.BLUE}{'='*60}")
    print("Test Summary")
    print(f"{'='*60}{Colors.RESET}\n")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"{status} - {test_name}")
    
    print(f"\n{Colors.BLUE}Results: {passed}/{total} tests passed{Colors.RESET}\n")
    
    if passed == total:
        print_success("All tests passed!")
        sys.exit(0)
    else:
        print_error(f"{total - passed} test(s) failed")
        sys.exit(1)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test YouTube scraper and API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python youtube_test.py --scraper    # Test scraper setup
  python youtube_test.py --api        # Test API connectivity
  python youtube_test.py --full       # Full test suite
  python youtube_test.py              # Basic tests (default)
        """
    )
    
    parser.add_argument("--scraper", action="store_true", help="Test scraper only")
    parser.add_argument("--api", action="store_true", help="Test API only")
    parser.add_argument("--full", action="store_true", help="Full test suite")
    
    args = parser.parse_args()
    
    if args.scraper:
        run_tests("scraper")
    elif args.api:
        run_tests("api")
    elif args.full:
        run_tests("full")
    else:
        run_tests("basic")


if __name__ == "__main__":
    main()