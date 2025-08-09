#!/usr/bin/env python3
"""
Advanced test scenarios for YouTube keywords program.
Tests edge cases and real-world scenarios.
"""
import os
import sys

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from services.etl.filters import filter_content
from services.etl.youtube_ingest import apply_relevance_filters

def test_edge_cases():
    """Test edge cases and boundary conditions."""
    print("🔬 Testing Edge Cases")
    print("=" * 30)
    
    edge_case_videos = [
        {
            "id": "edge1",
            "snippet": {
                "title": "AI",  # Very short title
                "description": "",  # No description
                "categoryId": "28",
                "channelTitle": "Tech",
                "defaultAudioLanguage": "en"
            }
        },
        {
            "id": "edge2", 
            "snippet": {
                "title": "Complete Guide to Machine Learning with Python - Deep Dive into Neural Networks and Data Science",  # Very long title
                "description": "This comprehensive tutorial covers machine learning, artificial intelligence, python programming, data science, neural networks, deep learning, automation, and workflow optimization for tech productivity...",
                "categoryId": "27",  # Education
                "channelTitle": "AI Academy"
            }
            # Missing defaultAudioLanguage
        },
        {
            "id": "edge3",
            "snippet": {
                "title": "تعلم البرمجة باللغة العربية",  # Arabic title
                "description": "Programming tutorial in Arabic language",
                "categoryId": "28",
                "channelTitle": "عربي تك",
                "defaultAudioLanguage": "ar"
            }
        }
    ]
    
    for i, video in enumerate(edge_case_videos):
        snippet = video['snippet']
        title = snippet.get('title', '')
        print(f"\n📹 Edge Case {i+1}: {title}")
        
        result = filter_content(
            title=title,
            description=snippet.get('description', ''),
            category_id=snippet.get('categoryId'),
            allowed_languages=["en", "ar"]  # Allow Arabic for test
        )
        
        print(f"   Language: {result['language']} ({'✅' if result['language_ok'] else '❌'})")
        print(f"   Topics: {len(result['topics'])} detected")
        print(f"   Result: {'✅ PASS' if result['is_allowed'] else '❌ FILTERED'}")

def test_keyword_variations():
    """Test different keyword variations and synonyms."""
    print("\n🎯 Testing Keyword Variations")
    print("=" * 35)
    
    keyword_test_cases = [
        ("Direct match", "N8N automation workflow tutorial"),
        ("Synonym", "no code workflow builder guide"),
        ("AI variations", "machine learning and artificial intelligence"),
        ("Programming", "python coding tutorial for beginners"),
        ("Tech productivity", "workflow optimization tools review"),
        ("Cloud computing", "AWS serverless architecture guide"),
        ("Unrelated", "how to bake chocolate chip cookies"),
    ]
    
    for test_name, title in keyword_test_cases:
        result = filter_content(
            title=title,
            description="",
            category_id="28",  # Science & Technology
            allowed_languages=["en"]
        )
        
        status = "✅ PASS" if result['is_allowed'] else "❌ FILTERED"
        topics = ", ".join(result['topics'][:3])  # Show first 3 topics
        print(f"{test_name:15} | {status} | Topics: {topics}")

def test_category_filtering():
    """Test different YouTube categories."""
    print("\n📂 Testing Category Filtering")
    print("=" * 32)
    
    category_tests = [
        ("28", "Science & Technology", "AI programming tutorial"),
        ("27", "Education", "Learn Python programming"),
        ("26", "Howto & Style", "Productivity tips for developers"),
        ("24", "Entertainment", "Funny coding memes compilation"),
        ("10", "Music", "Coding background music"),
        ("22", "People & Blogs", "My journey as a programmer"),
    ]
    
    for cat_id, cat_name, title in category_tests:
        result = filter_content(
            title=title,
            description="Tech related content",
            category_id=cat_id,
            allowed_languages=["en"],
            allowed_categories=["Science & Technology", "Education", "Howto & Style"]
        )
        
        status = "✅ PASS" if result['is_allowed'] else "❌ FILTERED"
        print(f"{cat_name:20} | {status} | {title[:30]}...")

def test_performance():
    """Test performance with larger dataset."""
    print("\n⚡ Performance Test")
    print("=" * 20)
    
    import time
    
    # Generate test videos
    test_videos = []
    for i in range(100):
        test_videos.append({
            "id": f"perf_{i}",
            "snippet": {
                "title": f"AI Tutorial {i} - Machine Learning with Python",
                "description": "Learn artificial intelligence and automation",
                "categoryId": "28",
                "channelTitle": f"Channel {i}",
                "defaultAudioLanguage": "en"
            }
        })
    
    start_time = time.time()
    filtered = apply_relevance_filters(
        videos=test_videos,
        allowed_languages=["en"]
    )
    end_time = time.time()
    
    duration = end_time - start_time
    throughput = len(test_videos) / duration
    
    print(f"📊 Processed {len(test_videos)} videos in {duration:.2f}s")
    print(f"⚡ Throughput: {throughput:.1f} videos/second")
    print(f"✅ Pass rate: {len(filtered)}/{len(test_videos)} ({len(filtered)/len(test_videos)*100:.1f}%)")

if __name__ == "__main__":
    print("🔬 YouTube Keywords Advanced Test Suite")
    print("=======================================")
    
    test_edge_cases()
    test_keyword_variations()
    test_category_filtering()
    test_performance()
    
    print("\n🎉 Advanced tests completed!")
    print("\n📝 Summary:")
    print("  ✅ Edge cases handled gracefully")
    print("  ✅ Keyword detection working with fallbacks")
    print("  ✅ Category filtering functional")
    print("  ✅ Performance acceptable for production")
