"""
Content filtering and labeling module for Content Trend Advisor ETL pipeline.

This module provides:
- Language detection for multi-lingual filtering
- Category-based filtering using YouTube categories
- Zero-shot topic classification for niche content identification
- Robust error handling and logging

Focus: Keep dataset niche + language focused and properly labeled.
"""
import os
import logging
import yaml
from typing import List, Dict, Any, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global config cache
_config_cache = {}

def _load_config(config_name: str) -> Dict[str, Any]:
    """Load and cache YAML configuration files."""
    if config_name not in _config_cache:
        config_path = Path(__file__).parent / "config" / f"{config_name}.yml"
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                _config_cache[config_name] = yaml.safe_load(f)
                logger.info(f"Loaded config: {config_name}")
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            _config_cache[config_name] = {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config {config_name}: {e}")
            _config_cache[config_name] = {}
    
    return _config_cache[config_name]

def detect_language(text: str) -> str:
    """
    Return ISO language code ('en', 'tl', etc.) from title/description.
    Uses langdetect library with fallback to 'unknown'.
    
    Args:
        text: Text to analyze (title, description, etc.)
        
    Returns:
        str: ISO language code or 'unknown' if detection fails
        
    Example:
        lang = detect_language("This is an English title")  # Returns 'en'
    """
    if not text or not text.strip():
        return 'unknown'
    
    try:
        from langdetect import detect, LangDetectException
        return detect(text.strip())
    except ImportError:
        logger.warning("langdetect not installed, falling back to English detection")
        # Simple English heuristic fallback
        english_words = {'the', 'and', 'or', 'is', 'are', 'was', 'were', 'to', 'in', 'on', 'at'}
        words = set(text.lower().split())
        if any(word in english_words for word in words):
            return 'en'
        return 'unknown'
    except LangDetectException:
        logger.debug(f"Language detection failed for text: {text[:50]}...")
        return 'unknown'
    except Exception as e:
        logger.error(f"Unexpected error in language detection: {e}")
        return 'unknown'

def is_language_allowed(lang: str, allow: List[str]) -> bool:
    """
    True if detected language is in allowlist (e.g., ['en','tl']).
    
    Args:
        lang: Language code from detect_language()
        allow: List of allowed language codes
        
    Returns:
        bool: True if language is allowed
    """
    return lang in allow

def map_category_id_to_name(category_id: str) -> str:
    """
    Use categories.yml mapping from YouTube categoryId to name.
    
    Args:
        category_id: YouTube category ID (e.g., "28")
        
    Returns:
        str: Category name or 'Unknown' if not found
        
    Example:
        name = map_category_id_to_name("28")  # Returns "Science & Technology"
    """
    config = _load_config("categories")
    categories = config.get("categories", {})
    return categories.get(str(category_id), "Unknown")

def is_category_allowed(category_name: str, allow: List[str], deny: List[str]) -> bool:
    """
    True if category is in allow list and not in deny list.
    
    Args:
        category_name: Category name from map_category_id_to_name()
        allow: List of allowed categories
        deny: List of denied categories
        
    Returns:
        bool: True if category is allowed
    """
    # If category is explicitly denied, reject
    if category_name in deny:
        return False
    
    # If allow list is empty, allow all (except denied)
    if not allow:
        return True
        
    # Otherwise, must be in allow list
    return category_name in allow

def zero_shot_topic_labels(title: str, description: str | None = None) -> List[str]:
    """
    Return labels from your niche topic set using lightweight keyword matching.
    For ETL pipeline efficiency - heavy ML models moved to services/ml/.
    
    Args:
        title: Video/content title
        description: Optional description text
        
    Returns:
        List[str]: List of relevant topic labels based on keyword matching
        
    Example:
        labels = zero_shot_topic_labels("Building AI Apps with Python", "Tutorial on...")
        # Returns: ["Artificial Intelligence", "Programming", "Software Development"]
    """
    config = _load_config("topics")
    niche_topics = config.get("niche_topics", [])
    
    if not niche_topics:
        logger.warning("No niche topics configured")
        return []
    
    # Combine title and description for better context
    text = title
    if description:
        text += f" {description}"
    
    if not text.strip():
        return []
    
    # Lightweight keyword-based topic detection for ETL
    text_lower = text.lower()
    relevant_labels = []
    
    # Enhanced keyword mappings for better accuracy
    keyword_map = {
        # AI & ML
        "Artificial Intelligence": ["ai", "artificial intelligence", "machine learning", "neural", "deep learning", "ml model", "chatgpt", "openai"],
        "Machine Learning": ["machine learning", "ml", "neural network", "deep learning", "tensorflow", "pytorch", "scikit"],
        
        # Automation & Workflows  
        "N8N": ["n8n", "n8n.io"],
        "Workflow Automation": ["workflow automation", "automated workflow", "workflow builder", "process automation"],
        "Automation": ["automation", "automate", "automated", "script", "bot", "zapier", "ifttt"],
        "No Code": ["no code", "low code", "nocode", "lowcode", "visual programming", "drag and drop"],
        
        # Programming & Development
        "Programming": ["python", "javascript", "java", "code", "coding", "programming", "developer", "development"],
        "Software Development": ["software", "development", "app", "application", "framework", "library", "package"],
        "API Development": ["api", "rest", "graphql", "webhook", "integration", "endpoint", "microservice"],
        
        # Productivity & Tools
        "Tech Productivity": ["productivity", "tools", "efficiency", "optimization", "workflow", "task management"],
        "Workflow Optimization": ["workflow", "optimization", "efficiency", "process improvement", "streamline"],
        "Tech Tools": ["tools", "software tools", "tech tools", "utilities", "applications"],
        
        # Infrastructure & Data
        "Cloud Computing": ["cloud", "aws", "azure", "gcp", "serverless", "docker", "kubernetes"],
        "Data Science": ["data", "analytics", "visualization", "pandas", "analysis", "dashboard", "metrics"],
        "Database": ["database", "sql", "nosql", "postgres", "mongodb", "mysql", "sqlite"],
        "DevOps": ["devops", "ci/cd", "jenkins", "github actions", "deployment", "infrastructure"],
        
        # Specific Niches
        "Notion": ["notion", "notion.so", "notion app", "notion workspace"],
        "Productivity": ["productivity", "gtd", "time management", "organization", "planning"],
        "Mental Health": ["mental health", "wellness", "mindfulness", "stress", "anxiety"],
        "ADHD": ["adhd", "attention deficit", "focus", "concentration", "executive function"]
    }
    
    # Score-based matching for better accuracy
    topic_scores = {}
    
    for topic, keywords in keyword_map.items():
        score = 0
        for keyword in keywords:
            if keyword in text_lower:
                # Weight longer, more specific keywords higher
                weight = len(keyword.split()) * 2 if len(keyword.split()) > 1 else 1
                score += weight
        
        if score > 0:
            topic_scores[topic] = score
    
    # Sort by relevance and return topics above threshold
    min_score = 1  # Minimum score to include topic
    relevant_labels = [topic for topic, score in topic_scores.items() if score >= min_score]
    
    # Sort by score (most relevant first)
    relevant_labels.sort(key=lambda t: topic_scores[t], reverse=True)
    
    logger.debug(f"Topic detection for '{title[:50]}...': {relevant_labels}")
    return relevant_labels

def is_topic_relevant(labels: List[str], required_any: List[str]) -> bool:
    """
    True if any desired labels present (e.g., at least one of your niche topics).
    
    Args:
        labels: List of detected topic labels
        required_any: List of required topics (need at least one)
        
    Returns:
        bool: True if at least one required topic is present
        
    Example:
        relevant = is_topic_relevant(
            ["Programming", "Tech Tools"], 
            ["Programming", "AI", "Data Science"]
        )  # Returns True (Programming matches)
    """
    return any(label in required_any for label in labels)

# Convenience function for complete filtering pipeline
def filter_content(title: str, 
                  description: str | None = None,
                  category_id: str | None = None,
                  allowed_languages: List[str] = None,
                  allowed_categories: List[str] = None,
                  denied_categories: List[str] = None) -> Dict[str, Any]:
    """
    Complete content filtering pipeline with all checks.
    
    Args:
        title: Content title
        description: Content description
        category_id: YouTube category ID
        allowed_languages: List of allowed language codes
        allowed_categories: List of allowed categories
        denied_categories: List of denied categories
        
    Returns:
        Dict with filtering results and metadata
        
    Example:
        result = filter_content(
            title="Building AI Apps",
            category_id="28",
            allowed_languages=["en"],
            allowed_categories=["Science & Technology"]
        )
        # Returns: {"is_allowed": True, "language": "en", "topics": [...], ...}
    """
    # Set defaults
    allowed_languages = allowed_languages or ["en"]
    config = _load_config("topics")
    required_topics = config.get("required_topics", [])
    
    # Detect language
    language = detect_language(title + " " + (description or ""))
    language_ok = is_language_allowed(language, allowed_languages)
    
    # Check category
    category_name = map_category_id_to_name(category_id) if category_id else "Unknown"
    category_ok = is_category_allowed(
        category_name, 
        allowed_categories or [], 
        denied_categories or []
    )
    
    # Detect topics
    topics = zero_shot_topic_labels(title, description)
    topics_ok = is_topic_relevant(topics, required_topics)
    
    # Overall decision
    is_allowed = language_ok and category_ok and topics_ok
    
    return {
        "is_allowed": is_allowed,
        "language": language,
        "language_ok": language_ok,
        "category": category_name,
        "category_ok": category_ok,
        "topics": topics,
        "topics_ok": topics_ok,
        "metadata": {
            "title_length": len(title),
            "has_description": description is not None,
            "topic_count": len(topics)
        }
    }