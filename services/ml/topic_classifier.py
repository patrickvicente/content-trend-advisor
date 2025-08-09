"""
Advanced topic classification using transformers for ML pipeline.
Heavy ML dependencies isolated from ETL pipeline.
"""
import logging
from typing import List, Dict, Any, Optional
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopicClassifier:
    """
    Advanced topic classification using transformer models.
    Used for feature engineering and model training.
    """
    
    def __init__(self, model_name: str = "facebook/bart-large-mnli"):
        """Initialize classifier with transformer model."""
        self.model_name = model_name
        self._classifier = None
        
    def _load_classifier(self):
        """Lazy load the transformer model."""
        if self._classifier is None:
            try:
                from transformers import pipeline
                logger.info(f"Loading transformer model: {self.model_name}")
                self._classifier = pipeline(
                    "zero-shot-classification",
                    model=self.model_name,
                    device=-1  # Use CPU (set to 0 for GPU)
                )
                logger.info("Transformer model loaded successfully")
            except ImportError as e:
                raise ImportError(
                    "transformers not installed. Run: pip install transformers torch"
                ) from e
    
    def classify_topics(self, 
                       text: str, 
                       candidate_labels: List[str],
                       threshold: float = 0.3) -> Dict[str, Any]:
        """
        Classify text against candidate topic labels.
        
        Args:
            text: Input text to classify
            candidate_labels: List of possible topic labels
            threshold: Minimum confidence threshold
            
        Returns:
            Dict with labels, scores, and filtered results
        """
        if not text.strip():
            return {"labels": [], "scores": [], "filtered_labels": []}
            
        self._load_classifier()
        
        try:
            result = self._classifier(text, candidate_labels)
            
            # Filter by threshold
            filtered_labels = []
            for label, score in zip(result['labels'], result['scores']):
                if score >= threshold:
                    filtered_labels.append(label)
            
            return {
                "labels": result['labels'],
                "scores": result['scores'], 
                "filtered_labels": filtered_labels,
                "text_sample": text[:100] + "..." if len(text) > 100 else text
            }
            
        except Exception as e:
            logger.error(f"Error in topic classification: {e}")
            return {"labels": [], "scores": [], "filtered_labels": []}
    
    def batch_classify(self,
                      texts: List[str],
                      candidate_labels: List[str],
                      threshold: float = 0.3) -> List[Dict[str, Any]]:
        """
        Classify multiple texts efficiently.
        
        Args:
            texts: List of input texts
            candidate_labels: Topic labels to classify against
            threshold: Confidence threshold
            
        Returns:
            List of classification results
        """
        results = []
        for text in texts:
            result = self.classify_topics(text, candidate_labels, threshold)
            results.append(result)
        return results

def classify_youtube_content(videos: List[Dict[str, Any]], 
                           niche_topics: List[str] = None) -> List[Dict[str, Any]]:
    """
    High-level function to classify YouTube video content.
    
    Args:
        videos: List of YouTube video dictionaries
        niche_topics: List of topic labels to classify against
        
    Returns:
        Enhanced videos with ML-based topic classifications
    """
    if not niche_topics:
        niche_topics = [
            "Artificial Intelligence", "Machine Learning", "Programming",
            "Automation", "N8N", "No Code", "Tech Productivity",
            "Cloud Computing", "Data Science", "Software Development"
        ]
    
    classifier = TopicClassifier()
    enhanced_videos = []
    
    for video in videos:
        snippet = video.get('snippet', {})
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        
        # Combine title and description
        content = f"{title} {description}"
        
        # Classify content
        classification = classifier.classify_topics(
            text=content,
            candidate_labels=niche_topics,
            threshold=0.3
        )
        
        # Add ML classification to video metadata
        video['ml_classification'] = {
            'detected_topics': classification['filtered_labels'],
            'topic_scores': dict(zip(classification['labels'], classification['scores'])),
            'model_used': classifier.model_name,
            'confidence_threshold': 0.3
        }
        
        enhanced_videos.append(video)
    
    logger.info(f"Classified {len(enhanced_videos)} videos with ML model")
    return enhanced_videos

# Example usage and testing
if __name__ == "__main__":
    # Test the classifier
    test_texts = [
        "Complete N8N Automation Tutorial - No Code Workflows",
        "Building AI Apps with Python and FastAPI", 
        "How to cook pasta - Italian style"
    ]
    
    niche_topics = [
        "Artificial Intelligence", "Programming", "Automation", 
        "N8N", "No Code", "Cooking", "Tech Productivity"
    ]
    
    classifier = TopicClassifier()
    
    print("🧪 Testing ML Topic Classifier")
    print("=" * 40)
    
    for i, text in enumerate(test_texts):
        print(f"\n📝 Text {i+1}: {text}")
        result = classifier.classify_topics(text, niche_topics)
        
        print(f"🎯 Detected topics: {result['filtered_labels']}")
        print(f"📊 Top scores:")
        for label, score in zip(result['labels'][:3], result['scores'][:3]):
            print(f"   {label}: {score:.3f}")
    
    print("\n🎉 ML classification test completed!")
