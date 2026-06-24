# add_videos.py - Add Missing Videos to Qdrant

"""
This script adds the missing videos for days 9, 12, 19, 20, 21 to Qdrant.

Run it:
    python add_videos.py

The videos will be immediately available in your API!
"""

from sentence_transformers import SentenceTransformer
from qdrant.write import upsert_videos

# Initialize the embedding model
print("Loading embedding model...")
model = SentenceTransformer("all-MiniLM-L6-v2")

# Define the 5 videos for missing days
videos_to_add = [
    {
        "id": 1000,
        "title": "Supervisor Tips for Customer Generation",
        "creator_name": "Your Supervisor",
        "lead_indicators": ["customer_generation"],
        "language": "en",
        "sales_phase": "acquisition",
        "experience_level": "all",
        "summary": "Learn supervisor-approved techniques for better customer generation",
        "key_lesson": "Act on supervisor feedback to improve results",
        "problem_solved": "Understanding customer generation strategy",
        "video_id": 1000,
    },
    {
        "id": 1001,
        "title": "Advanced Lead Closing Techniques",
        "creator_name": "Expert Trainer",
        "lead_indicators": ["lead_closing"],
        "language": "en",
        "sales_phase": "conversion",
        "experience_level": "experienced",
        "summary": "Advanced techniques for closing leads and converting to customers",
        "key_lesson": "Master the closing techniques to improve conversion rates",
        "problem_solved": "Improving closing rates and deal conversion",
        "video_id": 1001,
    },
    {
        "id": 1002,
        "title": "Leadership Message: Winning Strategy",
        "creator_name": "Company Leader",
        "lead_indicators": ["customer_generation"],
        "language": "en",
        "sales_phase": "acquisition",
        "experience_level": "all",
        "summary": "Company leader shares the winning strategy for success",
        "key_lesson": "Follow leadership guidance and company strategy",
        "problem_solved": "Aligning with company strategy and vision",
        "video_id": 1002,
    },
    {
        "id": 1003,
        "title": "Inspirational Success Story",
        "creator_name": "Top Performer",
        "lead_indicators": ["performance_management"],
        "language": "en",
        "sales_phase": "all",
        "experience_level": "all",
        "summary": "Hear from a top performer who started exactly where you are",
        "key_lesson": "Never give up on your goals and keep pushing forward",
        "problem_solved": "Finding motivation and inspiration for success",
        "video_id": 1003,
    },
    {
        "id": 1004,
        "title": "CBO Message: Win at Work",
        "creator_name": "Chief Business Officer",
        "lead_indicators": ["customer_generation"],
        "language": "en",
        "sales_phase": "conversion",
        "experience_level": "all",
        "summary": "Chief Business Officer delivers final message on winning at work",
        "key_lesson": "Commit to winning at work and apply all your learning",
        "problem_solved": "Final commitment to success and implementation",
        "video_id": 1004,
    }
]

# Convert videos to Qdrant format with embeddings
print(f"Creating embeddings for {len(videos_to_add)} videos...")
points = []

for i, video in enumerate(videos_to_add):
    # Create embedding from video text
    text = f"{video['title']} {video['summary']} {video['key_lesson']}"
    vector = model.encode(text).tolist()
    
    point = {
        "id": video["id"],
        "vector": vector,
        "payload": video
    }
    points.append(point)
    print(f"  ✓ {i+1}. {video['title']}")

# Upload to Qdrant
print(f"\nUploading {len(points)} videos to Qdrant...")
try:
    upsert_videos(points)
    print(f"✅ Successfully added {len(points)} videos to Qdrant!")
    print("\nVideos added:")
    for video in videos_to_add:
        print(f"  - Video {video['video_id']}: {video['title']}")
    print("\n🎉 Your API now has all 21-day videos!")
    print("\nTest with:")
    print("  curl -X POST http://localhost:8000/notifications/send -d '{\"user_id\": 953, \"campaign_day\": 9}'")
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure Qdrant is running (http://172.20.3.65:6333)")
    print("2. Check your QDRANT_URL environment variable")
    print("3. Ensure sentence-transformers is installed: pip install sentence-transformers")
