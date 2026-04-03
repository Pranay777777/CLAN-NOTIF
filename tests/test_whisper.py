import whisper
import time

# ── CHANGE THIS to any video you want to test ──
VIDEO_PATH = "./videos/Abhishek_Sahu_-_An_Intro_720P.mp4"
# ──────────────────────────────────────────────

print("Loading model...")
model = whisper.load_model("medium")
print("Model loaded")

print(f"Transcribing: {VIDEO_PATH}")
print("This may take 1-2 minutes...")
start = time.time()

result = model.transcribe(VIDEO_PATH, task="translate")  # translate to English

elapsed = time.time() - start
print(f"Done in {elapsed:.1f}s")
print("=" * 50)
print("TRANSCRIPT:")
print("=" * 50)
print(result["text"])
print("=" * 50)
print(f"Detected language: {result['language']}")