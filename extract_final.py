import cv2
import os
import re
import whisper
import logging
import time
import io
from difflib import SequenceMatcher
import requests
from PIL import Image
from unstructured.partition.image import partition_image
from dotenv import load_dotenv
import subprocess
import tempfile

load_dotenv()
os.makedirs("logs", exist_ok=True)

# ── LOGGING ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/clan_extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('extract')

# ── SETTINGS ──────────────────────────────────────────
VIDEO_FOLDER   = "./videos"
FRAME_INTERVAL = float(os.getenv("OCR_FRAME_INTERVAL", "1.0"))
OCR_SCALE      = float(os.getenv("OCR_SCALE", "1.0"))
OCR_CLEAN_LINES = os.getenv("OCR_CLEAN_LINES", "1").strip().lower() not in {"0", "false", "no"}
OCR_CLEAN_MODE = os.getenv("OCR_CLEAN_MODE", "relaxed").strip().lower()  # relaxed|strict
OCR_DEDUP_LINES = os.getenv("OCR_DEDUP_LINES", "1").strip().lower() not in {"0", "false", "no"}
OCR_DEBUG = os.getenv("OCR_DEBUG", "1").strip().lower() in {"1", "true", "yes"}
OCR_CROP_MODE = None
OCR_CROP_RATIO = float(os.getenv("OCR_CROP_RATIO", "0.35"))
OCR_MAX_FRAMES = int(os.getenv("OCR_MAX_FRAMES", "0"))
OCR_DEDUP_SIMILARITY = float(os.getenv("OCR_DEDUP_SIMILARITY", "0.75"))
OCR_SKIP_SAME_FRAME = os.getenv("OCR_SKIP_SAME_FRAME", "0").strip().lower() in {"1", "true", "yes"}
OCR_HASH_SIZE = int(os.getenv("OCR_HASH_SIZE", "8"))
OCR_PRESERVE_PARENS = os.getenv("OCR_PRESERVE_PARENS", "1").strip().lower() in {"1", "true", "yes"}
WHISPER_MODEL  = "medium"
UNSTRUCTURED_OCR_URL = os.getenv("UNSTRUCTURED_OCR_URL", "").strip()
UNSTRUCTURED_OCR_TIMEOUT = int(os.getenv("UNSTRUCTURED_OCR_TIMEOUT", "45"))
UNSTRUCTURED_OCR_MODE = os.getenv("UNSTRUCTURED_OCR_MODE", "auto").strip().lower()  # auto|remote|local
OCR_RUNTIME_STATS = {
    "unstructured_errors": 0,
    "processed_frames": 0,
    "last_frame_processed": 0,
}
# ──────────────────────────────────────────────────────

_WHISPER_MODEL_INSTANCE = None


def get_whisper_model():
    global _WHISPER_MODEL_INSTANCE
    if _WHISPER_MODEL_INSTANCE is None:
        logger.info("Loading Whisper model...")
        _WHISPER_MODEL_INSTANCE = whisper.load_model(WHISPER_MODEL)
        logger.info("Whisper model ready")
    return _WHISPER_MODEL_INSTANCE


def _extract_unstructured_text(payload):
    if isinstance(payload, dict):
        if isinstance(payload.get("text"), str):
            return payload["text"]
        if isinstance(payload.get("content"), str):
            return payload["content"]

        elements = payload.get("elements")
        if isinstance(elements, list):
            parts = []
            for item in elements:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            return "\n".join(parts)

    if isinstance(payload, list):
        parts = []
        for item in payload:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts)

    return ""


def _ocr_with_unstructured(img):
    if not UNSTRUCTURED_OCR_URL:
        raise RuntimeError("UNSTRUCTURED_OCR_URL is not set")

    image_buf = io.BytesIO()
    img.save(image_buf, format="PNG")
    image_buf.seek(0)

    response = requests.post(
        UNSTRUCTURED_OCR_URL,
        files={"file": ("frame.png", image_buf.getvalue(), "image/png")},
        timeout=UNSTRUCTURED_OCR_TIMEOUT,
    )
    response.raise_for_status()

    text = _extract_unstructured_text(response.json())
    return text.strip()


def _ocr_with_unstructured_local(img):
    image_buf = io.BytesIO()
    img.save(image_buf, format="PNG")
    image_buf.seek(0)

    # Use 'ocr_only' strategy to avoid layout-based filtering of text
    # Also set languages=['eng'] explicitly
    elements = partition_image(file=image_buf, strategy="ocr_only", languages=["eng"])
    text_parts = []
    for element in elements:
        element_text = str(getattr(element, "text", "") or "").strip()
        if element_text:
            text_parts.append(element_text)

    return "\n".join(text_parts).strip()


def run_ocr(img, provider):
    active_provider = (provider or "unstructured").strip().lower()

    if active_provider == "unstructured":
        try:
            mode = UNSTRUCTURED_OCR_MODE or "auto"
            if mode not in {"auto", "remote", "local"}:
                raise ValueError(
                    f"Invalid UNSTRUCTURED_OCR_MODE={mode}. Use one of: auto, remote, local"
                )

            if mode == "remote":
                return _ocr_with_unstructured(img)

            if mode == "local":
                return _ocr_with_unstructured_local(img)

            # auto mode: prefer configured URL, otherwise local in-process OCR.
            if UNSTRUCTURED_OCR_URL:
                return _ocr_with_unstructured(img)
            return _ocr_with_unstructured_local(img)
        except Exception:
            OCR_RUNTIME_STATS["unstructured_errors"] += 1
            raise

    raise ValueError(f"Unsupported OCR provider: {active_provider}")


def reset_ocr_runtime_stats():
    OCR_RUNTIME_STATS["unstructured_errors"] = 0


def get_ocr_runtime_stats():
    return dict(OCR_RUNTIME_STATS)


def clean_line(line):
    line = line.strip()
    if not line:
        return None

    # ── PROTECT (( )) double-paren lines — these are subtitle overlays ──
    # Extract and preserve them before any cleaning
    if "((" in line and "))" in line:
        letter_count = sum(1 for c in line if c.isalpha())
        if letter_count >= 3:
            # Strip only leading noise before the parens
            cleaned = re.sub(r'^[~&°•¢©®€£¥₹\*«»:>—\-=\s<>»~]+', '', line).strip()
            return cleaned if cleaned else line

    # ── AGGRESSIVE PREFIX STRIPPING (Bullets/UI Artifacts) ──
    # Target specific Unstructured/Tesseract common prefixes: Cc», tc», <C>, c>, ty, dd, &, |, >>>
    # We do multiple passes for nested symbols like <C>» or c> »
    prev_line = ""
    while line != prev_line:
        prev_line = line
        line = re.sub(r'^[<\[\(]?\s*[A-Za-z]{1,2}\s*[<>»~|:.,-]\s*[»>|]?\s*', '', line)
        line = re.sub(r'^[~&°•¢©®€£¥₹\*«»:>—\-=\s<>»~|]+', '', line)
        line = line.strip()

    if not line:
        return None

    # ── SKIP pure garbage / OCR noise lines ──
    if len(line) < 3:
        return None
    
    # ── SKIP standalone UI elements / Brand fragments ──
    # If a line is just a single word or short phrase that is a known UI label
    ui_labels = [
        'Handling', 'Rejected', 'Loan', 'Housing', 'Finance', 'Executive', 
        'Senior', 'Sales', 'BANEGA', 'DESH', 'WIN AT WORK', 'Relationship', 'Customer'
    ]
    if len(line) < 20 and any(line.lower() == label.lower() for label in ui_labels):
        return None
    
    # Also skip fragments that are just brand noise
    if len(line) < 15 and any(noise.lower() in line.lower() for noise in ['GHAR BANEGA', 'TOH DESH', 'WORK']):
        return None

    words = line.split()
    real_words = [w for w in words if len(w) >= 3 and any(c.isalpha() for c in w)]
    if len(real_words) == 0:
        return None
    
    # Skip lines that are just short lowercase or "camelCase" noise (Tesseract noise)
    if len(line) < 12 and (line.islower() or len(words) <= 2) and not any(v in line.lower() for v in 'aeiou'):
        return None
    
    # Generic junk words found in latest run
    junk_words = ['Pies', 'Saw', 'Prey', 'Yn Fae', 'Y YE ORR', 'sei', 'forn', 'rah']
    if any(junk.lower() in line.lower() for junk in junk_words) and len(line) < 15:
        return None

    # All-caps short line with no vowels = OCR noise (e.g. "lCUw", "TFN")
    if len(line) <= 8 and not re.search(r'[aeiouAEIOU]{1}', line):
        return None

    # Random char soup — high ratio of non-alpha non-space chars
    non_alpha = sum(1 for c in line if not c.isalpha() and not c.isspace())
    if len(line) > 0 and non_alpha / len(line) > 0.5:
        return None

    # ── SKIP watermark lines ──
    noise_exact = [
        'GHAR BANEGA, TOH DESH BANEGA.',
        'GHAR BANEGA,', 'TOH DESH BANEGA.',
        'WIN AT WORK', 'WIN AT WORK GHAR',
        'CL >N', 'CLe', 'CL >',
        'PPPP', 'N AT WORK', 'BANEGA,',
    ]
    for noise in noise_exact:
        if line.strip() == noise:
            return None

    if 'GHAR BANEGA' in line or 'TOH DESH BANEGA' in line:
        return None

    # ── SKIP repeated chars ──
    if re.search(r'(.)\1{3,}', line):
        return None

    # ── SKIP arrow graphics ──
    if '>>' in line:
        return None

    # ── STRIP trailing noise ──
    line = re.sub(r'\s+[A-Z][a-z]+\s+[A-Z][a-z]+$', '', line).strip()
    line = re.sub(r'\s+Senior\s+Executive.*$', '', line).strip()
    line = re.sub(r'\s+Relationship\s+Manager.*$', '', line).strip()
    line = re.sub(r'\s+[a-z]{1,2}$', '', line)
    line = re.sub(r'\s*\.\s*[0-9a-z]{1,3}$', '', line)
    line = re.sub(r'[i]{2,}', '', line)
    line = re.sub(r'\s+e+$', '', line)
    line = line.strip()

    if not line:
        return None

    # ── MINIMUM letter check ──
    letter_count = sum(1 for c in line if c.isalpha())
    if letter_count < 3:
        return None
    
    # ── TARGET more specific garbage patterns ──
    # If line is just a few chars of noise like "ct»" or "dd" or "‘a?"
    if len(line) < 5 and not any(v in line.lower() for v in 'aeiou'):
        return None
    
    # Catch weird Tesseract artifacts like ‘a? use UE EM e—
    if any(chunk in line for chunk in ["UE EM", "UE EM e", "‘a?"]):
        return None

    # ── SKIP common headers/titles always on screen ──
    if 'How can we calmly handle' in line:
        return None
    if 'Senior Executive' in line or 'Housing Finance' in line:
        return None
    if 'GHAR BANEGA' in line or 'TOH DESH BANEGA' in line:
        return None

    return line


def _normalize_for_dedup(line):
    marker = ""
    if OCR_PRESERVE_PARENS and "((" in line and "))" in line:
        marker = "paren:"
    normalized = re.sub(r"[^a-z0-9]+", " ", line.lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return f"{marker}{normalized}" if normalized else normalized


def _is_similar_text(a, b, threshold):
    if not a or not b:
        return False
    return SequenceMatcher(None, a, b).ratio() >= threshold


def preprocess_frame(frame):
    """Enhance frame for better OCR: upscale (2.5x) + grayscale + sharpening."""
    if frame is None:
        return None
    
    # 1. Upscale (2.5x) - provides more pixels for Tesseract/Paddle to work with
    height, width = frame.shape[:2]
    new_size = (int(width * 2.5), int(height * 2.5))
    frame = cv2.resize(frame, new_size, interpolation=cv2.INTER_CUBIC)
    
    # 2. Grayscale
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    
    # 3. Unsharp Mask (Sharpening)
    # This enhances edges which is critical for smaller fonts
    gaussian_3 = cv2.GaussianBlur(gray, (0, 0), 2.0)
    unsharp_image = cv2.addWeighted(gray, 1.8, gaussian_3, -0.8, 0)
    
    return unsharp_image


def _frame_hash(pil_img):
    size = max(2, OCR_HASH_SIZE)
    gray = pil_img.convert("L").resize((size, size), Image.BILINEAR)
    pixels = list(gray.getdata())
    avg = sum(pixels) / len(pixels)
    return "".join("1" if p > avg else "0" for p in pixels)


def extract_screen_text(video_path, provider=None):
    video_name = os.path.basename(video_path)
    active_provider = (provider or "unstructured").strip().lower()
    logger.info(
        "OCR started: %s | provider=%s | interval=%.2fs | scale=%.2f | clean=%s | dedup=%s",
        video_name,
        active_provider,
        FRAME_INTERVAL,
        OCR_SCALE,
        OCR_CLEAN_LINES,
        OCR_DEDUP_LINES,
    )
    if OCR_DEBUG:
        logger.info("OCR debug enabled")
        logger.info(
            "OCR dedup similarity=%.2f | skip_same_frame=%s",
            OCR_DEDUP_SIMILARITY,
            OCR_SKIP_SAME_FRAME,
        )
    if OCR_MAX_FRAMES > 0:
        logger.info("OCR max frames: %d", OCR_MAX_FRAMES)
    if OCR_CROP_MODE != "none":
        logger.info("OCR crop: mode=%s | ratio=%.2f", OCR_CROP_MODE, OCR_CROP_RATIO)

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_step = int(fps * FRAME_INTERVAL)
    if frame_step == 0:
        frame_step = 1

    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    total_seconds = int(total_frames / fps) if fps > 0 else 0
    logger.info(f"Video: {total_seconds}s | FPS: {fps:.1f} | Frame step: {frame_step}")

    all_lines_ordered = []
    frame_count = 0
    frames_processed = 0
    start_time = time.time()
    raw_chars_total = 0
    raw_lines_total = 0
    kept_lines_total = 0
    last_frame_hash = None

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_count % frame_step == 0:
            if OCR_SCALE and OCR_SCALE != 1.0:
                frame = cv2.resize(
                    frame,
                    None,
                    fx=OCR_SCALE,
                    fy=OCR_SCALE,
                    interpolation=cv2.INTER_AREA,
                )
            if OCR_CROP_MODE == "bottom":
                height = frame.shape[0]
                crop_start = int(height * max(0.0, min(OCR_CROP_RATIO, 1.0)))
                frame = frame[crop_start:, :]
            # Preprocess for better OCR
            processed_frame = preprocess_frame(frame)
            img = Image.fromarray(processed_frame)

            if OCR_SKIP_SAME_FRAME:
                current_hash = _frame_hash(img)
                if current_hash == last_frame_hash:
                    if OCR_DEBUG:
                        logger.info("Frame %d | skipped (same hash)", frame_count)
                    frame_count += 1
                    continue
                last_frame_hash = current_hash
            raw_text = run_ocr(img, active_provider)
            # ── DEBUGGING: Log raw OCR output before any cleaning ──
            if raw_text.strip():
                logger.info("FRAME %d | RAW OCR: %s", frame_count, raw_text.replace('\n', ' | '))

            raw_chars_total += len(raw_text)
            # Split by newline and pipe (|) since Unstructured uses pipes for layout blocks
            raw_lines = re.split(r'[\n|]+', raw_text)
            for line in raw_lines:
                clean = clean_line(line)
                if clean:
                    all_lines_ordered.append(clean)
                    kept_lines_total += 1
            
            # Update runtime stats
            OCR_RUNTIME_STATS["processed_frames"] += 1
            OCR_RUNTIME_STATS["last_frame_processed"] = frame_count
            if OCR_MAX_FRAMES > 0 and frames_processed >= OCR_MAX_FRAMES:
                break

        frame_count += 1

    cap.release()

    if OCR_DEDUP_LINES:
        unique_ordered = []
        for i, line in enumerate(all_lines_ordered):
            # 1. Fuzzy Subsegment Consolidation
            # Removes lines whose key words are already contained within a longer line
            is_fragment = False
            words = set(re.findall(r'\w+', line.lower()))
            if len(words) < 2:  # Single words are easily fragmented
                is_fragment = True 
            else:
                for j, other in enumerate(all_lines_ordered):
                    if i != j and len(other) > len(line):
                        other_words = set(re.findall(r'\w+', other.lower()))
                        # If more than 75% of our words are in the other line, we are likely a fragment
                        common = words.intersection(other_words)
                        if len(common) / len(words) > 0.75:
                            is_fragment = True
                            break
            
            if is_fragment:
                continue
            
            # 2. Similarity Dedup against already kept lines
            if any(_is_similar_text(line, existing, OCR_DEDUP_SIMILARITY) for existing in unique_ordered):
                continue
                
            unique_ordered.append(line)
        
        final_lines = unique_ordered
    else:
        final_lines = list(all_lines_ordered)

    elapsed = time.time() - start_time
    logger.info(
        "OCR done: %d frames | raw_chars=%d | raw_lines=%d | kept_lines=%d | final_lines=%d | %.1fs",
        frames_processed,
        raw_chars_total,
        raw_lines_total,
        kept_lines_total,
        len(final_lines),
        elapsed,
    )

    # Using double newline for the final narrative look as in ocrxmaple.txt
    final_out = '\n\n'.join(final_lines) if final_lines else "No screen text found"
    return final_out

def _transcribe_with_sarvam(video_path: str, api_key: str) -> str:
    """Extract audio and transcribe via Sarvam AI. Returns transcript string."""
    import subprocess
    import tempfile
    from sarvamai import SarvamAI

    sarvam_client = SarvamAI(api_subscription_key=api_key)

    # Step 1: extract mono 16k mp3 from video
    tmp_dir = tempfile.mkdtemp()
    audio_path = os.path.join(tmp_dir, "audio.mp3")

    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000",
        "-c:a", "libmp3lame", audio_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr[-300:]}")

    # Step 2: split into 28s chunks (Sarvam sync limit)
    chunk_dir = os.path.join(tmp_dir, "chunks")
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_pattern = os.path.join(chunk_dir, "chunk_%03d.mp3")

    cmd2 = [
        "ffmpeg", "-y", "-i", audio_path,
        "-f", "segment", "-segment_time", "28",
        "-ac", "1", "-ar", "16000",
        "-c:a", "libmp3lame", chunk_pattern,
    ]
    result2 = subprocess.run(cmd2, capture_output=True, text=True)
    if result2.returncode != 0:
        raise RuntimeError(f"ffmpeg chunk split failed: {result2.stderr[-300:]}")

    chunks = sorted(
        os.path.join(chunk_dir, f)
        for f in os.listdir(chunk_dir)
        if f.lower().endswith(".mp3")
    )

    if not chunks:
        raise RuntimeError("No audio chunks produced")

    # Step 3: transcribe each chunk
    parts = []
    for i, chunk_path in enumerate(chunks, 1):
        try:
            with open(chunk_path, "rb") as f:
                response = sarvam_client.speech_to_text.translate(
                    file=f,
                    model="saaras:v2.5",
                    input_audio_codec="mp3",
                )
            # handle response variants
            text = ""
            if hasattr(response, "transcript") and response.transcript:
                text = str(response.transcript).strip()
            elif hasattr(response, "text") and response.text:
                text = str(response.text).strip()
            elif isinstance(response, dict):
                text = str(response.get("transcript") or response.get("text") or "").strip()

            if text:
                parts.append(text)
            logger.info(f"Sarvam chunk {i}/{len(chunks)} | {len(text)} chars")
        except Exception as e:
            logger.warning(f"Sarvam chunk {i} failed: {e}")

    return " ".join(parts).strip()

def extract_audio_transcript(video_path):
    """
    Transcribes audio using Sarvam AI (primary).
    Falls back to Whisper if Sarvam key not set or fails.
    Never crashes — returns empty string on total failure.
    """
    video_name = os.path.basename(video_path)
    logger.info(f"Transcript started: {video_name}")
    start_time = time.time()

    sarvam_key = os.getenv("SARVAM_API_KEY", "").strip()

    # ── PRIMARY: Sarvam AI ──
    if sarvam_key:
        try:
            transcript = _transcribe_with_sarvam(video_path, sarvam_key)
            if transcript and len(transcript.strip()) > 10:
                elapsed = round(time.time() - start_time, 2)
                logger.info(f"Sarvam done | {len(transcript)} chars | {elapsed}s")
                return transcript.strip()
            else:
                logger.warning("Sarvam returned empty — falling back to Whisper")
        except Exception as e:
            logger.warning(f"Sarvam failed ({e}) — falling back to Whisper")

    # ── FALLBACK: Whisper ──
    try:
        logger.info("Using Whisper fallback")
        whisper_model = get_whisper_model()
        result = whisper_model.transcribe(
            video_path,
            task="transcribe",
            fp16=False
        )
        transcript = result["text"].strip()
        language = result["language"]
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Whisper done | lang={language} | {len(transcript)} chars | {elapsed}s")
        if transcript:
            return transcript
    except Exception as e:
        logger.warning(f"Whisper also failed: {e}")

    # ── NEVER CRASH — return empty, pipeline continues ──
    logger.warning(
        f"Both Sarvam and Whisper failed for {video_name}. "
        "Continuing with empty transcript."
    )
    return ""

def get_video_duration(video_path):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    cap.release()
    total_seconds = int(total_frames / fps) if fps > 0 else 0
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def process_all_videos():
    video_extensions = ('.mp4', '.mov', '.avi', '.mkv', '.m4v')
    videos = sorted([
        f for f in os.listdir(VIDEO_FOLDER)
        if f.lower().endswith(video_extensions)
    ])

    logger.info(f"Found {len(videos)} videos")
    logger.info("=" * 50)

    results     = []
    total_start = time.time()

    for i, video_file in enumerate(videos, 1):
        video_path = os.path.join(VIDEO_FOLDER, video_file)
        video_name = os.path.splitext(video_file)[0]

        logger.info(f"[{i}/{len(videos)}] START: {video_file}")

        screen_text = extract_screen_text(video_path)
        transcript  = extract_audio_transcript(video_path)
        duration    = get_video_duration(video_path)

        results.append({
            "video_name":  video_name,
            "file_name":   video_file,
            "duration":    duration,
            "screen_text": screen_text,
            "transcript":  transcript,
        })

        logger.info(f"[{i}/{len(videos)}] DONE: {video_file}")
        logger.info("-" * 50)

    total_elapsed = time.time() - total_start
    logger.info(f"All {len(videos)} videos done in {total_elapsed:.1f}s")

    return results





# ── RUN ───────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("CLAN Video Extraction Pipeline started")
    logger.info(f"OCR: unstructured | Audio: Whisper {WHISPER_MODEL}")
    results = process_all_videos()
    logger.info("Pipeline complete. Metadata extracted and ready for storage")
    for result in results:
        logger.info(f"✓ {result['video_name']}: {len(result['screen_text'])} chars OCR | {len(result['transcript'])} chars transcript")