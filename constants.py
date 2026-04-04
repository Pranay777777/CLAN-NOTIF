ACCOUNT_ID = 14

EXCLUDED_VIDEO_TITLES = (
    "Abhishek Sahu - An Intro",
    "Kasibabu Kanchi - An Intro",
    "Charan Teja - An Intro",
)

LANGUAGE_ALIASES = {
    "en": "english",
    "eng": "english",
    "english (india)": "english",
    "hi": "hindi",
    "te": "telugu",
    "ar": "arabic",
}


def normalize_language(value: str, default: str = "english") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return LANGUAGE_ALIASES.get(text, text)


def is_excluded_video(title: str) -> bool:
    title_str = str(title).strip().lower()
    return any(excluded.lower() in title_str for excluded in EXCLUDED_VIDEO_TITLES)
