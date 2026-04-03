import unittest

from notifications.video_selector import VideoSelector


class VideoSelectorLanguageTests(unittest.TestCase):
    def test_match_language_strict_when_metadata_exists(self):
        selector = VideoSelector()
        videos = [
            {"video_id": 1, "language_name": "hi", "Title": "A"},
            {"video_id": 2, "language_name": "te", "Title": "B"},
            {"video_id": 3, "language_name": "hi", "Title": "C"},
        ]

        out = selector._match_language(videos, "hindi")
        ids = {v["video_id"] for v in out}
        self.assertEqual(ids, {1, 3})

    def test_match_language_fallback_when_metadata_missing(self):
        selector = VideoSelector()
        videos = [
            {"video_id": 1, "language_name": "", "Title": "A"},
            {"video_id": 2, "Title": "B"},
        ]

        out = selector._match_language(videos, "hindi")
        ids = {v["video_id"] for v in out}
        self.assertEqual(ids, {1, 2})


if __name__ == "__main__":
    unittest.main()
