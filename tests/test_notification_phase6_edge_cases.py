from notifications.models import NotificationRequest
from notifications.service import NotificationService


def assert_true(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def run_case(name: str, fn):
    try:
        fn()
        print(f"PASS: {name}")
        return True
    except Exception as exc:
        print(f"FAIL: {name} -> {exc}")
        return False


def main():
    service = NotificationService()
    base = {
        "user_id": 1001,
        "user_name": "Aarav",
        "region": "Mumbai",
        "language": "en",
        "campaign_day": 12,
        "creator_name": "Shikha",
        "creator_team": "Andheri branch",
        "outcome_hint": "improve lead conversion",
    }

    results = []

    def case_missing_creator_name():
        req = NotificationRequest(**{**base, "creator_name": None})
        res = service.build_notification(req)
        assert_true("Aarav" in res.notification_body, "personalized body should include user name")
        assert_true(len(res.notification_body) <= 120, "body must stay <= 120 chars")

    results.append(run_case("Missing creator name", case_missing_creator_name))

    def case_very_long_user_name():
        long_name = "A" * 180
        req = NotificationRequest(**{**base, "user_name": long_name})
        res = service.build_notification(req)
        assert_true(len(res.notification_body) <= 120, "body must stay <= 120 chars")
        assert_true("..." in res.notification_body or "A" in res.notification_body, "body should contain a safe name rendering")

    results.append(run_case("Very long user name", case_very_long_user_name))

    def case_unsupported_language_code():
        req = NotificationRequest(**{**base, "campaign_day": 2, "language": "xx"})
        res = service.build_notification(req)
        assert_true(":en:" in res.cohort_key, "unsupported language should normalize to en")

    results.append(run_case("Unsupported language code", case_unsupported_language_code))

    def case_empty_outcome_hint():
        req = NotificationRequest(**{**base, "outcome_hint": "   "})
        res = service.build_notification(req)
        assert_true("improve conversions" in res.notification_body.lower(), "empty outcome_hint should fallback")

    results.append(run_case("Empty outcome hint", case_empty_outcome_hint))

    def case_video_title_too_long():
        long_title = "How to Handle Objections " * 20
        req = NotificationRequest(**{**base, "video_title": long_title})
        res = service.build_notification(req)
        assert_true(len(res.notification_body) <= 120, "body must stay <= 120 chars with long title")

    results.append(run_case("Video title too long", case_video_title_too_long))

    def case_body_truncate_safely():
        req = NotificationRequest(
            **{
                **base,
                "outcome_hint": "increase conversions while reducing drop-offs and improving trust with multi-step follow-ups " * 5,
                "video_title": "Very Long Video Title " * 15,
            }
        )
        res = service.build_notification(req)
        assert_true(len(res.notification_body) <= 120, "body must truncate to <= 120 chars")
        assert_true(not res.notification_body.endswith(" "), "body should not end with trailing spaces")

    results.append(run_case("Notification body over 120 chars", case_body_truncate_safely))

    def case_generic_phrasing_detection():
        req = NotificationRequest(**{**base, "campaign_day": 4})
        res = service.build_notification(req)
        banned = ["today's 2-minute tip", "top rm from your region"]
        body_l = res.notification_body.lower()
        assert_true(all(b not in body_l for b in banned), "generic phrasing should be avoided")

    results.append(run_case("Generic phrasing detection", case_generic_phrasing_detection))

    def case_user_name_in_personalized_body():
        req = NotificationRequest(**base)
        res = service.build_notification(req)
        assert_true("Aarav" in res.notification_body, "user name must appear in personalized body")

    results.append(run_case("User name appears in body", case_user_name_in_personalized_body))

    def case_duplicate_same_day_idempotency():
        req = NotificationRequest(**{**base, "user_id": 7777})
        first = service.build_notification(req)
        second = service.build_notification(req)
        assert_true(first.model_dump() == second.model_dump(), "duplicate same-day call should be idempotent")

    results.append(run_case("Duplicate same-day idempotency", case_duplicate_same_day_idempotency))

    def case_special_chars_and_emoji_name():
        req = NotificationRequest(**{**base, "user_name": "Ana-Maria 😊"})
        res = service.build_notification(req)
        assert_true("Ana-Maria" in res.notification_body, "special characters name should be preserved")

    results.append(run_case("Special characters and emoji in names", case_special_chars_and_emoji_name))

    def case_rtl_language_text():
        rtl_hint = "تحسين معدل التحويل مع متابعة العملاء"
        req = NotificationRequest(**{**base, "language": "ar", "outcome_hint": rtl_hint})
        res = service.build_notification(req)
        assert_true(len(res.notification_body) <= 120, "RTL body must stay <= 120 chars")
        assert_true("Aarav" in res.notification_body, "personalized RTL body should still include user name")

    results.append(run_case("Right-to-left language text", case_rtl_language_text))

    def case_null_region_and_team_fallback():
        req = NotificationRequest(
            user_id=8888,
            user_name="Aarav",
            region=None,
            language="en",
            campaign_day=12,
            video_id="manual_vid",
            creator_name="Shikha",
            creator_team=None,
            creator_region=None,
            outcome_hint="improve conversion",
        )
        res = service.build_notification(req)
        assert_true("network" in res.notification_body.lower(), "null team/region should fallback to network scope")

    results.append(run_case("Null region / null team fallback", case_null_region_and_team_fallback))

    def case_day_based_routing_mismatch():
        req_day2 = NotificationRequest(**{**base, "campaign_day": 2, "creator_name": "Someone", "outcome_hint": "x"})
        req_day4 = NotificationRequest(**{**base, "campaign_day": 4, "creator_name": "Someone", "outcome_hint": "x"})
        req_day12 = NotificationRequest(**{**base, "campaign_day": 12})
        r2 = service.build_notification(req_day2)
        r4 = service.build_notification(req_day4)
        r12 = service.build_notification(req_day12)
        assert_true(r2.notification_title.startswith("Day 2"), "day 2 must route to day 2 template")
        assert_true(r4.notification_title.startswith("Day 4"), "day 4 must route to day 4 template")
        assert_true("someone you know" in r12.notification_title.lower(), "day 12 must use personalized template")

    results.append(run_case("Day-based routing mismatch", case_day_based_routing_mismatch))

    def case_missing_selected_video_id():
        local_service = NotificationService()
        local_service.selector.select_for_campaign = lambda **kwargs: None
        req = NotificationRequest(
            user_id=9999,
            user_name="Aarav",
            region="Mumbai",
            language="en",
            campaign_day=2,
            video_id=None,
        )
        res = local_service.build_notification(req)
        assert_true(res.video_id is None, "missing selected video id should remain None")
        assert_true(bool(res.notification_body), "notification should still be generated")

    results.append(run_case("Missing selected video id", case_missing_selected_video_id))

    passed = sum(1 for x in results if x)
    total = len(results)
    print("-" * 72)
    print(f"PHASE6 EDGE CASES: {passed}/{total} passed")

    if passed != total:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
