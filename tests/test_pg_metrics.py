from notifications.metric_provider import PostgresMetricProvider
import json

def test_fetch_metrics_with_indicator():
    provider = PostgresMetricProvider()
    user_id = "RM_204"
    
    print(f"--- Testing for User: {user_id} ---")
    
    # 1. Test without indicator (Global count)
    print("\n[Test 1] No Indicator (Global activities)...")
    metrics_all = provider.fetch_user_metrics(user_id)
    print(f"Yesterday Count (All): {metrics_all.get('yesterday_count')}")
    
    # 2. Test with 'customer_generation' indicator
    indicator = "customer_generation"
    print(f"\n[Test 2] Indicator: {indicator}...")
    metrics_filtered = provider.fetch_user_metrics(user_id, indicator=indicator)
    print(f"Yesterday Count ({indicator}): {metrics_filtered.get('yesterday_count')}")
    
    # 3. Test Streak (Confirming it's for 'yesterday')
    print(f"\n[Test 3] Streak (Up to yesterday): {metrics_all.get('user_streak')}")
    
    # 4. Test Ranks (Weekly)
    print(f"\n[Test 4] Rank Details:")
    print(f"Current Rank: {metrics_all.get('current_rank')}")
    print(f"Previous Rank: {metrics_all.get('previous_rank')}")

if __name__ == "__main__":
    test_fetch_metrics_with_indicator()
