import pendulum
from dagster_dag_factory.factory.helpers.macros import CronMacros

def test_cron_macros():
    cron = CronMacros()
    base_date = "2023-01-01"
    schedule = "0 0 * * *" # Daily at midnight
    
    next_val = cron.next(schedule, base_date)
    print(f"Next: {next_val}")
    assert "2023-01-02" in next_val
    
    prev_val = cron.prev(schedule, base_date)
    print(f"Prev: {prev_val}")
    assert "2022-12-31" in prev_val
    
    diff_val = cron.diff("2023-01-01", "2023-01-05", "days")
    print(f"Diff (days): {diff_val}")
    assert diff_val == 5
    
    range_val = cron.range("2023-01-01", "2023-01-03", schedule)
    print(f"Range: {range_val}")
    assert len(range_val) == 2 # 01-01 and 01-02 (end is exclusive in croniter_range usually)
    
    print("SUCCESS: Cron macros verified.")

if __name__ == "__main__":
    test_cron_macros()
