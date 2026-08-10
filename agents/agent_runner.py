"""Runs all three AI agents in sequence.

Used by Airflow as an optional nightly task and runnable directly:
    python agents/agent_runner.py
"""


def run_all():
    from advisor_agent import generate_advice
    from planner_agent import generate_plan
    from report_agent import generate_report

    print("=" * 50)
    print("Running Advisor Agent")
    print("=" * 50)
    generate_advice()

    print("=" * 50)
    print("Running Planner Agent")
    print("=" * 50)
    generate_plan()

    print("=" * 50)
    print("Running Report Agent")
    print("=" * 50)
    generate_report()

    print("\nAll agents complete.")


if __name__ == "__main__":
    run_all()
