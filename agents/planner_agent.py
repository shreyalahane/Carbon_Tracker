"""Planner Agent.

Reads the last 7 days of emissions and weather data and produces a
day-by-day weekly reduction strategy. Uses Claude when available, with
a rule-based fallback otherwise.
"""

from datetime import date, datetime
import json

from common import (
    get_mysql_connection,
    get_llm,
    run_llm,
    save_to_mongodb,
    MONGO_COLLECTIONS,
)


def load_last_7_days():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT date, total_co2_kg, carbon_intensity, pm2_5,
               temperature_mean
        FROM emissions
        ORDER BY date DESC LIMIT 7
    """)
    emissions = cursor.fetchall()

    cursor.close()
    conn.close()

    emissions = list(reversed(emissions))
    return {
        "start_date": (date.today()).isoformat(),
        "days": emissions,
        "generated_on": datetime.now().isoformat(),
    }


def rule_based_plan(context):
    days = context["days"]
    if not days:
        return {
            "summary": "Not enough data for a weekly plan.",
            "plan": [],
        }

    avg_co2 = sum(float(d.get("total_co2_kg") or 0) for d in days) / len(days)
    avg_intensity = sum(float(d.get("carbon_intensity") or 0) for d in days) / len(days)
    peak_day = max(days, key=lambda d: float(d.get("total_co2_kg") or 0))

    plan = [
        {
            "day": 1,
            "focus": "Energy audit",
            "action": f"Compare energy use against the grid forecast; "
                      f"average intensity is {avg_intensity:.1f} gCO2/kWh this week.",
            "target_saving": "5%",
        },
        {
            "day": 2,
            "focus": "Transport shift",
            "action": "Move deliveries to off-peak hours and promote "
                      "pooled transport where PM2.5 readings are highest.",
            "target_saving": "5%",
        },
        {
            "day": 3,
            "focus": "Renewable usage",
            "action": "Run heavy machinery during low-intensity windows "
                      "and shift to solar/wind-backed power.",
            "target_saving": "8%",
        },
        {
            "day": 4,
            "focus": "HVAC optimization",
            "action": "Adjust setpoints based on temperature data and "
                      "pre-cool during clean-grid hours.",
            "target_saving": "6%",
        },
        {
            "day": 5,
            "focus": "Employee engagement",
            "action": "Share daily CO2 numbers and simple actions "
                      "with the team to build awareness.",
            "target_saving": "3%",
        },
        {
            "day": 6,
            "focus": "Supplier review",
            "action": "Flag any supplier operations running during "
                      "peak carbon-intensity hours.",
            "target_saving": "4%",
        },
        {
            "day": 7,
            "focus": "Review & report",
            "action": f"Compile the week's results, compare against the "
                      f"{avg_co2:.1f} kg/day baseline, and plan next week.",
            "target_saving": "—",
        },
    ]

    return {
        "summary": (
            f"Weekly strategy targeting the highest emitter "
            f"({peak_day.get('date')}: {float(peak_day.get('total_co2_kg') or 0):.1f} kg)."
        ),
        "plan": plan,
        "source": "rule_based",
    }


def generate_plan():
    context = load_last_7_days()
    llm = get_llm(temperature=0.4)

    plan = None
    if llm is not None:
        system_prompt = (
            "You are a carbon reduction planner. Based on the last 7 days "
            "of data, produce a 7-day reduction strategy. Return strict JSON "
            "with keys 'summary' (string) and 'plan' (list of objects with "
            "'day', 'focus', 'action', 'target_saving')."
        )
        user_prompt = json.dumps(context, default=str)
        llm_text = run_llm(llm, system_prompt, user_prompt)
        if llm_text:
            try:
                parsed = json.loads(llm_text)
                plan = {
                    "summary": parsed.get("summary", "Weekly reduction strategy."),
                    "plan": parsed.get("plan", []),
                    "source": "claude",
                }
            except json.JSONDecodeError:
                plan = {
                    "summary": "Weekly reduction strategy.",
                    "plan": [{"day": 0, "focus": "AI plan", "action": llm_text,
                              "target_saving": "—"}],
                    "source": "claude",
                }

    if plan is None:
        plan = rule_based_plan(context)

    document = {**context, **plan}
    ok = save_to_mongodb(MONGO_COLLECTIONS["plan"], document)
    print(f"Planner agent: {len(plan['plan'])} days planned, saved={ok}")
    return document


if __name__ == "__main__":
    generate_plan()
