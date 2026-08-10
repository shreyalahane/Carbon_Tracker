"""Advisor Agent.

Reads today's prediction plus current air quality and carbon intensity,
then produces actionable emission-reduction recommendations. Uses Claude
when an API key is available, otherwise a deterministic rule-based engine.
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


def load_today_context():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    today = date.today().isoformat()

    cursor.execute(
        "SELECT date, predicted_co2_kg, confidence, risk_level "
        "FROM predictions WHERE date = %s ORDER BY created_at DESC LIMIT 1",
        (today,))
    prediction = cursor.fetchone()

    cursor.execute(
        "SELECT date, carbon_intensity, fossil_fuel_percentage, index_value "
        "FROM carbon_intensity_data ORDER BY fetched_at DESC LIMIT 1")
    carbon = cursor.fetchone()

    cursor.execute(
        "SELECT date, pm2_5, nitrogen_dioxide, carbon_monoxide "
        "FROM air_quality_data ORDER BY date DESC LIMIT 1")
    air = cursor.fetchone()

    cursor.execute(
        "SELECT date, temperature_mean, precipitation, wind_speed "
        "FROM weather_data ORDER BY date DESC LIMIT 1")
    weather = cursor.fetchone()

    cursor.close()
    conn.close()

    return {
        "prediction": prediction or {},
        "carbon": carbon or {},
        "air": air or {},
        "weather": weather or {},
        "generated_on": datetime.now().isoformat(),
    }


def rule_based_advice(context):
    """Deterministic recommendations derived from thresholds."""
    recommendations = []

    prediction = context["prediction"]
    carbon = context["carbon"]
    air = context["air"]
    weather = context["weather"]

    risk = (prediction or {}).get("risk_level", "")
    if risk:
        recommendations.append(
            f"Tomorrow's predicted risk level is '{risk}'. "
            "Prioritize low-carbon operations for the next 24 hours.")

    intensity = (carbon or {}).get("carbon_intensity") or 0
    if intensity:
        if intensity > 250:
            recommendations.append(
                f"Grid carbon intensity is high ({intensity} gCO2/kWh). "
                "Shift heavy electricity use to off-peak or renewable hours.")
        elif intensity < 100:
            recommendations.append(
                f"Grid is clean ({intensity} gCO2/kWh). "
                "Schedule energy-intensive tasks now.")

    pm25 = (air or {}).get("pm2_5") or 0
    if pm25:
        if pm25 > 50:
            recommendations.append(
                f"PM2.5 is elevated ({pm25} ug/m3). "
                "Reduce transport during peak hours and avoid idling vehicles.")
        else:
            recommendations.append(
                "Air quality is moderate today. "
                "Maintain current transport scheduling.")

    temp = (weather or {}).get("temperature_mean")
    if temp is not None:
        if temp > 32:
            recommendations.append(
                "High temperature detected. Optimize AC setpoints "
                "and pre-cool during low-intensity hours.")
        elif temp < 12:
            recommendations.append(
                "Low temperature detected. Audit heating usage "
                "to avoid fossil-fuel overuse.")

    if not recommendations:
        recommendations.append(
            "Conditions are within normal ranges. Continue current "
            "operations and re-evaluate after the next data refresh.")

    return {
        "summary": "Rule-based recommendations generated from live sensor data.",
        "recommendations": recommendations,
        "source": "rule_based",
    }


def generate_advice():
    context = load_today_context()
    llm = get_llm()

    advice = None
    if llm is not None:
        system_prompt = (
            "You are a sustainability advisor for a company tracking "
            "carbon emissions. Give 4-6 specific, actionable recommendations "
            "in JSON with keys 'summary' and 'recommendations' (a list of "
            "strings). Base everything only on the provided data."
        )
        user_prompt = json.dumps(context, default=str)
        llm_text = run_llm(llm, system_prompt, user_prompt)
        if llm_text:
            try:
                parsed = json.loads(llm_text)
                advice = {
                    "summary": parsed.get("summary", "AI recommendations."),
                    "recommendations": parsed.get("recommendations", []),
                    "source": "claude",
                }
            except json.JSONDecodeError:
                advice = {
                    "summary": "AI recommendations.",
                    "recommendations": [llm_text],
                    "source": "claude",
                }

    if advice is None:
        advice = rule_based_advice(context)

    document = {**context, **advice}
    ok = save_to_mongodb(MONGO_COLLECTIONS["advice"], document)
    print(f"Advisor agent: {len(advice['recommendations'])} "
          f"recommendations ({advice['source']}), saved={ok}")
    return document


if __name__ == "__main__":
    generate_advice()
