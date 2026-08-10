"""Report Agent.

Reads the last 30 days of data and produces an ESG compliance report with
a Scope 1/2/3 breakdown. Saves the report to MongoDB and generates a
downloadable PDF via reportlab.
"""

from datetime import date, datetime
from pathlib import Path
import json

from common import (
    BASE_DIR,
    get_mysql_connection,
    get_llm,
    run_llm,
    save_to_mongodb,
    MONGO_COLLECTIONS,
)

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# Emission factors (aligned with ml/train_model.py)
ELECTRICITY_FACTOR = 0.233   # kg CO2 per kWh
TRANSPORT_FACTOR = 2.31      # kg CO2 per litre petrol
INDUSTRIAL_FACTOR = 0.5      # kg CO2 per unit


def load_last_30_days():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT e.date, e.total_co2_kg, e.carbon_intensity, e.pm2_5,
               e.nitrogen_dioxide, e.temperature_mean,
               a.carbon_monoxide
        FROM emissions e
        LEFT JOIN air_quality_data a ON a.date = e.date
        ORDER BY e.date DESC LIMIT 30
    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return list(reversed(rows))


def compute_breakdown(rows):
    if not rows:
        return {}

    total = sum(float(r.get("total_co2_kg") or 0) for r in rows)
    scope2 = sum(
        float(r.get("carbon_intensity") or 0) * 1000 * ELECTRICITY_FACTOR
        for r in rows)
    transport = sum(
        float(r.get("nitrogen_dioxide") or 0) * 10 * TRANSPORT_FACTOR
        for r in rows)
    industrial = sum(
        float(r.get("carbon_monoxide") or 0) * INDUSTRIAL_FACTOR
        for r in rows)

    # Scope 1 = on-site direct emissions not captured by sensor proxies
    scope3_proxy = transport
    scope1 = max(total - scope2 - scope3_proxy, 0.0)
    scope3 = scope3_proxy

    avg_daily = total / len(rows) if rows else 0.0
    best_day = min(rows, key=lambda r: float(r.get("total_co2_kg") or 0))
    worst_day = max(rows, key=lambda r: float(r.get("total_co2_kg") or 0))

    return {
        "total_co2_kg": round(total, 2),
        "avg_daily_co2_kg": round(avg_daily, 2),
        "scope1_kg": round(scope1, 2),
        "scope2_kg": round(scope2, 2),
        "scope3_kg": round(scope3, 2),
        "best_day": {
            "date": best_day.get("date").isoformat() if best_day.get("date") else None,
            "total_co2_kg": round(float(best_day.get("total_co2_kg") or 0), 2),
        },
        "worst_day": {
            "date": worst_day.get("date").isoformat() if worst_day.get("date") else None,
            "total_co2_kg": round(float(worst_day.get("total_co2_kg") or 0), 2),
        },
    }


def rule_based_summary(breakdown):
    if not breakdown:
        return "Insufficient data to generate an ESG summary."

    total = breakdown["total_co2_kg"]
    if total == 0:
        return "No measurable emissions in the reporting window."
    if breakdown["scope2_kg"] > 0.5 * total:
        return ("Electricity (Scope 2) dominates your footprint. "
                "Prioritize renewable procurement and load shifting.")
    return ("Emissions are spread across scopes; focus on transport "
            "and electricity optimization for the next cycle.")


def generate_pdf(breakdown, summary, window):
    filename = f"esg_report_{window['end_date']}.pdf"
    filepath = REPORTS_DIR / filename

    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Spacer

    styles = getSampleStyleSheet()

    doc = SimpleDocTemplate(str(filepath), pagesize=A4)
    story = [
        Paragraph("Carbon Tracker - ESG Compliance Report", styles['Title']),
        Spacer(1, 12),
        Paragraph(
            f"Reporting window: {window['start_date']} to {window['end_date']}",
            styles['Normal']),
        Spacer(1, 12),
        Paragraph("Scope Breakdown (estimated, kg CO2e)", styles['Heading2']),
        Table(
            [
                ["Scope", "Emissions (kg CO2e)"],
                ["Scope 1 (direct)", f"{breakdown.get('scope1_kg', 0):.2f}"],
                ["Scope 2 (electricity)", f"{breakdown.get('scope2_kg', 0):.2f}"],
                ["Scope 3 (value chain)", f"{breakdown.get('scope3_kg', 0):.2f}"],
                ["Total", f"{breakdown.get('total_co2_kg', 0):.2f}"],
            ],
            colWidths=[200, 150],
        ),
        Spacer(1, 16),
        Paragraph("Summary", styles['Heading2']),
        Paragraph(summary, styles['Normal']),
        Spacer(1, 16),
        Paragraph(
            f"Best day: {breakdown.get('best_day', {}).get('date')} "
            f"({breakdown.get('best_day', {}).get('total_co2_kg', 0)} kg)",
            styles['Normal']),
        Paragraph(
            f"Worst day: {breakdown.get('worst_day', {}).get('date')} "
            f"({breakdown.get('worst_day', {}).get('total_co2_kg', 0)} kg)",
            styles['Normal']),
        Spacer(1, 12),
        Paragraph(
            "Note: breakdowns are model estimates derived from sensor proxies.",
            styles['Italic']),
    ]
    doc.build(story)
    return filename, str(filepath)


def generate_report():
    rows = load_last_30_days()
    today = date.today()
    window = {
        "start_date": (today.replace(day=1)).isoformat(),
        "end_date": today.isoformat(),
    }

    breakdown = compute_breakdown(rows)
    llm = get_llm()

    summary = None
    if llm is not None and breakdown:
        system_prompt = (
            "You are an ESG reporting analyst. Write a concise 3-4 sentence "
            "compliance summary from the given monthly carbon data. Return "
            "plain text only."
        )
        user_prompt = json.dumps({"breakdown": breakdown, "window": window},
                                 default=str)
        summary = run_llm(llm, system_prompt, user_prompt)

    if not summary:
        summary = rule_based_summary(breakdown)

    filename, filepath = generate_pdf(breakdown, summary, window)

    document = {
        "window": window,
        "breakdown": breakdown,
        "summary": summary,
        "pdf_file": filename,
        "pdf_path": filepath,
        "generated_on": datetime.now().isoformat(),
    }
    ok = save_to_mongodb(MONGO_COLLECTIONS["report"], document)
    print(f"Report agent: PDF={filename}, saved={ok}")
    return document


if __name__ == "__main__":
    generate_report()
