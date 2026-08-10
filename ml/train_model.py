import pandas as pd
import numpy as np
import pymysql
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import shap
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import warnings
from sklearn.model_selection import TimeSeriesSplit
warnings.filterwarnings('ignore')
# calculate total CO2 emissions
# using emission factors
ELECTRICITY_FACTOR = 0.233  # kg CO2 per kWh
TRANSPORT_FACTOR = 2.31     # kg CO2 per liter petrol
INDUSTRIAL_FACTOR = 0.5     # kg CO2 per unit

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / "myfile.env")

print("KAFKA:", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
print("MYSQL:", os.getenv("MYSQL_HOST"))
# ── DATABASE CONNECTION ──────────────────────────────────────────

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )

# ── LOAD DATA FROM MYSQL ─────────────────────────────────────────

def load_data():
    conn = get_mysql_connection()

    weather_df = pd.read_sql("""
        SELECT date, temperature_mean, precipitation,
               wind_speed, solar_radiation
        FROM weather_data
        ORDER BY date
    """, conn)

    air_df = pd.read_sql("""
        SELECT date, pm2_5, nitrogen_dioxide, carbon_monoxide
        FROM air_quality_data
        ORDER BY date
    """, conn)

    carbon_df = pd.read_sql("""
    SELECT date,
           AVG(carbon_intensity) AS carbon_intensity,
           AVG(fossil_fuel_percentage) AS fossil_fuel_percentage
    FROM carbon_intensity_data
    GROUP BY date
    ORDER BY date
""", conn)

    conn.close()

    print(f"Weather rows: {len(weather_df)}")
    print(f"Air quality rows: {len(air_df)}")
    print(f"Carbon intensity rows: {len(carbon_df)}")

    return weather_df, air_df, carbon_df

# ── MERGE AND FEATURE ENGINEERING ───────────────────────────────

def prepare_features(weather_df, air_df, carbon_df):

    # convert date columns to datetime
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    air_df['date'] = pd.to_datetime(air_df['date'])
    carbon_df['date'] = pd.to_datetime(carbon_df['date'])

    # merge all 3 tables on date
    df = weather_df.merge(air_df, on='date', how='inner')
    df = df.merge(carbon_df, on='date', how='inner')

    print(f"Merged dataset rows: {len(df)}")

    if len(df) < 5:
        print("Not enough data to train. Need at least 5 days.")
        return None

    # sort by date
    df = df.sort_values('date').reset_index(drop=True)

    # ── FEATURE ENGINEERING ──────────────────────────────────────

# Date based features
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['is_weekend'] = df['day_of_week'].apply(
        lambda x: 1 if x >= 5 else 0
)
    df['day_of_year'] = df['date'].dt.dayofyear
    df['quarter'] = df['date'].dt.quarter


# ============================================================
# CO2 CALCULATION
# ============================================================

# Scope 2 CO2
    df["scope2_co2"] = (
        df["carbon_intensity"] * 1000 * ELECTRICITY_FACTOR
)

# Transport CO2
    df["transport_co2"] = (
    df["nitrogen_dioxide"] * 10 * TRANSPORT_FACTOR
)

# Industrial CO2
    df["industrial_co2"] = (
    df["carbon_monoxide"] * INDUSTRIAL_FACTOR
)

# Total CO2
    df["total_co2_kg"] = (
    df["scope2_co2"]
    + df["transport_co2"]
    + df["industrial_co2"]
)


# ============================================================
# CO2 LAG FEATURES
# ============================================================

    df["prev_day_co2"] = (
    df["total_co2_kg"].shift(1)
)

    df["prev_2_day_co2"] = (
    df["total_co2_kg"].shift(2)
)

    df["prev_3_day_co2"] = (
    df["total_co2_kg"].shift(3)
)


# ============================================================
# ROLLING FEATURES
# ============================================================

    df["temp_14day_avg"] = (
    df["temperature_mean"]
    .rolling(14, min_periods=1)
    .mean()
)

    df["carbon_14day_avg"] = (
    df["carbon_intensity"]
    .rolling(14, min_periods=1)
    .mean()
)

    df["temp_7day_avg"] = (
    df["temperature_mean"]
    .rolling(7, min_periods=1)
    .mean()
)

    df["pm25_7day_avg"] = (
    df["pm2_5"]
    .rolling(7, min_periods=1)
    .mean()
)


# ============================================================
# OTHER LAG FEATURES
# ============================================================

    df["prev_day_temp"] = (
    df["temperature_mean"].shift(1)
)

    df["prev_day_pm25"] = (
    df["pm2_5"].shift(1)
)


# ============================================================
# TEMPERATURE CATEGORY
# ============================================================

    df["temp_category"] = pd.cut(
    df["temperature_mean"],
    bins=[-50, 10, 20, 30, 60],
    labels=[0, 1, 2, 3]
).astype(float)


# ============================================================
# HIGH POLLUTION FLAG
# ============================================================

    df["high_pollution"] = (
    (df["pm2_5"] > 50) |
    (df["nitrogen_dioxide"] > 40)
).astype(int)


# ============================================================
# CARBON INTENSITY CATEGORY
# ============================================================

    df["intensity_category"] = pd.cut(
    df["carbon_intensity"],
    bins=[0, 100, 200, 300, 700],
    labels=[0, 1, 2, 3]
).astype(float)


# ============================================================
# FILL MISSING VALUES
# ============================================================

    df = df.fillna(
    df.mean(numeric_only=True)
)


# ============================================================
# TARGET
# ============================================================

# Tomorrow's carbon intensity
    df["next_day_carbon_intensity"] = (
    df["carbon_intensity"].shift(-1)
)


# ============================================================
# REMOVE LAST ROW
# ============================================================

# Last row has no tomorrow's value
    df = df.dropna(
    subset=["next_day_carbon_intensity"]
)


    print(f"Final dataset rows after engineering: "f"{len(df)}")

    print(f"Columns: {list(df.columns)}")
    return df

# ── TRAIN MODEL ──────────────────────────────────────────────────

def train_model(df, tune=True):

    # -----------------------------
    # Feature Selection
    # -----------------------------
    feature_cols = [
    'temperature_mean',
    'precipitation',
    'wind_speed',
    'solar_radiation',
    'pm2_5',
    'nitrogen_dioxide',
    'carbon_monoxide',
    'carbon_intensity',
    'fossil_fuel_percentage',
    'day_of_week',
    'month',
    'is_weekend',
    'day_of_year',
    'quarter',
    'temp_7day_avg',
    "prev_3_day_co2",
    'pm25_7day_avg',
    "prev_2_day_co2",
    'prev_day_temp',
    'prev_day_pm25',
    'temp_category',
    'high_pollution',
    'intensity_category',
    "temp_14day_avg",
    "carbon_14day_avg",
    "prev_day_co2"
]

    feature_cols = [c for c in feature_cols if c in df.columns]

    X = df[feature_cols]
    y = df['next_day_carbon_intensity']

    print(f"Features used: {feature_cols}")
    print(f"Training samples: {len(X)}")

    # -----------------------------
    # Train/Test Split
    # -----------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        shuffle=False
    )

    print(f"Train size: {len(X_train)}")
    print(f"Test size: {len(X_test)}")

    # -----------------------------
    # MLflow
    # -----------------------------
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment("carbon_footprint_prediction")

    with mlflow.start_run():

        # -----------------------------
        # Base Model
        # -----------------------------
        base_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1
        )

        # -----------------------------
        # Model Selection
        # tune=True  -> full GridSearchCV with time-series CV
        # tune=False -> fixed params (nightly retrain, fast)
        # -----------------------------
        if tune:
            param_grid = {
        "n_estimators": [100, 200],
        "max_depth": [3, 4, 5],
        "learning_rate": [0.03, 0.05, 0.1],
        "subsample": [0.8, 1.0],
        "colsample_bytree": [0.8, 1.0],
        "min_child_weight": [1, 3]
    }
            tscv = TimeSeriesSplit(n_splits=5)

            grid = GridSearchCV(
                estimator=base_model,
                param_grid=param_grid,
                cv=tscv,
                scoring="neg_mean_absolute_error",
                n_jobs=-1,
                verbose=2
            )

            grid.fit(X_train, y_train)

            model = grid.best_estimator_
            params = grid.best_params_

            print("\nBest Parameters Found")
            print(params)
        else:
            params = {
                "n_estimators": 200,
                "max_depth": 4,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "min_child_weight": 1,
            }
            model = xgb.XGBRegressor(
                objective="reg:squarederror",
                random_state=42,
                n_jobs=-1,
                **params
            )

            print("\nUsing fixed nightly parameters")
            print(params)

        model.fit(
                X_train,
                y_train,
                eval_set=[(X_test, y_test)],
                verbose=False
            )

        # -----------------------------
        # Prediction
        # -----------------------------
        y_pred = model.predict(X_test)

        # -----------------------------
        # Metrics
        # -----------------------------
        mae = mean_absolute_error(y_test, y_pred)

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        r2 = r2_score(y_test, y_pred)

        print("\nModel Performance")
        print(f"MAE  : {mae:.2f}")
        print(f"RMSE : {rmse:.2f}")
        print(f"R2   : {r2:.4f}")

        # -----------------------------
        # MLflow Logging
        # -----------------------------
        mlflow.log_params(params)

        mlflow.log_metric("MAE", mae)

        mlflow.log_metric("RMSE", rmse)

        mlflow.log_metric("R2", r2)

        mlflow.log_metric("Train_Size", len(X_train))

        mlflow.log_metric("Test_Size", len(X_test))

        # -----------------------------
        # Feature Importance
        # -----------------------------
        importance = dict(
            zip(feature_cols, model.feature_importances_)
        )

        for feat, imp in importance.items():
            mlflow.log_metric(f"importance_{feat}", float(imp))

        # -----------------------------
        # Save Model
        # -----------------------------
        mlflow.xgboost.log_model(
            model,
            artifact_path="carbon_model",
            registered_model_name="CarbonFootprintPredictor"
        )

        print("\nModel Saved to MLflow")

                # -----------------------------
        # SHAP Explainability
        # -----------------------------
        try:
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_test)

            shap_importance = pd.DataFrame({
                "feature": feature_cols,
                "shap_importance": np.abs(shap_values).mean(axis=0)
            }).sort_values(
                by="shap_importance",
                ascending=False
            )

            print("\nTop 5 SHAP Features")
            print(shap_importance.head())

            for _, row in shap_importance.iterrows():
                mlflow.log_metric(
                    f"shap_{row['feature']}",
                    float(row["shap_importance"])
                )

        except Exception as e:
            print(f"SHAP error: {e}")

            # Fallback if SHAP fails
            shap_importance = pd.DataFrame({
                "feature": feature_cols,
                "shap_importance": model.feature_importances_
            }).sort_values(
                by="shap_importance",
                ascending=False
            )

            print("\nTop 5 Features (XGBoost importance)")
            print(shap_importance.head())

        # IMPORTANT:
        # Return values regardless of whether SHAP succeeds or fails
        return model, feature_cols, shap_importance

# ── GENERATE PREDICTIONS ─────────────────────────────────────────

def generate_predictions(model, feature_cols, df):

    # Get the latest day's features
    latest = df[feature_cols].iloc[-1:]

    # Predict tomorrow's carbon intensity
    prediction = model.predict(latest)[0]

    # Determine risk level
    if prediction <= 100:
        risk = "VERY LOW"
    elif prediction <= 200:
        risk = "LOW"
    elif prediction <= 300:
        risk = "MODERATE"
    elif prediction <= 400:
        risk = "HIGH"
    else:
        risk = "VERY HIGH"

    print("\nTomorrow's Prediction:")
    print(f"Predicted Carbon Intensity: {prediction:.2f}")
    print(f"Risk Level: {risk}")

    # Return values to main()
    return prediction, risk

# ── SAVE EMISSIONS TO MYSQL ──────────────────────────────────────

def save_emissions(df):
    conn = get_mysql_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO emissions
            (date, temperature_mean, precipitation,
             wind_speed, solar_radiation,
             pm2_5, nitrogen_dioxide,
             carbon_intensity, day_of_week,
             is_weekend, month, total_co2_kg,
             created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['date'].date(),
            row['temperature_mean'],
            row['precipitation'],
            row['wind_speed'],
            row['solar_radiation'],
            row['pm2_5'],
            row['nitrogen_dioxide'],
            row['carbon_intensity'],
            int(row['day_of_week']),
            int(row['is_weekend']),
            int(row['month']),
            row['total_co2_kg'],
            datetime.now()
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Emissions table updated: {len(df)} rows")

# ── SAVE PREDICTION TO MYSQL ────────────────────────────────────

def save_prediction(prediction, risk, confidence=0.85):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    tomorrow = (datetime.now() + timedelta(days=1)).date()

    cursor.execute("""
        INSERT INTO predictions
        (date, predicted_co2_kg, confidence, risk_level, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        predicted_co2_kg=VALUES(predicted_co2_kg),
        confidence=VALUES(confidence),
        risk_level=VALUES(risk_level),
        created_at=VALUES(created_at)
    """, (tomorrow, round(float(prediction), 2),
          confidence, risk, datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Prediction saved: {prediction:.2f} Risk: {risk}")

# ── NIGHTLY RETRAIN (called by Airflow DAG) ─────────────────────

def nightly_retrain():
    print("\nNightly retrain started")

    weather_df, air_df, carbon_df = load_data()

    df = prepare_features(weather_df, air_df, carbon_df)

    if df is None:
        print("Not enough data to retrain")
        return

    save_emissions(df)

    model, feature_cols, shap_importance = train_model(df, tune=False)

    prediction, risk = generate_predictions(model, feature_cols, df)

    save_prediction(prediction, risk)

    print("\nNightly retrain complete")

# ── MAIN ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("Carbon Footprint ML Training Started")
    print("=" * 50)

    # Step 1 load data
    print("\nStep 1: Loading data from MySQL...")
    weather_df, air_df, carbon_df = load_data()

    # Step 2 prepare features
    print("\nStep 2: Engineering features...")
    df = prepare_features(weather_df, air_df, carbon_df)

    if df is None:
        print("Not enough data. Run api_fetcher.py more times.")
        exit()

    # Step 3 save emissions
    print("\nStep 3: Saving emissions to MySQL...")
    save_emissions(df)

    # Step 4 train model
    print("\nStep 4: Training XGBoost model...")
    model, feature_cols, shap_importance = train_model(df)

    # Step 5 generate predictions
    print("\nStep 5: Generating tomorrow's prediction...")
    prediction, risk = generate_predictions(
        model, feature_cols, df)

    print("\n" + "=" * 50)
    print("Phase 4 Complete!")
    print("=" * 50)