import pandas as pd
import numpy as np
import pymysql
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import shap
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

load_dotenv("myfile.env")

print("KAFKA:", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
print("MYSQL:", os.getenv("MYSQL_HOST"))
# ── DATABASE CONNECTION ──────────────────────────────────────────

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
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
        SELECT DATE(fetched_at) as date,
               AVG(carbon_intensity) as carbon_intensity,
               AVG(fossil_fuel_percentage) as fossil_fuel_percentage
        FROM carbon_intensity_data
        GROUP BY DATE(fetched_at)
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

    # date based features
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['is_weekend'] = df['day_of_week'].apply(
        lambda x: 1 if x >= 5 else 0)
    df['day_of_year'] = df['date'].dt.dayofyear
    df['quarter'] = df['date'].dt.quarter

    # calculate total CO2 emissions
    # using emission factors
    ELECTRICITY_FACTOR = 0.233  # kg CO2 per kWh
    TRANSPORT_FACTOR = 2.31     # kg CO2 per liter petrol
    INDUSTRIAL_FACTOR = 0.5     # kg CO2 per unit

    df['scope2_co2'] = (
        df['carbon_intensity'] * 1000 * ELECTRICITY_FACTOR
    )

    df['transport_co2'] = (
        df['nitrogen_dioxide'] * 10 * TRANSPORT_FACTOR
    )

    df['industrial_co2'] = (
        df['carbon_monoxide'] * INDUSTRIAL_FACTOR
    )

    df['total_co2_kg'] = (
        df['scope2_co2'] +
        df['transport_co2'] +
        df['industrial_co2']
    )

    # rolling averages
    df['temp_7day_avg'] = df['temperature_mean'].rolling(
        window=7, min_periods=1).mean()
    df['co2_7day_avg'] = df['total_co2_kg'].rolling(
        window=7, min_periods=1).mean()
    df['pm25_7day_avg'] = df['pm2_5'].rolling(
        window=7, min_periods=1).mean()

    # lag features
    df['prev_day_co2'] = df['total_co2_kg'].shift(1)
    df['prev_day_temp'] = df['temperature_mean'].shift(1)
    df['prev_day_pm25'] = df['pm2_5'].shift(1)

    # temperature categories
    df['temp_category'] = pd.cut(
        df['temperature_mean'],
        bins=[-50, 10, 20, 30, 60],
        labels=[0, 1, 2, 3]
    ).astype(float)

    # high pollution flag
    df['high_pollution'] = (
        (df['pm2_5'] > 50) | (df['nitrogen_dioxide'] > 40)
    ).astype(int)

    # carbon intensity category
    df['intensity_category'] = pd.cut(
        df['carbon_intensity'],
        bins=[0, 100, 200, 300, 700],
        labels=[0, 1, 2, 3]
    ).astype(float)

    # fill any remaining nulls
    df = df.fillna(df.mean(numeric_only=True))

    # target variable
    # predict next day total CO2
    df['next_day_co2'] = df['total_co2_kg'].shift(-1)

    # drop last row (no target)
    df = df.dropna(subset=['next_day_co2'])

    print(f"Final dataset rows after engineering: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    return df

# ── TRAIN MODEL ──────────────────────────────────────────────────

def train_model(df):

    # define features
    feature_cols = [
        'temperature_mean', 'precipitation',
        'wind_speed', 'solar_radiation',
        'pm2_5', 'nitrogen_dioxide', 'carbon_monoxide',
        'carbon_intensity', 'fossil_fuel_percentage',
        'day_of_week', 'month', 'is_weekend',
        'day_of_year', 'quarter',
        'temp_7day_avg', 'co2_7day_avg', 'pm25_7day_avg',
        'prev_day_co2', 'prev_day_temp', 'prev_day_pm25',
        'temp_category', 'high_pollution',
        'intensity_category', 'total_co2_kg'
    ]

    # keep only available columns
    feature_cols = [c for c in feature_cols if c in df.columns]

    X = df[feature_cols]
    y = df['next_day_co2']

    print(f"Features used: {feature_cols}")
    print(f"Training samples: {len(X)}")

    # train test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False)

    print(f"Train size: {len(X_train)}")
    print(f"Test size: {len(X_test)}")

    # set MLflow tracking
    mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI'))
    mlflow.set_experiment("carbon_footprint_prediction")

    with mlflow.start_run():

        # XGBoost parameters
        params = {
            'n_estimators': 100,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 1,
            'gamma': 0,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'n_jobs': -1
        }

        # train XGBoost model
        model = xgb.XGBRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )

        # predictions
        y_pred = model.predict(X_test)

        # metrics
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)

        print(f"\nModel Performance:")
        print(f"MAE:  {mae:.2f} kg CO2")
        print(f"RMSE: {rmse:.2f} kg CO2")
        print(f"R2:   {r2:.4f} ({r2*100:.1f}%)")

        # log to MLflow
        mlflow.log_params(params)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("train_size", len(X_train))
        mlflow.log_metric("test_size", len(X_test))

        # log feature importance
        importance = dict(zip(
            feature_cols,
            model.feature_importances_))
        for feat, imp in importance.items():
            mlflow.log_metric(f"importance_{feat}", imp)

        # save model to MLflow
        mlflow.xgboost.log_model(
            model, "carbon_model",
            registered_model_name="CarbonFootprintPredictor")

        print(f"\nModel saved to MLflow")

        # SHAP explainability
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        # top 5 features by SHAP
        shap_importance = pd.DataFrame({
            'feature': feature_cols,
            'shap_importance': np.abs(shap_values).mean(axis=0)
        }).sort_values('shap_importance', ascending=False)

        print(f"\nTop 5 Features (SHAP):")
        print(shap_importance.head())

        # log SHAP values to MLflow
        for _, row in shap_importance.iterrows():
            mlflow.log_metric(
                f"shap_{row['feature']}",
                row['shap_importance'])

        return model, feature_cols, shap_importance

# ── GENERATE PREDICTIONS ─────────────────────────────────────────

def generate_predictions(model, feature_cols, df):

    # use latest row to predict tomorrow
    latest = df[feature_cols].iloc[-1:].copy()
    prediction = model.predict(latest)[0]

    # risk level
    if prediction < 500:
        risk = 'LOW'
    elif prediction < 1000:
        risk = 'MEDIUM'
    else:
        risk = 'HIGH'

    tomorrow = (datetime.now() + timedelta(days=1)).date()

    print(f"\nTomorrow's Prediction:")
    print(f"Date: {tomorrow}")
    print(f"Predicted CO2: {prediction:.2f} kg")
    print(f"Risk Level: {risk}")

    # save prediction to MySQL
    conn = get_mysql_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO predictions
        (date, predicted_co2_kg, confidence, risk_level, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        predicted_co2_kg = VALUES(predicted_co2_kg),
        risk_level = VALUES(risk_level),
        created_at = VALUES(created_at)
    """, (tomorrow, round(float(prediction), 2),
          0.85, risk, datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Prediction saved to MySQL")
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