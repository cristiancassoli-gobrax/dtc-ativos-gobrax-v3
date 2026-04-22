from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DTC_CSV = ROOT / "Veiculos_v3 - base_dtc.csv"
VEHICLES_CSV = ROOT / "Veiculos_v3 - base_veiculos.csv"
OUTPUT_JSON = ROOT / "dashboard_data.json"
OUTPUT_JS = ROOT / "dashboard_data.js"
OUTPUT_CSV_TOP30 = ROOT / "top_30_potenciais_dtc.csv"
VALIDATION_REPORT = ROOT / "dashboard_data_validation.txt"
LAST_TRUSTED_MATCH_SNAPSHOT = {
    "matched_total": 4338,
    "fleet_total": 25000,
    "coverage_rate": 17.35,
    "label": "ultimo snapshot valido antes da exportacao atual",
}

REQUIRED_DTC_COLUMNS = {
    "imei",
    "clientName",
    "fwVersion",
    "lastTimeConnected",
    "created",
    "status",
}

REQUIRED_VEHICLE_COLUMNS = {
    "start_date",
    "vehicle_id",
    "brand",
    "truckmodel",
    "customer_name",
    "document",
    "device_identification",
    "brand_devices",
    "chassi_ou_plate",
}
EXCLUDED_CUSTOMER_PATTERNS = ("gobrax", "indimplentes")
EXCLUDED_DEVICE_BRANDS = {"opt", "queclink", "sat_lite2", "bce"}


def validate_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        preview = ", ".join(map(str, df.columns.tolist()[:10]))
        raise ValueError(
            f"{label} invalido. Colunas obrigatorias ausentes: {', '.join(missing)}. "
            f"Colunas encontradas: {preview or 'nenhuma'}."
        )


def write_validation_report(message: str) -> None:
    VALIDATION_REPORT.write_text(message + "\n", encoding="utf-8")


def exclude_customers(df: pd.DataFrame) -> pd.DataFrame:
    customer_names_raw = df["customer_name"].fillna("").astype(str).str.strip()
    customer_names = customer_names_raw.str.lower()
    excluded_by_pattern = customer_names.str.contains("|".join(EXCLUDED_CUSTOMER_PATTERNS), regex=True)
    excluded_by_prefix = customer_names.str.startswith("ch -")
    mask = (customer_names_raw != "") & ~excluded_by_pattern & ~excluded_by_prefix
    return df.loc[mask].copy()


def exclude_device_brands(df: pd.DataFrame) -> pd.DataFrame:
    device_brand = df["brand_devices"].fillna("").astype(str).str.strip().str.lower()
    mask = ~device_brand.isin(EXCLUDED_DEVICE_BRANDS)
    return df.loc[mask].copy()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_device_identifier(value: object) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    text = text.replace(" ", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")

    try:
        number = float(text)
        if math.isfinite(number):
            return str(int(round(number)))
    except ValueError:
        pass

    digits_only = "".join(ch for ch in text if ch.isdigit())
    return digits_only or text


def read_csv_flexible(path: Path) -> pd.DataFrame:
    path = Path(path)
    lower_name = path.name.lower()
    if "base_dtc" in lower_name:
        attempts = [
            {"sep": ";", "encoding": "utf-8", "dtype": str},
            {"sep": ";", "encoding": "latin1", "dtype": str},
            {"sep": ",", "encoding": "utf-8", "dtype": str},
        ]
    elif "base_veiculos" in lower_name:
        attempts = [
            {"sep": ";", "encoding": "utf-8", "dtype": str},
            {"sep": ";", "encoding": "latin1", "dtype": str},
            {"sep": ",", "encoding": "utf-8", "dtype": str},
        ]
    else:
        attempts = [
            {"sep": ",", "encoding": "utf-8"},
            {"sep": ";", "encoding": "utf-8"},
            {"sep": ";", "encoding": "latin1"},
            {"sep": ";", "encoding": "cp1252"},
            {"sep": ",", "encoding": "latin1"},
        ]
    last_error: Exception | None = None

    for options in attempts:
        try:
            df = pd.read_csv(path, **options)
            if len(df.columns) > 1:
                return df
        except Exception as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise ValueError(f"Nao foi possivel ler o arquivo {path.name}")


def top_records(series: pd.Series, limit: int = 8) -> list[dict[str, object]]:
    counts = series.fillna("Nao informado").value_counts().head(limit)
    return [{"label": str(label), "value": int(value)} for label, value in counts.items()]


def monthly_records(series: pd.Series, limit: int = 8) -> list[dict[str, object]]:
    periods = series.dropna().dt.to_period("M").value_counts().sort_index().tail(limit)
    return [{"label": str(label), "value": int(value)} for label, value in periods.items()]


def rounded_rate(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def summarize_customer_view(df: pd.DataFrame) -> dict[str, object]:
    fleet_total = int(len(df))
    matched_total = int(df["matched"].sum())

    brand_summary = (
        df.groupby("brand")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values(["total", "matched"], ascending=[False, False])
    )
    brand_summary["gap"] = brand_summary["total"] - brand_summary["matched"]
    brand_summary["rate"] = (brand_summary["matched"] / brand_summary["total"] * 100).round(2)

    model_summary = (
        df.groupby("truckmodel")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values(["total", "matched"], ascending=[False, False])
    )
    model_summary["gap"] = model_summary["total"] - model_summary["matched"]
    model_summary["rate"] = (model_summary["matched"] / model_summary["total"] * 100).round(2)

    device_summary = (
        df.groupby("brand_devices")
        .agg(
            total=("vehicle_id", "count"),
            matched=("matched", "sum"),
            eligible=("eligible_dtc_activation", "sum"),
        )
        .sort_values(["total", "matched"], ascending=[False, False])
    )
    device_summary["rate"] = (device_summary["matched"] / device_summary["total"] * 100).round(2)
    device_summary["activatable_gap"] = (
        ((~df["matched"]) & (df["eligible_dtc_activation"])).groupby(df["brand_devices"]).sum()
    )
    device_summary["non_activatable_gap"] = (
        ((~df["matched"]) & (~df["eligible_dtc_activation"])).groupby(df["brand_devices"]).sum()
    )

    brand_hotspots = (
        df.groupby("brand")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values(["matched", "total"], ascending=[False, False])
    )
    brand_hotspots["rate"] = (brand_hotspots["matched"] / brand_hotspots["total"] * 100).round(2)

    highlights = [
        f"Cliente filtrado com {fleet_total} veiculos e cobertura real de {rounded_rate(matched_total, fleet_total)}%.",
        f"O principal gap do cliente esta em {brand_summary['gap'].idxmax() if len(brand_summary) else 'N/A'}, com {int(brand_summary['gap'].max()) if len(brand_summary) else 0} veiculos sem match.",
        f"O device com maior presenca no cliente e {device_summary.index[0] if len(device_summary) else 'N/A'}.",
    ]

    return {
        "kpis": {
            "fleet_total": fleet_total,
            "matched_total": matched_total,
            "coverage_rate": rounded_rate(matched_total, fleet_total),
            "brand_total": int(df["brand"].nunique(dropna=True)),
        },
        "charts": {
            "top_brands": [
                {"label": str(index), "value": int(row["total"])}
                for index, row in brand_summary.head(8).iterrows()
            ],
            "top_models": [
                {"label": str(index), "value": int(row["total"])}
                for index, row in model_summary.head(8).iterrows()
            ],
            "device_brands": [
                {"label": str(index), "value": int(row["total"])}
                for index, row in device_summary.head(8).iterrows()
            ],
        },
        "tables": {
            "brand_coverage": [
                {
                    "brand": str(index),
                    "total": int(row["total"]),
                    "matched": int(row["matched"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in brand_summary.head(10).iterrows()
            ],
            "models_by_gap": [
                {
                    "truckmodel": str(index),
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in model_summary.sort_values(["gap", "total"], ascending=[False, False])
                .head(10)
                .iterrows()
            ],
            "device_brand_coverage": [
                {
                    "brand_devices": str(index),
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "eligible": int(row["eligible"]),
                    "activatable_gap": int(row["activatable_gap"]),
                    "non_activatable_gap": int(row["non_activatable_gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in device_summary.head(10).iterrows()
            ],
            "customer_brand_hotspots": [
                {
                    "customer_name": str(df["customer_name"].iloc[0]) if fleet_total else "",
                    "brand": str(index),
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "rate": float(row["rate"]),
                }
                for index, row in brand_hotspots.head(8).iterrows()
            ],
        },
        "highlights": highlights,
    }


def scientific_notation_ratio(series: pd.Series) -> float:
    values = series.dropna().astype(str).str.strip()
    if len(values) == 0:
        return 0.0
    ratio = values.str.contains(r"[Ee]\+", regex=True).mean()
    return float(ratio)


def scientific_signature(value: object) -> str:
    if pd.isna(value):
        return ""
    try:
        number = float(str(value).strip().replace(",", "."))
        if math.isfinite(number):
            return f"{number:.5E}".replace(".", ",")
    except ValueError:
        return ""
    return ""


def main() -> None:
    try:
        dtc = read_csv_flexible(DTC_CSV)
        vehicles = read_csv_flexible(VEHICLES_CSV)
        validate_columns(dtc, REQUIRED_DTC_COLUMNS, "CSV DTC")
        validate_columns(vehicles, REQUIRED_VEHICLE_COLUMNS, "CSV de veiculos")
        vehicles = exclude_customers(vehicles)
        vehicles = exclude_device_brands(vehicles)
    except Exception as exc:
        message = (
            "Falha ao gerar dashboard_data.\n"
            f"Motivo: {exc}\n"
            f"Arquivo DTC: {DTC_CSV.name}\n"
            f"Arquivo veiculos: {VEHICLES_CSV.name}\n"
            "Verifique se a exportacao do CSV de veiculos contem o cabecalho esperado.\n"
        )
        write_validation_report(message)
        raise SystemExit(message)

    dtc["imei_key"] = dtc["imei"].map(normalize_device_identifier)
    dtc["imei_raw"] = dtc["imei"].astype(str).str.strip()
    dtc["imei_signature"] = dtc["imei"].map(scientific_signature)
    vehicles["device_key"] = vehicles["device_identification"].map(normalize_device_identifier)
    vehicles["device_raw"] = vehicles["device_identification"].astype(str).str.strip()
    vehicles["device_signature"] = vehicles["device_identification"].map(scientific_signature)
    vehicles["eligible_dtc_activation"] = (
        vehicles["brand_devices"].fillna("").astype(str).str.strip().str.lower() != "rp4"
    )
    dtc["lastTimeConnected"] = pd.to_datetime(dtc["lastTimeConnected"], errors="coerce")
    dtc["created"] = pd.to_datetime(dtc["created"], errors="coerce")
    vehicles["start_date"] = pd.to_datetime(
        vehicles["start_date"], format="%d/%m/%Y %H:%M", errors="coerce"
    )

    dtc_matchable = dtc[
        [
            "imei_key",
            "status",
            "lastTimeConnected",
            "fwVersion",
            "clientName",
            "supportSmsSending",
            "dmpEnabled",
        ]
    ].copy()
    dtc_matchable = dtc_matchable[dtc_matchable["imei_key"].astype(str).str.len() > 0]
    dtc_matchable = dtc_matchable.drop_duplicates(subset=["imei_key"])

    merged = vehicles.merge(
        dtc_matchable,
        left_on="device_key",
        right_on="imei_key",
        how="left",
    )
    merged["matched"] = merged["imei_key"].notna()
    merged["eligible_dtc_activation"] = vehicles["eligible_dtc_activation"].values
    sci_ratio = scientific_notation_ratio(vehicles["device_identification"])
    id_match_reliable = not (sci_ratio > 0.5 and int(merged["matched"].sum()) == 0)
    total_fleet = int(len(vehicles))
    valid_vehicle_raw = vehicles["device_raw"].astype(str).str.len() > 0
    valid_vehicle_key = vehicles["device_key"].astype(str).str.len() > 0
    valid_dtc_raw = dtc["imei_raw"].astype(str).str.len() > 0
    valid_dtc_key = dtc["imei_key"].astype(str).str.len() > 0
    direct_match_total = int(
        vehicles.loc[valid_vehicle_raw, "device_raw"].isin(set(dtc.loc[valid_dtc_raw, "imei_raw"])).sum()
    )
    normalized_match_total = int(
        vehicles.loc[valid_vehicle_key, "device_key"].isin(set(dtc.loc[valid_dtc_key, "imei_key"])).sum()
    )
    signature_overlap_total = int(vehicles["device_raw"].isin(set(dtc["imei_signature"])).sum())
    signature_overlap_distinct = int(
        pd.Series(list(set(vehicles["device_raw"]) & set(dtc["imei_signature"]))).nunique()
    )
    vehicle_ids_raw = vehicles["device_identification"].astype(str)
    validated_scope_mask = (vehicles["device_key"].astype(str).str.len() > 0) & (
        ~vehicle_ids_raw.str.contains(r"[Ee]\+", regex=True, na=False)
    )
    validated_scope_total = int(validated_scope_mask.sum())
    validated_match_total = int((merged["matched"] & validated_scope_mask).sum())
    validated_coverage_rate = rounded_rate(validated_match_total, total_fleet) if validated_match_total else 0.0
    validated_scope_rate = rounded_rate(validated_scope_total, total_fleet)

    customer_coverage = (
        merged.groupby("customer_name")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values(["matched", "total"], ascending=[False, False])
    )
    customer_coverage["rate"] = (
        customer_coverage["matched"] / customer_coverage["total"] * 100
    ).round(2)

    customer_coverage_high = customer_coverage[customer_coverage["total"] > 50].sort_values(
        ["rate", "matched"], ascending=[False, False]
    )
    customer_coverage_low = customer_coverage[customer_coverage["total"] > 50].sort_values(
        ["rate", "total"], ascending=[True, False]
    )

    brand_coverage = (
        merged.groupby("brand")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values("matched", ascending=False)
    )
    brand_coverage["rate"] = (brand_coverage["matched"] / brand_coverage["total"] * 100).round(2)
    customer_brand_coverage = (
        merged.groupby(["customer_name", "brand"])
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .reset_index()
    )
    customer_brand_coverage["rate"] = (
        customer_brand_coverage["matched"] / customer_brand_coverage["total"] * 100
    ).round(2)
    customer_coverage["gap"] = customer_coverage["total"] - customer_coverage["matched"]
    customer_coverage["opportunity_score"] = (
        customer_coverage["gap"] * (100 - customer_coverage["rate"]) / 100
    ).round(2)
    brand_coverage["gap"] = brand_coverage["total"] - brand_coverage["matched"]
    device_brand_coverage = (
        merged.groupby("brand_devices")
        .agg(
            total=("vehicle_id", "count"),
            matched=("matched", "sum"),
            eligible=("eligible_dtc_activation", "sum"),
        )
        .sort_values("total", ascending=False)
    )
    device_brand_coverage["rate"] = (
        device_brand_coverage["matched"] / device_brand_coverage["total"] * 100
    ).round(2)
    device_brand_coverage["gap"] = device_brand_coverage["total"] - device_brand_coverage["matched"]
    device_brand_coverage["activatable_gap"] = (
        ((~merged["matched"]) & (merged["eligible_dtc_activation"]))
        .groupby(merged["brand_devices"])
        .sum()
    )
    device_brand_coverage["non_activatable_gap"] = (
        ((~merged["matched"]) & (~merged["eligible_dtc_activation"]))
        .groupby(merged["brand_devices"])
        .sum()
    )
    model_coverage = (
        merged.groupby("truckmodel")
        .agg(total=("vehicle_id", "count"), matched=("matched", "sum"))
        .sort_values("total", ascending=False)
    )
    model_coverage["gap"] = model_coverage["total"] - model_coverage["matched"]
    model_coverage["rate"] = (model_coverage["matched"] / model_coverage["total"] * 100).round(2)
    pareto_gap = customer_coverage.sort_values(["gap", "total"], ascending=[False, False])[["gap"]].copy()
    pareto_gap["cum_gap"] = pareto_gap["gap"].cumsum()
    pareto_gap["cum_pct"] = (pareto_gap["cum_gap"] / pareto_gap["gap"].sum() * 100).round(2)
    clients_to_50 = int((pareto_gap["cum_pct"] < 50).sum() + 1) if len(pareto_gap) else 0
    clients_to_80 = int((pareto_gap["cum_pct"] < 80).sum() + 1) if len(pareto_gap) else 0
    size_buckets = [
        ("1-20", 1, 20),
        ("21-50", 21, 50),
        ("51-100", 51, 100),
        ("101+", 101, 999999),
    ]
    bucket_summary = []
    for label, min_fleet, max_fleet in size_buckets:
        bucket = customer_coverage[
            (customer_coverage["total"] >= min_fleet) & (customer_coverage["total"] <= max_fleet)
        ]
        if len(bucket) == 0:
            continue
        bucket_summary.append(
            {
                "label": label,
                "clients": int(len(bucket)),
                "fleet": int(bucket["total"].sum()),
                "matched": int(bucket["matched"].sum()),
                "rate": rounded_rate(int(bucket["matched"].sum()), int(bucket["total"].sum())),
            }
        )
    top_customers = vehicles["customer_name"].fillna("Nao informado").value_counts()
    merged["ideal_dtc_eligibility"] = (
        (~merged["brand"].fillna("").astype(str).str.strip().str.lower().str.contains("mercedes", na=False))
        & (merged["brand_devices"].fillna("").astype(str).str.strip().str.lower() != "rp4")
    )
    merged["is_rp4"] = (merged["brand_devices"].fillna("").astype(str).str.strip().str.lower() == "rp4")
    merged["is_mercedes"] = merged["brand"].fillna("").astype(str).str.strip().str.lower().str.contains("mercedes", na=False)
    customer_activation = (
        merged.groupby("customer_name")
        .agg(
            total=("vehicle_id", "count"),
            matched=("matched", "sum"),
            eligible=("eligible_dtc_activation", "sum"),
            rp4_total=("is_rp4", "sum"),
            mercedes_total=("is_mercedes", "sum"),
        )
        .sort_values("total", ascending=False)
    )
    customer_activation["gap"] = customer_activation["total"] - customer_activation["matched"]
    customer_activation["activatable_gap"] = (
        ((~merged["matched"]) & (merged["eligible_dtc_activation"]))
        .groupby(merged["customer_name"])
        .sum()
    )
    customer_activation["non_activatable_gap"] = (
        ((~merged["matched"]) & (~merged["eligible_dtc_activation"]))
        .groupby(merged["customer_name"])
        .sum()
    )
    customer_activation["activatable_ideal_gap"] = (
        ((~merged["matched"]) & (merged["ideal_dtc_eligibility"]))
        .groupby(merged["customer_name"])
        .sum()
    )
    customer_activation["non_ideal_gap"] = (
        ((~merged["matched"]) & (~merged["ideal_dtc_eligibility"]))
        .groupby(merged["customer_name"])
        .sum()
    )
    customer_activation["coverage"] = (
        customer_activation["matched"] / customer_activation["total"] * 100
    ).round(2)
    eligible_fleet_total = int(merged["eligible_dtc_activation"].sum())
    eligible_matched_total = int(merged.loc[merged["eligible_dtc_activation"], "matched"].sum())
    activatable_gap_total = int(((~merged["matched"]) & (merged["eligible_dtc_activation"])).sum())
    non_activatable_gap_total = int(((~merged["matched"]) & (~merged["eligible_dtc_activation"])).sum())
    customer_views = {}
    for customer_name, group in merged.groupby("customer_name", sort=True):
        if pd.isna(customer_name):
            continue
        customer_views[str(customer_name)] = summarize_customer_view(group.copy())

    payload = {
        "generated_at": pd.Timestamp.now(tz="America/Sao_Paulo").isoformat(),
        "source_files": [DTC_CSV.name, VEHICLES_CSV.name],
        "kpis": {
            "fleet_total": int(len(vehicles)),
            "dtc_total": int(len(dtc)),
            "matched_total": int(merged["matched"].sum()) if id_match_reliable else None,
            "coverage_rate": (
                rounded_rate(int(merged["matched"].sum()), int(len(vehicles)))
                if id_match_reliable
                else None
            ),
            "customer_total": int(vehicles["customer_name"].nunique(dropna=True)),
            "brand_total": int(vehicles["brand"].nunique(dropna=True)),
            "top_10_customer_concentration": rounded_rate(int(top_customers.head(10).sum()), total_fleet),
        },
        "matching": {
            "reliable": id_match_reliable,
            "scientific_notation_ratio": round(sci_ratio * 100, 2),
            "direct_match_total": direct_match_total,
            "normalized_match_total": normalized_match_total,
            "signature_overlap_total": signature_overlap_total,
            "signature_overlap_distinct": signature_overlap_distinct,
            "message": (
                "O campo device_identification veio majoritariamente em notacao cientifica e perdeu precisao, por isso a cobertura DTC nao e confiavel nesta importacao."
                if not id_match_reliable
                else "Chave device_identification valida para cruzamento com IMEI."
            ),
        },
        "fallback_match": {
            "last_trusted_matched_total": LAST_TRUSTED_MATCH_SNAPSHOT["matched_total"],
            "last_trusted_coverage_rate": LAST_TRUSTED_MATCH_SNAPSHOT["coverage_rate"],
            "last_trusted_fleet_total": LAST_TRUSTED_MATCH_SNAPSHOT["fleet_total"],
            "label": LAST_TRUSTED_MATCH_SNAPSHOT["label"],
            "estimated_current_matched_total": (
                LAST_TRUSTED_MATCH_SNAPSHOT["matched_total"] if not id_match_reliable else None
            ),
            "estimated_current_coverage_rate": (
                rounded_rate(LAST_TRUSTED_MATCH_SNAPSHOT["matched_total"], total_fleet)
                if not id_match_reliable
                else None
            ),
        },
        "validation": {
            "validated_scope_total": validated_scope_total,
            "validated_scope_rate": validated_scope_rate,
            "validated_match_total": validated_match_total,
            "validated_coverage_rate": validated_coverage_rate,
            "estimated_match_total": (
                LAST_TRUSTED_MATCH_SNAPSHOT["matched_total"] if not id_match_reliable else None
            ),
            "estimated_coverage_rate": (
                rounded_rate(LAST_TRUSTED_MATCH_SNAPSHOT["matched_total"], total_fleet)
                if not id_match_reliable
                else None
            ),
        },
        "activation": {
            "eligible_fleet_total": eligible_fleet_total,
            "eligible_coverage_rate": rounded_rate(eligible_matched_total, eligible_fleet_total),
            "activatable_gap_total": activatable_gap_total,
            "non_activatable_gap_total": non_activatable_gap_total,
            "rp4_gap_total": int(
                ((merged["brand_devices"].fillna("").astype(str).str.strip().str.lower() == "rp4") & (~merged["matched"])).sum()
            ),
        },
        "dates": {
            "vehicles_start_min": vehicles["start_date"].min().isoformat() if vehicles["start_date"].notna().any() else None,
            "vehicles_start_max": vehicles["start_date"].max().isoformat() if vehicles["start_date"].notna().any() else None,
            "dtc_created_min": dtc["created"].min().isoformat() if dtc["created"].notna().any() else None,
            "dtc_created_max": dtc["created"].max().isoformat() if dtc["created"].notna().any() else None,
            "dtc_connected_min": dtc["lastTimeConnected"].min().isoformat() if dtc["lastTimeConnected"].notna().any() else None,
            "dtc_connected_max": dtc["lastTimeConnected"].max().isoformat() if dtc["lastTimeConnected"].notna().any() else None,
        },
        "filters": {
            "customers": sorted(customer_views.keys()),
        },
        "charts": {
            "fleet_growth": monthly_records(vehicles["start_date"], limit=10),
            "dtc_growth": monthly_records(dtc["created"], limit=10),
            "top_brands": top_records(vehicles["brand"], limit=8),
            "top_models": top_records(vehicles["truckmodel"], limit=8),
            "top_customers": top_records(vehicles["customer_name"], limit=10),
            "device_brands": top_records(vehicles["brand_devices"], limit=8),
            "fw_versions": top_records(dtc["fwVersion"], limit=8),
        },
        "tables": {
            "largest_customers": [
                {
                    "customer_name": str(label),
                    "fleet": int(value),
                    "share": rounded_rate(int(value), total_fleet),
                }
                for label, value in top_customers.head(12).items()
            ],
            "customers_by_coverage": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage[customer_coverage["total"] > 50]
                .sort_values(["rate", "matched"], ascending=[False, False])
                .head(12)
                .iterrows()
            ],
            "customers_by_low_coverage": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage[customer_coverage["total"] > 50]
                .sort_values(["rate", "total"], ascending=[True, False])
                .head(12)
                .iterrows()
            ],
            "customer_brand_hotspots": [
                {
                    "customer_name": str(row["customer_name"]),
                    "brand": str(row["brand"]),
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "rate": float(row["rate"]),
                }
                for _, row in customer_brand_coverage[customer_brand_coverage["total"] > 40]
                .sort_values(["matched", "rate"], ascending=[False, False])
                .head(12)
                .iterrows()
            ],
            "customers_by_matches": [
                {
                    "customer_name": index,
                    "matched": int(row["matched"]),
                    "total": int(row["total"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage.head(10).iterrows()
            ] if id_match_reliable else [],
            "customers_best_coverage": [
                {
                    "customer_name": index,
                    "matched": int(row["matched"]),
                    "total": int(row["total"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage_high.head(8).iterrows()
            ] if id_match_reliable else [],
            "customers_low_coverage": [
                {
                    "customer_name": index,
                    "matched": int(row["matched"]),
                    "total": int(row["total"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage_low.head(8).iterrows()
            ] if id_match_reliable else [],
            "brand_coverage": [
                {
                    "brand": index,
                    "matched": int(row["matched"]),
                    "total": int(row["total"]),
                    "rate": float(row["rate"]),
                    "gap": int(row["gap"]),
                }
                for index, row in brand_coverage.head(8).iterrows()
            ],
            "customers_by_gap": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage.sort_values(["gap", "total"], ascending=[False, False])
                .head(12)
                .iterrows()
            ],
            "device_brand_coverage": [
                {
                    "brand_devices": index,
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "eligible": int(row["eligible"]),
                    "activatable_gap": int(row["activatable_gap"]),
                    "non_activatable_gap": int(row["non_activatable_gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in device_brand_coverage.head(8).iterrows()
            ],
            "models_by_gap": [
                {
                    "truckmodel": str(index),
                    "fleet": int(row["total"]),
                    "matched": int(row["matched"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in model_coverage.sort_values(["gap", "total"], ascending=[False, False])
                .head(12)
                .iterrows()
            ],
            "opportunity_ranking": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                    "score": float(row["opportunity_score"]),
                }
                for index, row in customer_coverage.sort_values(
                    ["opportunity_score", "gap"], ascending=[False, False]
                )
                .head(12)
                .iterrows()
            ],
            "quick_wins": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage[
                    (customer_coverage["total"] >= 80)
                    & (customer_coverage["rate"] >= 40)
                    & (customer_coverage["rate"] < 80)
                ]
                .sort_values(["gap", "rate"], ascending=[False, False])
                .head(10)
                .iterrows()
            ],
            "critical_cases": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "gap": int(row["gap"]),
                    "rate": float(row["rate"]),
                }
                for index, row in customer_coverage[
                    (customer_coverage["total"] >= 80) & (customer_coverage["rate"] < 10)
                ]
                .sort_values(["gap", "total"], ascending=[False, False])
                .head(10)
                .iterrows()
            ],
            "fleet_size_buckets": bucket_summary,
            "activation_opportunity": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "activatable_gap": int(row["activatable_gap"]),
                    "non_activatable_gap": int(row["non_activatable_gap"]),
                    "coverage": float(row["coverage"]),
                }
                for index, row in customer_activation.sort_values(
                    ["activatable_gap", "gap"], ascending=[False, False]
                )
                .head(12)
                .iterrows()
            ],
            "rp4_blocked_accounts": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "non_activatable_gap": int(row["non_activatable_gap"]),
                    "activatable_gap": int(row["activatable_gap"]),
                }
                for index, row in customer_activation[customer_activation["total"] >= 80]
                .sort_values(["non_activatable_gap", "activatable_gap"], ascending=[False, False])
                .head(12)
                .iterrows()
            ],
            "potential_30_clients_dtc": [
                {
                    "customer_name": index,
                    "fleet": int(row["total"]),
                    "activatable_ideal_gap": int(row["activatable_ideal_gap"]),
                    "rp4_total": int(row["rp4_total"]),
                    "mercedes_total": int(row["mercedes_total"]),
                    "non_ideal_gap": int(row["non_ideal_gap"]),
                    "coverage": float(row["coverage"]),
                }
                for index, row in customer_activation.sort_values(
                    ["activatable_ideal_gap", "gap"], ascending=[False, False]
                )
                .head(30)
                .iterrows()
            ],
        },
        "pareto": {
            "clients_to_50_gap_pct": clients_to_50,
            "clients_to_80_gap_pct": clients_to_80,
            "top_gap_rows": [
                {
                    "customer_name": index,
                    "gap": int(row["gap"]),
                    "cum_pct": float(row["cum_pct"]),
                }
                for index, row in pareto_gap.head(12).iterrows()
            ],
        },
        "quality": {
            "vehicles_missing_truckmodel": int(vehicles["truckmodel"].isna().sum()),
            "vehicles_missing_chassi_or_plate": int(vehicles["chassi_ou_plate"].isna().sum()),
            "vehicles_missing_document": int(vehicles["document"].isna().sum()),
            "dtc_missing_last_command_type": int(dtc["lastCommandStatus.type"].isna().sum()),
            "dtc_missing_cfg_version": int(dtc["cfgVersion"].isna().sum()),
            "vehicle_ids_in_scientific_notation_pct": round(sci_ratio * 100, 2),
        },
        "highlights": [
            (
                "A cobertura DTC nao pode ser calculada com confianca nesta importacao porque o identificador do device foi truncado pela exportacao."
                if not id_match_reliable
                else "A cobertura DTC atual pede uma leitura clara entre universo total e base conectada."
            ),
            "DAF, Scania e Volvo concentram a maior parte da operacao e devem liderar os comparativos visuais.",
            "Alguns clientes ja estao com cobertura muito alta, o que ajuda a destacar contas com maior potencial de expansao.",
            "Hoje devices rp4 nao possuem suporte a ativacao DTC, entao parte do gap atual nao e ativavel no cenario de hoje.",
        ],
        "customer_views": customer_views,
    }

    json_payload = json.dumps(payload, ensure_ascii=True, indent=2)
    OUTPUT_JSON.write_text(json_payload)
    OUTPUT_JS.write_text(f"window.DASHBOARD_DATA = {json_payload};\n")

    top_30_data = [
        {
            "Cliente": row["customer_name"],
            "Frota Total": int(row["fleet"]),
            "Gap Ideal (Alvo)": int(row["activatable_ideal_gap"]),
            "Gap Invalido (Geral)": int(row["non_ideal_gap"]),
            "Veiculos RP4": int(row["rp4_total"]),
            "Veiculos Mercedes": int(row["mercedes_total"]),
            "Cobertura Atual (%)": f"{float(row['coverage']):.2f}".replace('.', ',') + "%"
        }
        for row in payload["tables"]["potential_30_clients_dtc"]
    ]
    if top_30_data:
        pd.DataFrame(top_30_data).to_csv(OUTPUT_CSV_TOP30, index=False, sep=";", encoding="utf-8-sig")
        print(f"Top 30 CSV written to {OUTPUT_CSV_TOP30}")

    write_validation_report("OK: arquivos validados e dashboard_data gerado com sucesso.")
    print(f"Dashboard data written to {OUTPUT_JSON}")
    print(f"Dashboard script written to {OUTPUT_JS}")


if __name__ == "__main__":
    main()
