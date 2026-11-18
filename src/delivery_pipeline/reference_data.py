"""Master data for DSPs (Delivery Service Partners) and delivery stations.

All names are fictional. Behaviour parameters (first-attempt success, lateness) drive the
synthetic data generator so the KPIs show realistic differences between partners.
"""

from __future__ import annotations

import csv
from pathlib import Path

# station_code, city, state, region, pincode_prefix
STATIONS = [
    ("HYD1", "Hyderabad", "Telangana", "South", "500"),
    ("HYD3", "Hyderabad", "Telangana", "South", "500"),
    ("BLR2", "Bengaluru", "Karnataka", "South", "560"),
    ("MAA1", "Chennai", "Tamil Nadu", "South", "600"),
    ("BOM4", "Mumbai", "Maharashtra", "West", "400"),
    ("PNQ1", "Pune", "Maharashtra", "West", "411"),
    ("DEL3", "New Delhi", "Delhi", "North", "110"),
    ("CCU1", "Kolkata", "West Bengal", "East", "700"),
]

# dsp_code, dsp_name, onboarded_on, fleet_size, stations served,
# first-attempt success probability, mean minutes vs promise (negative = early)
DSPS = [
    ("DSP-ARROW", "Arrow Last Mile", "2021-03-15", 32, ("HYD1", "HYD3"), 0.95, -95),
    ("DSP-BLAZE", "Blaze Logistics", "2022-07-01", 24, ("HYD1",), 0.91, -60),
    ("DSP-COMET", "Comet Couriers", "2020-11-20", 28, ("BLR2",), 0.93, -80),
    ("DSP-DELTA", "Delta Swift", "2023-02-10", 26, ("BLR2", "MAA1"), 0.88, -40),
    ("DSP-ECHO", "Echo Express", "2021-09-05", 22, ("MAA1",), 0.90, -70),
    ("DSP-FALCON", "Falcon Fleet", "2020-05-18", 30, ("BOM4",), 0.94, -90),
    ("DSP-GARUDA", "Garuda Deliveries", "2023-08-22", 25, ("BOM4", "PNQ1"), 0.87, -30),
    ("DSP-HELIX", "Helix Mobility", "2022-01-12", 20, ("PNQ1",), 0.92, -75),
    ("DSP-INDUS", "Indus Riders", "2021-06-30", 27, ("DEL3",), 0.89, -50),
    ("DSP-JETSTREAM", "Jetstream Logistics", "2022-10-03", 29, ("DEL3", "CCU1"), 0.90, -65),
]

FAILURE_REASONS = (
    "CUSTOMER_UNAVAILABLE",
    "ADDRESS_NOT_FOUND",
    "ACCESS_RESTRICTED",
    "PAYMENT_ISSUE",
    "WEATHER_DELAY",
    "VEHICLE_BREAKDOWN",
)
FAILURE_REASON_WEIGHTS = (0.45, 0.18, 0.15, 0.10, 0.07, 0.05)
SERVICE_TYPES = ("STANDARD", "NEXT_DAY", "SAME_DAY")
SERVICE_TYPE_WEIGHTS = (0.60, 0.25, 0.15)


def write_reference_csvs(reference_root: str | Path) -> dict[str, Path]:
    """Write dsps/dsps.csv and stations/stations.csv (one folder per table, Spark-friendly)."""
    root = Path(reference_root)
    (root / "dsps").mkdir(parents=True, exist_ok=True)
    (root / "stations").mkdir(parents=True, exist_ok=True)
    dsp_path = root / "dsps" / "dsps.csv"
    station_path = root / "stations" / "stations.csv"
    with open(dsp_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["dsp_code", "dsp_name", "onboarded_on", "fleet_size"])
        for code, name, onboarded, fleet, *_ in DSPS:
            w.writerow([code, name, onboarded, fleet])
    with open(station_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["station_code", "city", "state", "region", "pincode_prefix"])
        w.writerows(STATIONS)
    return {"dsps": dsp_path, "stations": station_path}
