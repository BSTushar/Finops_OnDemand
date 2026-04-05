# FinOps EC2 Optimizer · v1.4
**Production-grade AWS EC2 cost optimisation tool for enterprise datasets.**

Release v1.4: workspace aligned with [Finops_OnDemand](https://github.com/BSTushar/Finops_OnDemand) main (April 2026).

---

## Quick Start

### Local (Python)
```bash
# 1. Unzip and enter the project directory
cd finops_tool

# 2. Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
# → Opens at http://localhost:8501
```

### Docker
```bash
# Build
docker build -t finops-ec2-optimizer .

# Run
docker run -p 8501:8501 finops-ec2-optimizer

# Access
open http://localhost:8501
```

---

## Features

| Feature | Detail |
|---|---|
| File upload | CSV, XLSX, XLS |
| Auto column detection | 50+ column name aliases, case-insensitive |
| **Failsafe manual mapping** | Dropdown UI when auto-detection fails |
| Pricing | Verified AWS On-Demand prices, 4 regions |
| Region dropdown | EU Ireland (default), US Virginia, AP Mumbai, EU Frankfurt |
| Recommendations | Alt 1 (next-gen) + Alt 2 (latest/Graviton) |
| Generation flag | Old Gen 🔴 / Current / Latest 🟢 |
| Colour-coded table | Green ≥20%, Amber 5–20%, Red = Old Gen |
| Sticky table header | Fixed height with scroll |
| Search + 5 filters | Text search, region, family, savings tier, generation |
| KPI tiles | Total, Optimisable, Old Gen count, Avg/Max savings, Est. saving |
| Charts | Savings by family · Generation distribution |
| Export | Formatted Excel (.xlsx) + CSV |
| Scale | 10,000+ rows · ~24,000 rows/sec |
| Zero network calls | Pricing from verified local cache |
| Docker-ready | Single-container deployment |

---

## Output Column Order (guaranteed)

| # | Column | Description |
|---|---|---|
| 1 | Instance Type | Original value |
| 2 | OS | Original value |
| 3 | On-Demand Price ($) | Verified AWS price ($/hr) |
| 4 | Alt 1 Instance | Next-generation upgrade |
| 5 | Alt 1 Price ($) | Alt 1 price ($/hr) |
| 6 | Alt 2 Instance | Latest/Graviton upgrade |
| 7 | Alt 2 Price ($) | Alt 2 price ($/hr) |
| 8 | Size | Extracted size (large, xlarge…) |
| 9 | Savings Opportunity (%) | (Original − Alt1) / Original × 100 |
| 10 | Generation Flag | Old Gen / Current / Latest / N/A |
| 11+ | All original columns | Preserved unchanged |

---

## Pricing Regions

| Region ID | Label | Default |
|---|---|---|
| `eu-west-1` | EU (Ireland) | ✅ |
| `us-east-1` | US East (N. Virginia) | |
| `ap-south-1` | Asia Pacific (Mumbai) | |
| `eu-central-1` | EU (Frankfurt) | |

All prices verified from AWS On-Demand pricing page, March 2025.

---

## File Format

Minimum required columns: **Instance Type**, **OS**

Optional (auto-detected): Cost, Usage, Region, Account, Application

Accepts 50+ column name variants (case-insensitive):
- `instance type`, `instancetype`, `ec2 type`, `type` → Instance Type
- `os`, `platform`, `operating system` → OS
- `cost`, `monthly cost`, `spend`, `blended cost` → Cost
- `usage`, `hours`, `running hours` → Usage
- `region`, `location`, `aws region` → Region
- `account`, `account id`, `linked account` → Account
- `application`, `service`, `workload`, `project` → Application

If columns cannot be auto-detected → manual mapping UI appears.

---

## Project Structure

```
finops_tool/
├── app.py              # Streamlit UI (685 lines)
├── data_loader.py      # File ingestion + failsafe column mapping
├── processor.py        # Enrichment pipeline + generation flagging
├── recommender.py      # Instance upgrade path logic (40+ families)
├── pricing_engine.py   # Verified AWS prices for 4 regions (230+ types)
├── requirements.txt    # Python dependencies
├── Dockerfile          # Production Docker image
├── .streamlit/
│   └── config.toml     # Streamlit server configuration
└── README.md
```

---

## Notes

- Prices are verified AWS On-Demand Linux prices. Always validate against the
  current AWS Pricing API before making purchasing decisions.
- Graviton (ARM) recommendations assume workload compatibility. Validate
  OS and runtime support before migrating.
- The tool never guesses prices: unknown instance types return N/A.
