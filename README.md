# FinOps EC2 Optimizer · v2.0
**Production-grade AWS EC2 cost optimisation tool for enterprise datasets.**

Release v2.0 — version 2 ready to play (April 2026).

---

## Disclaimer (mandatory for internal use)

**Costs are based on a static AWS list-price snapshot (eu-west-1). Values are indicative and must be validated against actual billing before decision-making.**

- **Pricing snapshot** (region id, source, as-of date) is shown in the app header and repeated on the **Recommendations** sheet (top rows) and **Metadata** sheet in Excel exports.
- This tool is **decision support only**; it is **not** a replacement for billing systems (CUR, Cost Explorer, invoices). **Recommendations must be validated** by engineering and finance before production changes.

---

## What it does / does not do

| Does | Does not |
|------|----------|
| Enrich uploads with indicative alt instance classes, costs, and savings % from a **local** price dataset | Call the AWS Pricing API or send your data externally |
| Preserve original columns and insert enrichment **after** the instance column | Apply enterprise discounts, RIs, or Savings Plans automatically |
| Show **N/A** when a SKU or OS is unknown | Guarantee performance or Graviton compatibility |

**How to use:** Upload CSV/Excel → choose pricing **region** and **Service** (EC2 / RDS / Both) → map columns if needed → **Run enrichment** → filter → download **Excel** (includes disclaimer + metadata) or **CSV** (data table only).

---

## Interface (guided experience)

The Streamlit UI is designed for a **calm, product-style flow** (clarity-first, similar in spirit to Apple’s marketing sites—generous whitespace, system typography, soft cards, no external font CDNs):

- **Centered layout** (~1080px) with **numbered steps**: load file → optional merge → map columns → run enrichment → results.
- **SF / system font stack** (`-apple-system`, `BlinkMacSystemFont`, `Segoe UI`, …), **antialiased** type, **pill** primary actions, **rounded** inputs and file dropzones.
- **Light** theme by default in `config.toml` with **blue** primary accent; **dark mode** follows the OS (`prefers-color-scheme`) for backgrounds and cards.
- **Trust card** surfaces pricing snapshot, disclaimer, and expectation-setting in one readable block.

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
| **Guided UI** | Numbered steps, hero headline, trust card, pill buttons |
| File upload | CSV, XLSX, XLS |
| **Fix Your Sheet** | Optional merge of two files on a common ID (`sheet_merger.py`) |
| Auto column detection | Broad header hints; manual mapping when ambiguous |
| Pricing | Local static lists, 4 regions (no live Pricing API) |
| Service modes | EC2-only, RDS-only, or both |
| CPU modes | Default, Intel, Graviton, or both |
| Recommendations | Alt1 / Alt2 instance API names + projected costs & savings % |
| Table | Colour hints on savings columns; scrollable frame |
| Filters | View EC2/RDS subset, OS text filter, column search |
| KPI tiles | Row count, avg / max Alt1 savings, actual-cost flag |
| Export | Excel (disclaimer + metadata rows) + CSV (table only) |
| Scale | Tested 10k+ rows |
| Security posture | App pricing logic uses **local** datasets only (no `requests`/`urllib` in tool `.py`) |

---

## Output columns (after enrichment)

New columns are inserted **immediately after** your mapped **instance** column (original columns otherwise **unchanged**):

| Column | Meaning |
|---|---|
| `Actual Cost ($)` | From your file (optional column) |
| `Alt1 Instance` / `Alt2 Instance` | Suggested API names |
| `Alt1 Cost ($)` / `Alt2 Cost ($)` | Indicative, from list-price ratio × actual |
| `Alt1 Savings %` / `Alt2 Savings %` | vs actual, or “No Savings” / `N/A` |
| `Alt2 Instance` (edge case) | If only one distinct upgrade exists, shows **`N/A (No distinct alternative)`** instead of an empty cell |

All **original** columns remain, in order, before/after that block.

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
├── app.py              # Streamlit UI
├── excel_export.py     # Excel download (disclaimer + metadata rows)
├── sheet_merger.py     # Fix Your Sheet: merge two uploads on a common key
├── data_loader.py      # File ingestion + column mapping
├── processor.py        # Enrichment pipeline
├── recommender.py      # Instance upgrade path logic
├── rds_recommender.py  # RDS API Name recommendations
├── pricing_engine.py   # Local price datasets + disclaimer constants
├── instance_api.py     # Strict API Name parsing
├── requirements.txt
├── Dockerfile
├── tests/
├── .streamlit/config.toml
└── README.md
```

---

## Notes

- Prices are verified AWS On-Demand Linux prices. Always validate against the
  current AWS Pricing API before making purchasing decisions.
- Graviton (ARM) recommendations assume workload compatibility. Validate
  OS and runtime support before migrating.
- The tool never guesses prices: unknown instance types return N/A.
