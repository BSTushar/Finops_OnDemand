# ── FinOps EC2 Optimizer · Docker Image ──────────────────────────────────
FROM python:3.12-slim

# Metadata
LABEL maintainer="FinOps Team"
LABEL description="EC2 Cost Optimisation Tool"
LABEL version="1.3"

# System deps (slim image needs these for openpyxl/xlrd)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Install Python dependencies first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app.py                  .
COPY data_loader.py          .
COPY processor.py            .
COPY recommender.py          .
COPY rds_recommender.py      .
COPY rds_mysql_sa_prices.py  .
COPY pricing_engine.py       .

# Streamlit config — disable telemetry, set port
RUN mkdir -p /root/.streamlit
COPY .streamlit/config.toml /root/.streamlit/config.toml

# Expose Streamlit port
EXPOSE 8501

# Health check — localhost only (no external network)
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5)" || exit 1

# Entry point
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
