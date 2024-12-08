
FROM quay.io/astronomer/astro-runtime:12.5.0

# Install additional Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
