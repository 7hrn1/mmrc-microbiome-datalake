# Microbiome Data Uploader & Validator

This Streamlit web app allows users to upload five specific CSV files related to microbiome research, validate them for:
- Intra-file duplicate sample IDs
- Missing sample IDs across files
- Duplicate sample IDs in DynamoDB (only for participant metadata)
- Upload validated files to AWS S3 and update hashes to DynamoDB

---

## Features

- CSV validation (intra-file and cross-file)
- Sample ID normalization and hashing (SHA256)
- Cross-checking for duplicates in DynamoDB
- File upload to respective AWS S3 buckets
- Auto DynamoDB update for new sample IDs

---

## Required Uploads

Users must upload the following 5 CSV files:

1. `bacterial-abundance.csv`
2. `bacterial-pathway.csv`
3. `enzymes-abundance.csv`
4. `nmr-asics-abundance.csv`
5. `participant-metadata.csv`

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://git.infotech.monash.edu/fit3164-monashmalaysiadatalake/datauploadui.git
cd datauploadui
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Streamlit (Optional)
Edit .streamlit/config.toml to increase upload limits:
```bash
[server]
maxUploadSize = 1024  # Size in MB, e.g., 1024MB = 1GB
```

### 4. Set AWS Credentials

Set environment variables in a .env file

#### for mac user
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=your-region
```

#### for windows/linux user
```
$Env:AWS_ACCESS_KEY_ID     = "your‐access‐key"
$Env:AWS_SECRET_ACCESS_KEY = "your‐secret‐key"
$Env:AWS_DEFAULT_REGION    = "your‐region"
```

### 5. Run the Streamlit App
#### for mac user
```bash
streamlit run app.py
```

#### for windows/linux user
```
python -m streamlit run app.py
```



