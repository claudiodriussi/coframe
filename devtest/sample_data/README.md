# Sample Data for Coframe Library Demo

This directory contains sample data to populate the Coframe library database with realistic books, authors, and publishers.

## 🚀 Usage

### Prerequisites

1. Update the model with new fields:
   ```bash
   cd /home/claudio/sviluppo/python/webapp/coframe/devtest
   python devtest.py  # Regenerates model.py with Publisher and new Book fields
   ```

2. Make sure the database is initialized (run flask-server.py or devtest.py once)

### Import Data

```bash
# From the devtest directory
python sample_data/import_sample_data.py
```

