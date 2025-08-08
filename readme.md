# YouTube Data Pipeline

## Overview

A modular ETL pipeline that:

1. **Extracts** video and channel data from the YouTube Data API.
2. **Transforms** and cleans the data for analysis.
3. **Validates** the transformed data.
4. **Loads** the data into a SQL Server database.
5. Includes sample SQL analysis scripts and a Power BI report for visualization.

This pipeline is designed to help content creators, data analysts, and researchers automate the collection and analysis of YouTube channel metrics.

---

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [SQL Analysis](#sql-analysis)
- [Power BI Report](#power-bi-report)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Features

- **Automated ETL**: Pulls data from the YouTube Data API and loads it into your database.
- **Modular Design**: Separate modules for extraction, transformation, validation, and loading.
- **Validation Checks**: Ensures data quality before insertion.
- **SQL & BI**: Sample SQL scripts and a Power BI (.pbix) file for further analysis and visualization.

---

## Prerequisites

- Python 3.10+
- Access to a SQL Server instance (e.g., SQL Express)
- YouTube Data API key
- [Power BI Desktop](https://powerbi.microsoft.com/) (optional, for the provided report)

---

## Installation

1. **Clone the repository**:

   ```bash
   git clone <your-repo-url>.git
   cd Youtube_Data_Pipeline
   ```

2. **Create and activate a virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate    # Linux/macOS
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**:

   ```bash
   pip install --upgrade pip
   pip install -r reqirements.txt
   ```

---

## Project Structure

```text
├── .env                  # Environment variables (API key)
├── etl_script.py         # Main ETL workflow
├── reqirements.txt       # Python dependencies
├── etl_utils/            # ETL modules
│   ├── extract.py        # Extraction logic
│   ├── transform.py      # Transformation logic
│   ├── validate.py       # Data validation
│   └── load.py           # Load into SQL Server
├── sql scripts/          # Sample SQL analysis scripts
│   └── video_analysis.sql
└── Video Analysis.pbix    # Power BI report template
```

---

## Configuration

1. **Populate **``:

   ```ini
   API_KEY=<YOUR_YOUTUBE_API_KEY>
   ```

2. **(Optional) Adjust database connection** in `etl_utils/load.py` if you are not using the default SQL Server Express settings:

   ```python
   params = urllib.parse.quote_plus(
       'DRIVER=ODBC Driver 17 for SQL Server;'
       'SERVER=<YOUR_SERVER_NAME>;<br>'
       'DATABASE=<YOUR_DATABASE_NAME>;<br>'
       'Trusted_Connection=yes'
   )
   ```

---

## Usage

Run the full ETL process:

```bash
python etl_script.py
```

The script will:

1. Load your API key from `.env`.
2. Extract channel and video data.
3. Transform and clean the datasets.
4. Validate data quality.
5. Load both tables (`videos`, `channels`) into your SQL Server database.

---

## SQL Analysis

Use the provided SQL script to analyze your loaded data:

```bash
sqlcmd -S <SERVER_NAME> -d <DATABASE_NAME> -i "sql scripts/video_analysis.sql"
```

This script performs sample queries such as:

- Top videos by view count
- Channel growth trends
- Duration vs. engagement analysis

---

## Power BI Report

Open `Video Analysis.pbix` in Power BI Desktop to explore pre-built dashboards:

- **Overview**: Summary metrics and trends.
- **Video Performance**: Visuals for views, likes, comments over time.
- **Channel Insights**: Subscriber and upload rate analyses.

---

## Contributing

Contributions are welcome! Please:

1. Fork the repo.
2. Create a new branch: `git checkout -b feature/my-feature`.
3. Commit your changes.
4. Create a pull request.

Please ensure code quality and add tests where applicable.

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.

---

## Contact

- **Author**: Jachimike Ozoh
- **Email**: [your.email@example.com](mailto\:your.email@example.com)
- **GitHub**: [github.com/yourusername](https://github.com/yourusername)

