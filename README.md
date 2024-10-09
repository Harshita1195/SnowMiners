
# Snow Miners: Data Extraction and Analysis Solution

**Snow Miners** is an advanced data extraction and analysis solution designed to handle unstructured, structured, and semi-structured data. It leverages cutting-edge technologies such as Large Language Models (LLMs), Generative AI, and Snowflake's AI products like Doc AI, Cortex Search, and Cortex Analyst. The application is built using a Streamlit frontend, enabling users to interact with their data seamlessly.

## Features

- **Data Extraction**: Pull data from diverse sources with high precision.
- **Data Review**: Clean and review your data with ease using real-time error-spotting and correction.
- **Advanced Analysis**: Generate insights and reports with built-in tools and AI models.
- **Metadata Sharing**: Collaborate effortlessly by sharing metadata and reports across platforms.

## Directory Structure

```
Snowminer/
│
├── data/                     # Contains sample data files
├── native-app/                # Snowflake Native App components
├── output/                    # Output files and logs
├── src/                       # Application source code
│   ├── admin_apps/            # Admin-specific features
│   ├── analyse_data.py        # Data analytics functions
│   ├── assets/                # Static assets (images, stylesheets, etc.)
│   ├── dashboard.py           # Streamlit app dashboard
│   ├── data_catalog_metadata.py # Metadata handling for data catalog
│   ├── Database/              # Database-specific scripts and setup
│   ├── doc_chat.py            # Chat feature for document interaction
│   ├── embeddings/            # Embedding generation utilities
│   ├── main.py                # **Main entry point** for the application
│   ├── Makefile               # Automation for build and deployment
│   ├── manifest.yml           # Snowflake Native App manifest
│   ├── requirements.txt       # Python dependencies
│   ├── scripts/               # Helper scripts
│   ├── semantic_model_generator/  # For generating semantic models
│   ├── setup.sql              # SQL setup script for Snowflake components
│   ├── style.css              # Custom styles for Streamlit app
│   └── utility.py             # Utility functions
```

## Prerequisites

Before running the project, ensure you have the following:

- **Python 3.8+**
- **Snowflake Account** with access to Snowflake AI features (e.g., Doc AI, Cortex Search, Cortex Analyst)
- Installed Python dependencies:
  ```bash
  pip install -r src/requirements.txt
  ```

## Setup Instructions

### Clone the repository:

```bash
git clone <repository-url>
cd Snowminer
```

### Configure environment variables:

Inside the `src/` directory, create a `.env` file with the following details:

```bash
SNOWFLAKE_ACCOUNT=<your_snowflake_account>
SNOWFLAKE_USER=<your_snowflake_user>
SNOWFLAKE_PASSWORD=<your_snowflake_password>
SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
```

### Set up Snowflake components:

Run the provided SQL script to initialize the required tables and configurations in Snowflake:

```bash
snowsql -f src/setup.sql
```

## Running the Application

To start the Snow Miners application, execute the `main.py` file located in the `src/` directory:

```bash
python src/main.py
```

This will launch the Streamlit-based user interface, where you can:

- **Extract data**: Upload documents or datasets and extract information using predefined AI models.
- **Review extraction jobs**: Monitor the progress and status of your extraction jobs.
- **Analyze data**: Ask questions, run queries, and generate insights from extracted data.
- **Manage metadata**: View and modify metadata related to your data assets.

## Screenshots

Below are some screenshots of the Snow Miners application:

### Dashboard
![Dashboard](images/IMG-20241009-WA0003.jpg)

### Extract Data
![Extract](images/IMG-20241009-WA0004.jpg)

### Review Jobs
![Review](images/IMG-20241009-WA0005.jpg)

### Analyze Data
![Analyze](images/IMG-20241009-WA0006.jpg)

## Contributing

We welcome contributions! If you’d like to improve Snow Miners, feel free to:

- Submit a pull request
- Open an issue for bug reports or feature requests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
