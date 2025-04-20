# NBA Data App - dbt Project

This project transforms raw NBA data into meaningful models for analysis and reporting using dbt (data build tool).

## Overview

- **Purpose**: Transform and model NBA data for business intelligence, reporting, and advanced analytics.
- **Layers**: The project is organized into multiple layers:
  - **Staging**: Cleans and standardizes raw data.
  - **Intermediate**: Implements business logic and intermediate transformations.
  - **Mart**: Delivers business-facing models.
  - **ML Features**: Provides models for machine learning feature engineering.

## Directory Structure

```
nba_dbt/
├── dbt_packages/    # Dependencies from dbt packages
├── docs/            # Documentation (e.g., style guide, data dictionaries)
├── logs/            # Logs generated from dbt runs
├── macros/          # dbt macros for reusable SQL code
├── models/          # SQL models organized by layer
│   ├── staging/          # Raw data cleaning and initial transformations
│   ├── intermediate/     # Business logic and intermediate transformations
│   ├── mart/            # Business-facing final models
│   └── ml_features/     # Machine learning feature engineering models
├── snapshots/       # Snapshot models capturing historical data
├── seeds/           # CSV seeds for small, static datasets
├── tests/           # Schema and data tests
├── analyses/        # Ad hoc analyses and exploratory queries
├── dbt_project.yml  # Main dbt configuration file
├── packages.yml     # Package dependencies configuration
├── package-lock.yml # Dependency lock file
├── README.md        # This project overview
└── .gitignore       # Files to ignore in version control
```

## Getting Started

### Prerequisites

- [dbt](https://docs.getdbt.com/) installed
- A configured `profiles.yml` for your connection profiles (refer to dbt docs for details)

### Setup Instructions

1. Clone the repository.
2. Install dependencies:
   ```
   dbt deps
   ```
3. Test your configuration:
   ```
   dbt debug
   ```
4. Run the models:
   ```
   dbt run
   ```
5. Run tests:
   ```
   dbt test
   ```

## Project Conventions

### Style Guide

This project adheres to a strict style guide for model naming, SQL formatting, and column abbreviations. Please refer to our detailed [Style Guide](docs/styling_guide.md) for more information.

### Naming Conventions

- Models are named following these prefixes:
  - **Staging**: `stg__[entity]_[type]`
  - **Intermediate**: `int__[entity]_[description]`
  - **Mart**: `mart_[domain]_[entity]`
  - **ML Feature Models**: `ml_[feature_type]_[description]`

### Configuration Standardization

All models follow consistent configuration patterns:

- **Staging Models**: 
  - All include explicit `config()` blocks with `schema='staging'` and `materialized='view'`
  - Some may also include incremental logic for efficient processing
  
- **Intermediate Models**:
  - Use incremental materialization with appropriate configuration
  - Include partitioning for performance optimization
  
- **Feature Models**:
  - Follow similar patterns to intermediate models
  - Optimized for analytical query performance

Configuration is specified in both individual model files and the `dbt_project.yml` file, with model-specific settings taking precedence.

### Type Handling Standards

- Explicit type casting for all fields
- Precise decimal specifications for percentages and floating point values
- Consistent handling for special data types (e.g., minutes)

### Testing & Documentation

- Models include tests for unique constraints, non-null values, and referential integrity.
- Relationship tests connect models to establish data lineage
- Range validation for percentage fields and other bounded values
- Documentation is embedded in models via comments and enhanced in external docs.

## Running the Project

- **Build Models**: `dbt run`
- **Run Tests**: `dbt test`
- **Generate Documentation**:
  1. ```
     dbt docs generate
     ```
  2. ```
     dbt docs serve
     ```

## Contribution Guidelines

- Fork the repository and create a feature branch for your changes.
- Ensure code adheres to the style guide and naming conventions.
- Write tests for any new models or changes.
- Submit pull requests for review.

## Contact & Support

For questions or support, please open an issue or contact the project maintainers.

## License

[Specify License Information Here]

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
