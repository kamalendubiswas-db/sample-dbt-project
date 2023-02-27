Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


dbt-databricks adapter [in dbt repo](https://github.com/databricks/dbt-databricks)

### Project structure:
```bash
.
├── README.md
├── databricks_demo
│   ├── README.md
│   ├── analyses
│   ├── macros
│   ├── models
│   │   ├── bronze
│   │   │   ├── agriculture.sql
│   │   │   ├── bronze_schema.yml
│   │   │   ├── buildings.sql
│   │   │   ├── fluorinated_gases.sql
│   │   │   ├── fossil_fuel_operations.sql
│   │   │   ├── manufacturing.sql
│   │   │   ├── mineral_extraction.sql
│   │   │   ├── power.sql
│   │   │   ├── transportation.sql
│   │   │   └── waste.sql
│   │   ├── gold
│   │   │   ├── emission_trend.sql
│   │   │   ├── gas_trend.sql
│   │   │   └── gold_schema.yml
│   │   └── silver
│   │       ├── overall_emission_data.sql
│   │       └── silver_schema.yml
│   ├── seeds
│   ├── snapshots
│   └── tests
├── dbt_project.yml
└── logs
    └── dbt.log
```