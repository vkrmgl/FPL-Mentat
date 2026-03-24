This project was a tool created mainly to increase my rank in my Fantasy Premier League. The name is inspired by the Sci-Fi fantasy novel Dune, where a "Mentat" is a human with computer-like analytical powers who provides valuable political insights to the ruling class.

Fitting with that theme, this tool predicts which players are most likely to earn the highest amount of points in a given gameweek.

Along with helping me gain fantasy league rank, I also wanted to spend my free time working on a project that gave me experience with some essential Data Engineering tools that I haven't had the opportunity to use in industry(DuckDB, dbt, and Airflow).

The Tech Stack used to create this project include-
- Python: Data ingestion
- DuckDB: Bronze data layer
- SQL: Used along with DuckDB/dbt to transform data
- dbt: Used to process data through the Silver/Gold layer
- Airflow: Pipeline orchestration. Running on a Raspberry pi.
- (tbd) LLM [Llama]: To process news data
