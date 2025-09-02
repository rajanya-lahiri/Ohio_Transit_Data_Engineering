**Ohio Transit Data Engineering 🚍**

**📌 Overview**

End-to-end data engineering project on Ohio’s public transit (COTA GTFS). Built a pipeline that transforms raw CSVs into analytics-ready datasets and dashboards for insights.

⸻

**⚙️ Tech Stack**
	•	Airflow – Orchestration
	•	BigQuery – Cloud data warehouse
	•	dbt – Transformations & tests
	•	Docker + Astro CLI – Local dev
	•	Looker Studio – Visualization

⸻

**🚀 What It Does**
	•	Automates ingestion of GTFS transit files
	•	Cleans & models data into staging + marts layers
	•	Delivers insights on:
	•	Busiest stops & routes
	•	Service hours (weekday vs weekend)
	•	Headway variability (frequency of service)

⸻

**📊 Outcomes**
	•	Turned static CSVs into live dashboards
	•	Reduced manual reporting with automated ETL
	•	Created a scalable pipeline replicable for other cities

⸻

**🛠️ How to Run**
	1.	Clone repo & configure GCP credentials
	2.	Run Airflow with astro dev start
	3.	Trigger DAGs → Data lands in BigQuery
	4.	Run dbt run → Transformations
	5.	Connect Looker Studio → Dashboards ready

⸻

**📂 Project Architecture**

Raw GTFS Data (COTA CSVs)  
        │  
        ▼  
 Google Cloud Storage (GCS)  
        │  
        ▼  
Apache Airflow DAGs ──────► BigQuery (raw → staging → marts)  
                                │  
                                ▼  
                          dbt Models & Tests  
                                │  
                                ▼  
                         Looker Studio Dashboard  
						 
⸻

**📖 Learnings**
	•	Hands-on with cloud data engineering
	•	Designing raw → staging → marts pipelines
	•	Turning messy GTFS data into usable insights
