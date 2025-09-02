# Ohio Transit Data Engineering 🚍  

## 📌 Overview  
End-to-end **data engineering project** on Ohio’s public transit (COTA GTFS).  
Built a pipeline that transforms raw CSVs into **analytics-ready datasets** and **dashboards** for insights.  

---

## ⚙️ Tech Stack  
- **Airflow** – Orchestration  
- **BigQuery** – Cloud data warehouse  
- **dbt** – Transformations & tests  
- **Docker + Astro CLI** – Local dev  
- **Looker Studio** – Visualization  

---

## 🚀 What It Does  
- Automates ingestion of GTFS transit files  
- Cleans & models data into staging + marts layers  
- Delivers insights on:  
  - Busiest stops & routes  
  - Service hours (weekday vs weekend)  
  - Headway variability (frequency of service)  

---

## 📊 Outcomes  
- Turned static CSVs into live dashboards  
- Reduced manual reporting with automated ETL  
- Created a scalable pipeline replicable for other cities  

---

## 🛠️ How to Run  
1. **Clone repo & configure GCP credentials**  
   ```bash
   git clone https://github.com/rajanya-lahiri/Ohio_Transit_Data_Engineering.git
   cd Ohio_Transit_Data_Engineering
2. **Start Airflow locally**
   astro dev start
3. **Trigger DAGs** → Loads raw → staging data into BigQuery
4. **Run dbt models**
    cd dbt
	dbt run
	dbt test
5. **Connect Looker Studio** → Build dashboards from marts dataset

---

## 📖 Learnings
	•	Hands-on with cloud data engineering
	•	Designing raw → staging → marts pipelines
	•	Turning messy GTFS data into usable insights

---

## 📈 Future Improvements
	•	Automate daily GTFS updates
	•	Add real-time bus tracking feeds
	•	Expand analysis to multiple cities
