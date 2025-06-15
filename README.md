# US Used Cars Analysis – Big Data Project 2025

Analysis of the **US Used Cars Dataset** (Kaggle) with over 3 million records on used cars listed in the US up to 2020. Developed for the Big Data course at Roma Tre University.

##  Dataset

- **Format**: CSV (66 columns)
- **Key fields**: `make_name`, `model_name`, `year`, `price`, `city`, `daysonmarket`, `description`, `horsepower`, `engine_displacement`

##  Technologies

- Hadoop MapReduce
- Apache Hive
- Apache Spark SQL

##  Implemented Jobs

### Job 1 – Brand & Model Statistics

For each **car brand**:
- List of **models**
- **Count** of listings
- **Min, max, avg price**
- **Years** available

### Job 2 – Price Bands by City & Year

For each **city** and **year**:
- Count of cars in **price bands**:
  - Low: < 20K USD  
  - Mid: 20K–50K USD  
  - High: > 50K USD  
- **Avg. days on market**
- **Top 3 frequent words** in descriptions

### Job 3 – Similar Engine Groups

Group car models with **similar engine specs**:
- ±10% in **horsepower** and **engine displacement**
- For each group:
  - **Avg. price**
  - **Model with max horsepower**

##  Setup & Execution

```bash
git clone https://github.com/ricmoccia/progetto-big-data.git
