# README — TP Spark SQL (Bike Sharing Analysis)

**Author:** Atif Youness (project files provided)

**What this README contains**

* For each TP question (1 → 17) I list: the question, the code/snippet used to answer it (Java + Spark SQL), and the observed result from running the program on `bike_sharing.csv`.
* Full program and execution log are included in the project files; this README contains a concise, well-organized presentation ready to include in your repository.

---

## Project: BikeSharingAnalysis (Spark SQL, Java)

**Main file:** `BikeSharingAnalysis.java`

This program:

1. Loads `bike_sharing.csv` as a DataFrame.
2. Prints schema and head rows.
3. Creates a temp view `bike_rentals_view`.
4. Executes a series of Spark SQL queries (17 questions) and shows their results.

---

## Questions, code and results

> Note: code snippets below are exact SQL statements executed inside the Java program. The full Java program contains the SparkSession setup and DataFrame loading; these SQL snippets are run via `spark.sql("...")` and the result displayed with `.show()`.


---

# 1) Schema (column names + types)

**Code (Java):**

```java
//1 Load CSV dataset
Dataset<Row> df = spark.read()
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("bike_sharing.csv");

//2 Print schema
df.printSchema();
```

```
root
 |-- rental_id: integer (nullable = true)
 |-- user_id: integer (nullable = true)
 |-- age: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- start_time: timestamp (nullable = true)
 |-- end_time: timestamp (nullable = true)
 |-- start_station: string (nullable = true)
 |-- end_station: string (nullable = true)
 |-- duration_minutes: integer (nullable = true)
 |-- price: double (nullable = true)
```

Source: printed schema in the run log. 

---

# 2) First 5 rows (dataset head)

**Code (Java):**

```java
//3 Show first 5 rows
df.show(5);
```

| rental_id | user_id | age | gender | start_time          | end_time            | start_station | end_station | duration_minutes | price |
| --------: | ------: | --: | :----- | :------------------ | :------------------ | :------------ | :---------- | ---------------: | ----: |
|         1 |    1001 |  25 | M      | 2025-11-01 08:15:00 | 2025-11-01 08:45:00 | Station A     | Station D   |               30 |   3.5 |
|         2 |    1002 |  32 | F      | 2025-11-01 09:00:00 | 2025-11-01 09:20:00 | Station B     | Station E   |               20 |   2.5 |
|         3 |    1003 |  28 | M      | 2025-11-01 10:30:00 | 2025-11-01 11:05:00 | Station C     | Station F   |               35 |   4.0 |
|         4 |    1004 |  22 | F      | 2025-11-01 11:15:00 | 2025-11-01 11:50:00 | Station D     | Station A   |               35 |   4.0 |
|         5 |    1005 |  45 | M      | 2025-11-01 12:05:00 | 2025-11-01 12:40:00 | Station E     | Station B   |               35 |   4.0 |

(From the run output — top 5 rows). 

---

# 3) Total number of rentals

**Code (Java):**

```java
//4 Count the number of rentals
long rentalCount = df.count();
System.out.println("Total number of rentals: " + rentalCount);
```

**Total number of rentals:** `10`
Printed in the run output. 

---

# 4) All rentals longer than 30 minutes

Rows returned (duration_minutes > 30):

**Code (Java):**

```java
//5 Create a temporary view called "bike_rentals_view"
df.createOrReplaceTempView("bike_rentals_view");
//6 List all rentals longer than 30 minutes
Dataset<Row> longRentals = spark.sql(
        "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
);
longRentals.show();
```


| rental_id | user_id | age | gender | start_time          | end_time            | start_station | end_station | duration_minutes | price |
| --------: | ------: | --: | :----- | :------------------ | :------------------ | :------------ | :---------- | ---------------: | ----: |
|         3 |    1003 |  28 | M      | 2025-11-01 10:30:00 | 2025-11-01 11:05:00 | Station C     | Station F   |               35 |   4.0 |
|         4 |    1004 |  22 | F      | 2025-11-01 11:15:00 | 2025-11-01 11:50:00 | Station D     | Station A   |               35 |   4.0 |
|         5 |    1005 |  45 | M      | 2025-11-01 12:05:00 | 2025-11-01 12:40:00 | Station E     | Station B   |               35 |   4.0 |
|         7 |    1007 |  29 | M      | 2025-11-01 14:10:00 | 2025-11-01 14:50:00 | Station A     | Station B   |               40 |   4.5 |
|        10 |    1010 |  41 | F      | 2025-11-01 17:15:00 | 2025-11-01 17:50:00 | Station D     | Station E   |               35 |   4.0 |

Shown in the output for `longRentals.show()`. 

---

# 5) Rentals starting at "Station A"

Rows with `start_station = 'Station A'`:


**Code (Java):**

```java
//7 Show all rentals starting at "Station A"
Dataset<Row> stationARentals = spark.sql(
        "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
);
stationARentals.show();
```


| rental_id | user_id | age | gender | start_time          | end_time            | start_station | end_station | duration_minutes | price |
| --------: | ------: | --: | :----- | :------------------ | :------------------ | :------------ | :---------- | ---------------: | ----: |
|         1 |    1001 |  25 | M      | 2025-11-01 08:15:00 | 2025-11-01 08:45:00 | Station A     | Station D   |               30 |   3.5 |
|         7 |    1007 |  29 | M      | 2025-11-01 14:10:00 | 2025-11-01 14:50:00 | Station A     | Station B   |               40 |   4.5 |

From `stationARentals.show()`. 

---

# 6) Total revenue (sum of `price`)

**Code (Java):**

```java
//8 Calculate total revenue (sum of price)
Dataset<Row> totalRevenue = spark.sql(
        "SELECT SUM(price) AS total_revenue FROM bike_rentals_view"
);
totalRevenue.show();
```

| total_revenue |
| ------------: |
|          36.5 |

Result printed by `totalRevenue.show()`. 

---

# 7) Number of rentals per start station

**Code (Java):**

```java
Dataset<Row> rentalsPerStation = spark.sql(
        "SELECT start_station, COUNT(*) AS rental_count " +
                "FROM bike_rentals_view " +
                "GROUP BY start_station " +
                "ORDER BY rental_count DESC"
);
rentalsPerStation.show();
```

| start_station | rental_count |
| :------------ | -----------: |
| Station C     |            2 |
| Station D     |            2 |
| Station B     |            2 |
| Station A     |            2 |
| Station E     |            1 |
| Station F     |            1 |

Produced by `rentalsPerStation.show()`. 

---

# 8) Average rental duration per start station (`avg_duration`)


**Code (Java):**

```java
//10 Compute the average rental duration per start station
Dataset<Row> avgDurationPerStation = spark.sql(
        "SELECT start_station, AVG(duration_minutes) AS avg_duration " +
                "FROM bike_rentals_view " +
                "GROUP BY start_station " +
                "ORDER BY avg_duration DESC"
);
avgDurationPerStation.show();
```

| start_station | avg_duration |
| :------------ | -----------: |
| Station D     |         35.0 |
| Station E     |         35.0 |
| Station A     |         35.0 |
| Station C     |         30.0 |
| Station F     |         30.0 |
| Station B     |         25.0 |

From `avgDurationPerStation.show()`. 

---

# 9) Station with the highest number of rentals (top 1)

**Code (Java):**

```java
//11 Identify the station with the highest number of rentals
Dataset<Row> topStation = spark.sql(
        "SELECT start_station, COUNT(*) AS rental_count " +
                "FROM bike_rentals_view " +
                "GROUP BY start_station " +
                "ORDER BY rental_count DESC " +
                "LIMIT 1"
);
topStation.show();
```

| start_station | rental_count |
| :------------ | -----------: |
| Station C     |            2 |

Note: several stations share the highest count (2 each), but the `LIMIT 1` query returned `Station C` in one of the runs. (see run output). 

---

# 10) Extracted hour (example — first 5 rows of `rentalsWithHour`)

The query added column `hour` (HOUR(start_time)) — first 5 rows (hour column shown in head):

**Code (Java):**

```java
//12 Extract the hour from start_time
Dataset<Row> rentalsWithHour = spark.sql(
        "SELECT *, HOUR(start_time) AS hour " +
                "FROM bike_rentals_view"
);
rentalsWithHour.show(5);
```

| rental_id | ... | price | hour |
| --------: | --: | ----: | ---: |
|         1 | ... |   3.5 |    8 |
|         2 | ... |   2.5 |    9 |
|         3 | ... |   4.0 |   10 |
|         4 | ... |   4.0 |   11 |
|         5 | ... |   4.0 |   12 |

(Shown by `rentalsWithHour.show(5)`). 

---

# 11) Number of rentals per hour (`rentalsPerHour`)

**Code (Java):**

```java
//13 Count how many bikes were rented per hour (peak hours)
Dataset<Row> rentalsPerHour = spark.sql(
        "SELECT HOUR(start_time) AS hour, COUNT(*) AS rentals_count " +
                "FROM bike_rentals_view " +
                "GROUP BY HOUR(start_time) " +
                "ORDER BY rentals_count DESC"
);
rentalsPerHour.show();
```

| hour | rentals_count |
| ---: | ------------: |
|   12 |             1 |
|   13 |             1 |
|   16 |             1 |
|   15 |             1 |
|    9 |             1 |
|   17 |             1 |
|    8 |             1 |
|   10 |             1 |
|   11 |             1 |
|   14 |             1 |

All hours present in dataset had 1 rental each. (From `rentalsPerHour.show()`). 

---

# 12) Most popular start station during the morning (7–12)

**Code (Java):**

```java
//14 Determine the most popular start station during the morning (7–12)
Dataset<Row> popularMorningStation = spark.sql(
        "SELECT start_station, COUNT(*) AS rental_count " +
                "FROM bike_rentals_view " +
                "WHERE HOUR(start_time) BETWEEN 7 AND 12 " +
                "GROUP BY start_station " +
                "ORDER BY rental_count DESC " +
                "LIMIT 1"
);
popularMorningStation.show();
```

| start_station | rental_count |
| :------------ | -----------: |
| Station C     |            1 |

The morning-window query returned `Station C` (count = 1) in the run. 

---

# 13) Average age of users


**Code (Java):**

```java
//15 Compute the average age of users
Dataset<Row> avgAge = spark.sql(
        "SELECT AVG(age) AS average_age FROM bike_rentals_view"
);
avgAge.show();
```

| average_age |
| ----------: |
|        31.8 |

From `avgAge.show()`. 

---

# 14) Count users by gender

**Code (Java):**

```java
//16 Count users by gender
Dataset<Row> usersByGender = spark.sql(
        "SELECT gender, COUNT(*) AS count FROM bike_rentals_view GROUP BY gender"
);
usersByGender.show();
```

| gender | count |
| :----- | ----: |
| F      |     5 |
| M      |     5 |

From `usersByGender.show()`. 

---

# 15) Rentals by age group

**Code (Java):**

```java
//17 Find which age group rents bicycles the most
Dataset<Row> ageGroups = spark.sql(
        "SELECT " +
                "CASE " +
                "  WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                "  WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                "  WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                "  ELSE '51+' " +
                "END AS age_group, " +
                "COUNT(*) AS rental_count " +
                "FROM bike_rentals_view " +
                "GROUP BY age_group " +
                "ORDER BY rental_count DESC"
);
ageGroups.show();
```

| age_group | rental_count |
| :-------- | -----------: |
| 18-30     |            5 |
| 31-40     |            3 |
| 41-50     |            2 |

From the age-group aggregation result. 

---
