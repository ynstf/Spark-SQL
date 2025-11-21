package ma.atif;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BikeSharingAnalysis {
    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Bike Sharing Analysis")
                .master("local[*]")
                .getOrCreate();

        //1 Load CSV dataset
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("bike_sharing.csv");

        //2 Print schema
        df.printSchema();

        //3 Show first 5 rows
        df.show(5);

        //4 Count the number of rentals
        long rentalCount = df.count();
        System.out.println("Total number of rentals: " + rentalCount);

        //5 Create a temporary view called "bike_rentals_view"
        df.createOrReplaceTempView("bike_rentals_view");

//        long rentalCountSQL = spark.sql("SELECT COUNT(*) AS total_rentals FROM bike_rentals_view")
//                .collectAsList()
//                .get(0)
//                .getLong(0);
//        System.out.println("Total number of rentals (SQL): " + rentalCountSQL);


        //6 List all rentals longer than 30 minutes
        Dataset<Row> longRentals = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
        );
        longRentals.show();

        //7 Show all rentals starting at "Station A"
        Dataset<Row> stationARentals = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
        );
        stationARentals.show();


        //8 Calculate total revenue (sum of price)
        Dataset<Row> totalRevenue = spark.sql(
                "SELECT SUM(price) AS total_revenue FROM bike_rentals_view"
        );
        totalRevenue.show();

        //9 Count how many rentals were made from each start station
        Dataset<Row> rentalsPerStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY rental_count DESC"
        );
        rentalsPerStation.show();

        //10 Compute the average rental duration per start station
        Dataset<Row> avgDurationPerStation = spark.sql(
                "SELECT start_station, AVG(duration_minutes) AS avg_duration " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY avg_duration DESC"
        );
        avgDurationPerStation.show();

        //11 Identify the station with the highest number of rentals
        Dataset<Row> topStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY rental_count DESC " +
                        "LIMIT 1"
        );
        topStation.show();

        //12 Extract the hour from start_time
        Dataset<Row> rentalsWithHour = spark.sql(
                "SELECT *, HOUR(start_time) AS hour " +
                        "FROM bike_rentals_view"
        );
        rentalsWithHour.show(5);

        //13 Count how many bikes were rented per hour (peak hours)
        Dataset<Row> rentalsPerHour = spark.sql(
                "SELECT HOUR(start_time) AS hour, COUNT(*) AS rentals_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY HOUR(start_time) " +
                        "ORDER BY rentals_count DESC"
        );
        rentalsPerHour.show();

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

        //15 Compute the average age of users
        Dataset<Row> avgAge = spark.sql(
                "SELECT AVG(age) AS average_age FROM bike_rentals_view"
        );
        avgAge.show();

        //16 Count users by gender
        Dataset<Row> usersByGender = spark.sql(
                "SELECT gender, COUNT(*) AS count FROM bike_rentals_view GROUP BY gender"
        );
        usersByGender.show();


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


        spark.stop();
    }
}
