from audioop import avg
import os
from statistics import stdev
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def find_nearby_vehicles(spark, data_path, geo_coordinates, proximity_size, time_period, vehicle_type_filter=None):

    df = spark.read.option("header", "true").option("delimiter", ";").csv(data_path, inferSchema=True)

    # Filtriranje po vremenskom periodu
    df_filtered_time = df.filter((col("timestep_time") >= time_period[0]) & (col("timestep_time") <= time_period[1]))

    # Filtriranje po tipu vozila (opciono)
    if vehicle_type_filter:
        df_filtered_type = df_filtered_time.filter(col("vehicle_type") == vehicle_type_filter)
    else:
        df_filtered_type = df_filtered_time

    #Filtriranje po blizini, tj. udaljenosti
    df_nearby_vehicles = df_filtered_type.filter(
            (pow(col("vehicle_x") - geo_coordinates[0], 2) + pow(col("vehicle_y") - geo_coordinates[1], 2)) <= pow(proximity_size, 2)
        )
    
    num_vehicles = df_nearby_vehicles.count()
    print(f"Number of nearby vehicles: {num_vehicles}")

    df_nearby_vehicles.show()


def calculate_statistics(spark, data_path, parameter_type, time_period):

    df = spark.read.option("header", "true").option("delimiter", ";").csv(data_path, inferSchema=True)
                          
    # Filtriranje po vremenskom periodu 
    df_filtered = df.filter(
        (col("timestep_time") >= time_period[0]) & 
        (col("timestep_time") <= time_period[1]) #& 
        #(col("vehicle_lane") == street_id)
    )

    if parameter_type == "pollution":
        # Racunanje statistika za sve parametre zagađenja
        parameters = ["vehicle_CO2", "vehicle_CO", "vehicle_HC", "vehicle_NOx", "vehicle_PMx", "vehicle_noise"]
        for pollution_parameter in parameters:
            print(f"Statistics for {pollution_parameter} in the given time period:")
            statistics_by_lane = df_filtered.groupBy("vehicle_lane").agg(
                F.min(pollution_parameter).alias("min_val"),
                F.max(pollution_parameter).alias("max_val"),
                F.avg(pollution_parameter).alias("avg_val"),
                F.mean(pollution_parameter).alias("mean_val"),
                F.variance(pollution_parameter).alias("variance_val"),
                F.stddev(pollution_parameter).alias("stddev_val")
            )
            # Stampanje rezultata za svaku ulicu
            print(f"Statistics for {pollution_parameter} by lane in the given time period:")
            statistics_by_lane.show()
    elif parameter_type == "fuel":
        # Racunanje statistika za sve parametre potrosnje goriva (fuel, electricity)
        parameters = ["vehicle_fuel", "vehicle_electricity"]
        for fuel_parameter in parameters:
            print(f"Statistics for {fuel_parameter} in the given time period:")
            statistics_by_lane = df_filtered.groupBy("vehicle_lane").agg(
                F.min(fuel_parameter).alias("min_val"),
                F.max(fuel_parameter).alias("max_val"),
                F.avg(fuel_parameter).alias("avg_val"),
                F.mean(fuel_parameter).alias("mean_val"),
                F.variance(fuel_parameter).alias("variance_val"),
                F.stddev(fuel_parameter).alias("stddev_val")
            )
            # Stampanje rezultata za svaku ulicu
            print(f"Statistics for {fuel_parameter} by lane in the given time period:")
            statistics_by_lane.show()
    else:
        print("Invalid parameter_type. Please choose 'pollution' or 'fuel'.")


def process_spark_arguments(input_number):

    #spark_arguments = os.getenv('SPARK_APPLICATION_ARGS')
    spark_arguments = "0.00 10.00 100 450.00 440.00 | pollution"

    # Razdvajanje argumenata koji se odnose na task1 od argumenata za Task2
    parts = spark_arguments.split('|', 1)

    if input_number == 1:   # Argumenti za task1
        substrings = parts[0].strip().split()
    elif input_number == 2 and len(parts) == 2: # Argumenti za Task2
        substrings = parts[1].strip().split()
    else:
        return None

    return substrings


if __name__ == "__main__":

    # Inicijalizacija Spark sesije
    spark = SparkSession.builder.appName("DataAnalysisApp").master("local[2]").getOrCreate()

    # Putanja do podataka
    script_directory = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_directory, "input_folder", "sumoTrace.csv")

    # Task1
    task1_args = process_spark_arguments(1)   # Izdvajanje argumenata za Task1
    geo_coordinates = (float(task1_args[3]), float(task1_args[4]))  
    proximity_size = int(task1_args[2])
    time_period = (float(task1_args[0]), float(task1_args[1]))  
    #vehicle_type_filter = "VEHICLE_DEFAULT"  # Opciono
    find_nearby_vehicles(spark, data_path, geo_coordinates, proximity_size, time_period)

    data_path = os.path.join(script_directory, "input_folder", "my_emission_file.csv")

    # Task2
    task2_args = process_spark_arguments(2)
    parameter_type = task2_args[0]  
    calculate_statistics(spark, data_path, parameter_type, time_period)

    # Zatvaranje Spark sesije
    spark.stop()