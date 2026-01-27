import pandas as pd

# # Load the green trip data (Parquet format)
# #df = pd.read_parquet('green_tripdata_2025-11.parquet')
df = pd.read_parquet('green_tripdata_2025-11.parquet')


# # Check the columns to understand the data
df.columns

# Convert the pickup datetime to datetime format if needed
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])

# Filter the data for trips between '2025-11-01' and '2025-12-01' and where trip_distance <= 1
filtered_df = df[(df['lpep_pickup_datetime'] >= '2025-11-01') & 
                 (df['lpep_pickup_datetime'] < '2025-12-01') & 
                 (df['trip_distance'] <= 1)]

# Count the number of rows
short_trip_count = filtered_df.shape[0]
print(short_trip_count)

#Question 4:
# Filter trips where the trip_distance is less than 100 miles
df_filtered = df[df['trip_distance'] < 100]

# Extract the pickup day from the pickup datetime
df_filtered['pickup_day'] = df_filtered['lpep_pickup_datetime'].dt.date

# Group by pickup day and get the max trip_distance for each day
longest_trips_per_day = df_filtered.groupby('pickup_day')['trip_distance'].max()

# Sort the result in descending order to get the longest trip
longest_trips_per_day_sorted = longest_trips_per_day.sort_values(ascending=False)

# Display the result
print(longest_trips_per_day_sorted)
# Get the day with the longest trip
longest_trip_day = longest_trips_per_day_sorted.idxmax()
print(f"The day with the longest trip is: {longest_trip_day}")

#Question 5:
zones = pd.read_csv("taxi_zone_lookup.csv")
trips = pd.read_parquet("green_tripdata_2025-11.parquet")
trips["lpep_pickup_datetime"] = pd.to_datetime(trips["lpep_pickup_datetime"])
trips_18 = trips[
    trips["lpep_pickup_datetime"].dt.date == pd.to_datetime("2025-11-18").date()
]
trips_18 = trips_18.merge(
    zones,
    left_on="PULocationID",
    right_on="LocationID",
    how="left"
)
zone_totals = (
    trips_18
    .groupby("Zone")["total_amount"]
    .sum()
    .sort_values(ascending=False)
)

print(zone_totals.head(10))
top_zone = zone_totals.idxmax()
top_zone

#Question 6:
import pandas as pd

# Load the datasets
trips = pd.read_parquet("green_tripdata_2025-11.parquet")
zones = pd.read_csv("taxi_zone_lookup.csv")

# Convert the pickup datetime to datetime format
trips["lpep_pickup_datetime"] = pd.to_datetime(trips["lpep_pickup_datetime"])

# Get the LocationID for "East Harlem North"
east_harlem_north_id = zones[zones["Zone"] == "East Harlem North"]["LocationID"].values[0]

# Filter trips for pickups in "East Harlem North"
trips_east_harlem_north = trips[
    (trips["lpep_pickup_datetime"].dt.month == 11) &
    (trips["lpep_pickup_datetime"].dt.year == 2025) &
    (trips["PULocationID"] == east_harlem_north_id)
]

# Merge with the zones data to get drop-off zone names
trips_east_harlem_north = trips_east_harlem_north.merge(
    zones[['LocationID', 'Zone']],  # Merge with 'LocationID' and 'Zone'
    left_on="DOLocationID",  # DOLocationID corresponds to the drop-off zone
    right_on="LocationID",  # Merge using LocationID from zones DataFrame
    how="left",  # Left join to keep all filtered trips
    suffixes=("_pickup", "_dropoff")  # To avoid any column name conflicts
)

# Rename the 'Zone' column to 'Zone_dropoff' for clarity
trips_east_harlem_north.rename(columns={'Zone': 'Zone_dropoff'}, inplace=True)

# Group by drop-off zone and calculate the total tip amount
dropoff_zone_totals = (
    trips_east_harlem_north.groupby("Zone_dropoff")["tip_amount"]
    .sum()
    .sort_values(ascending=False)
)

# Identify the drop-off zone with the largest tip
top_dropoff_zone = dropoff_zone_totals.idxmax()
print(f"The drop-off zone with the largest tip is: {top_dropoff_zone}")
