# SparkAmznProducts
PySpark script to transform remote data with an EMR cluster

## Concept
This is a relatively simple application which uses SSH to trigger a PySpark (Python) script hosted in an S3 bucket. The script runs on an EMR cluster (distributed EC2 architecture) and processes Amazon Product data - including reviews and prices for a range of products within each category - in order to generate insights from the transformed data

## Architecture & Security
The architecture is centered around an EMR cluster which integrates with an S3 bucket for input, output and logging. Each node in the cluster is an EC2 instance. There is a VPC for extra security and appropriate SSH certificates and security groups for a secure workflow.

## Logging The First Process
Please refer to the file "first_program_run_log.txt" for a full log of the first runtime. Here is the output corresponding to the python terminal:
```
24/09/22 12:28:37 INFO DAGScheduler: Job 2 finished: count at NativeMethodAccessorImpl.java:0, took 0.185285 s
Number of rows in SQL query: 129
```

## Example Input Data
Here is an example of the Amazon Product Data (available freely on Kaggle), used to generate the insights:

| Name | Main Category | Sub Category | Image | Link | Ratings | Number of Ratings | Discount Price | Actual Price |
|---|---|---|---|---|---|---|---|---|
| Lloyd 1.5 Ton 3 Star Inverter Split Ac... | appliances | Air Conditioners | *ommitted* | *ommitted* | 4.2 | 2,255 | ₹32,999 | ₹58,990 |
| LG 1.5 Ton 5 Star AI DUAL Inverter Split AC... | appliances | Air Conditioners | *ommitted* | *ommitted* | 4.2 | 2,948 | ₹46,490 | ₹75,990 |
| LG 1 Ton 4 Star Ai Dual Inverter Split Ac... | appliances | Air Conditioners | *ommitted* | *ommitted* | 4.2 | 1,206 | ₹34,490 | ₹61,990 |

## Example Output Data
Following transformation, the data is available with a new column (relative_discount_price) which is a decimal representation of the discount price, calculated by the ratio of discount price to actual price. Only products with more than 100 ratings have been considered.

| name | main_category | sub_category | discount_price | actual_price | relative_discount_price |
|---|---|---|---|---|---|
| LG 1.5 Ton 3 Star Hot & Cold DUAL Inverter Spl... | appliances | Air Conditioners | None | None | NaN |
| Hitachi 1.5 Ton 5 Star Inverter Split AC (Copp... | appliances | Air Conditioners | None | None | NaN |
| Panasonic 1.5 Ton 4 Star Wi-Fi Twin-Cool Inver... | appliances | Air Conditioners | None | None | NaN |
| Blue Star 1 Ton Fixed Speed Portable AC (Coppe... | appliances | Air Conditioners | 32990 | 39000 | 0.845897 |
| Blue Star 1 Ton 5 Star Fixed Speed Window AC (... | appliances | Air Conditioners | 31500 | 37000 | 0.851351 |
| Voltas 1 Ton 3 Star Window AC (123 Lyi/123 LZF... | appliances | Air Conditioners | 27990 | 29490 | 0.949135 |
