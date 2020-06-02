# Data:

The data was obtained as an intermediate state via JSON files.  The respective JSON files were then used for upload directly to InfluxDB, or via the Kafka producer script.  

The actual data is not contained here, but can be obtained with the following script files

- ClimateJSON
  - Climate relevant data as obtained from the energy-charts.de website

- PlantPowerJSON
  - Plant operation characteristics based on plant type
  
- PriceEnergyJSON
  - German energy market price information
  
- WeatherData
  - Python script to gather weather data from the openweathermap.org API
