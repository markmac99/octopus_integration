# octopus_integration
Using the Octopus Energy API to post data to my openhab influxdb database

This code does the following

* Collects data from Octopus's APIS

* Posts the last 30 minutes consumption data to InfluxDB tables

* Calculates the daily consumption and posts that to InfluxDB tables

# Solar forecast

Pull a solar energy forecast from forecast.solar using their API

All the above run as cron jobs. 

