| Attribute            | Type               | Description                                       |
|----------------------|--------------------|---------------------------------------------------|
| `timestamp`          | string (date-time) | Observation timestamp in ISO 8601 format          |
| `station_id`         | integer            | Unique AQICN station identifier                   |
| `station_name`       | string             | Name of the monitoring station                    |
| `latitude`           | float              | Station latitude                                  |
| `longitude`          | float              | Station longitude                                 |
| `aqi`                | integer / null     | Air Quality Index value                           |
| `dominant_pollutant` | string / null      | Main pollutant affecting AQI                      |
| `pollutants`         | object (map)       | Dynamic mapping of pollutant names to values      |
| `pollutants.<name>`  | float / null       | Pollutant value (`pm10`, `o3`, `no2`, `co`, etc.) |
| `temperature`        | float / null       | Temperature (°C)                                  |
| `humidity`           | float / null       | Humidity (%)                                      |
| `wind`               | float / null       | Wind speed                                        |
| `pressure`           | float / null       | Atmospheric pressure                              |
| `dew`                | float / null       | Dew point                                         |
