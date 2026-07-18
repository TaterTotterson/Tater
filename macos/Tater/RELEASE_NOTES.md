# Tater v96.3

## What's Changed

- Fixed temperature-unit detection so device states such as `Closed` cannot be mistaken for Celsius, while explicit Celsius and Fahrenheit readings remain correctly identified.
- Missing temperature and measurement values now report as unavailable instead of reusing an unrelated device state, keeping structured Little Spud readings safe for mixed-unit averages.
- Added regression coverage for nested Hue Celsius readings, Ecobee Fahrenheit readings, and non-temperature device states without changing other integration or provider paths.
