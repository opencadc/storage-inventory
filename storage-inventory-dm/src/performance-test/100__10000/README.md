# 10,000 requests across 100 Workers

Details of running a site test of one hundred simultaneous Workers issuing PUTs of a 1KB file
up to 10,000 times.

## Configuration

Configuration Item         | Value
-------------------------- | ------
Average time (ms)          | 3,261
Median time (ms)           | 2,300
Ninetieth percentile (ms)  | 4,300
Request count              | 10,000
Requests per second        | 23.2
Site                       | mach275.cadc.dao.nrc.ca
Worker count               | 100
Worker ramp-up             | 2

## Charts

There are attached charts representing the Worker ramp-up, the response times and how they fluctuated, and the total requests per second break down.