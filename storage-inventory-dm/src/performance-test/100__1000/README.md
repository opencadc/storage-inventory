# 1,000 requests across 100 Workers

Details of running a site test of one hundred simultaneous Workers issuing PUTs of a 1KB file
up to 1,000 times.

## Configuration

Configuration Item         | Value
-------------------------- | ------
Average time (ms)          | 3,620
Median time (ms)           | 2,600
Ninetieth percentile (ms)  | 5,000
Request count              | 1,060
Requests per second        | 19.74
Site                       | mach275.cadc.dao.nrc.ca
Worker count               | 100
Worker ramp-up             | 100

## Charts

There are attached charts representing the Worker ramp-up, the response times and how they fluctuated, and the total requests per second break down.