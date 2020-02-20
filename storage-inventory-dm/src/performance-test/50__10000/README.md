# 10,000 requests across 50 Workers

Details of running a site test of fifty simultaneous Workers issuing PUTs of a 1KB file
up to 10,000 times.

## Configuration

Configuration Item         | Value
-------------------------- | ------
Average time (ms)          | 1,743
Median time (ms)           | 1,600
Ninetieth percentile (ms)  | 2,200
Request count              | 10,000
Requests per second        | 21.04
Site                       | mach275.cadc.dao.nrc.ca
Worker count               | 50
Worker ramp-up             | 50

## Charts

There are attached charts representing the Worker ramp-up, the response times and how they fluctuated, and the total requests per second break down.