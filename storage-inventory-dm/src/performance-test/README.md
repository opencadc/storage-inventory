# Performance Tests

Testing was done using a product called [Locust](https://locust.io).  It runs the Python `vcp` client to issue uploads to a configured site.

Locust allows us to scale up Workers (Called Users to Locust) to issue multiple simultaneous requests.  Tests are executed through an intuitive UI, which features the ability to ramp up Workers per second to a set maximum.  Tests run until they are manually stopped.

All directories in here contain various configurations such as a hundred Workers running until ten thousand requests have been issued.
