###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton logic for the subscriber application
#
# Created: Spring 2022
#
###############################################


# The basic logic of the subscriber application will be as follows (see pubapp.py
# for additional details.
#
# (1) The subscriber app decides which all topics it is going to subscriber to.
# (see pubapp.py file for how the publisher gets its interest randomly chosen
# for it.
#
# (2) the application obtains a handle to the broker (which under the
# hood will be a proxy object but the application doesn't know it is a
# proxy).
#
# (3) Register with the broker letting it know all the topics it is interested in
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Obtain a handle to the subscriber object (which maybe a specialized
# object depending on whether it is using the direct dissemination or
# via broker)
#
# (5) Wait for the broker to let us know who our publishers are for the
# topics we are interested in. In the via broker approach, the broker is the
# only publisher for us.
#
#
# (5) Have a receiving loop. See scaffolding code for polling where
# we show how different ZMQ sockets can be polled whenever there is
# an incoming event.
#
#    In each iteration, handle all the events that have been enabled by
#    receiving the data. Get the timestamp and obtain the latency for each
#    received topic from each publisher and store this info possibly in a
#    time series database like InfluxDB
#
# (6) if you have logic that allows this forever loop to terminate, then
# go ahead and clean up everything.
