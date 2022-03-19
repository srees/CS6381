###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton code for the publisher application
#
# Created: Spring 2022
#
###############################################


# The core logic of the publisher application will be as follows
# (1) The publisher app decides which all topics it is going to publish.
# We don't care for the values for the topic being published (any
# arbitrary value for that topic is fine)
#
# (2) the application obtains a handle to the broker (which under the
# hood will be a proxy object but the application doesn't know it is a
# proxy).  Moreover, here I am assuming that the lookup and broker are
# lumped together. Else you need to get the lookup service and then
# ask the lookup service to send a representation of the broker
#
# (3) Register with the lookup/broker letting it know all the topics it is publishing
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Obtain a handle to the publisher object (which maybe a specialized
# object depending on whether it is using the direct dissemination or
# via broker approach)
#
# (5) Wait for a kickstart msg from the broker. This is when the publisher knows
# that all subscribers are ready and that it is time for publishers to start
# publishing.
#
# (6) Start a loop on the publisher object for sending of topics
#
#       In each iteration, the app decides (randomly) on which all
#       topics it is going to publish, and then accordingly invokes
#       the publish method of the publisher object passing the topic
#       and its value. 
#
#       Note that additional info like timestamp etc may also need to
#       be sent and the whole thing serialized under the hood before
#       actually sending it out.
#
#
# (6) When the loop terminates, possibly after a certain number of
# iterations of publishing are over, proceed to clean up the objects and exit
#

import argparse  # for argument parsing
import random
from random import randrange
from configurator import Configurator  # factory class
import time

# import any other packages you need.

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Publisher Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.
    parser.add_argument("-r", "--registry", default="127.0.0.1", help="IP Address of the registry")
    parser.add_argument("-p", "--port", default="5550", help="Port of the registry")
    parser.add_argument("-b", "--bind", default="5570", help="Port to publish topic on")
    parser.add_argument("-c", "--count", default="1000", help="Number of publish iterations")

    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    # first parse the arguments
    print("Main: parse command line arguments")
    args = parseCmdLineArgs()
    args.role = "publisher"

    # get hold of the configurator, which is the factory that produces
    # many kinds of artifacts for us
    config = Configurator(args)

    # Ask the configurator to give us a random subset of topics that we can publish
    my_topics = config.get_interest()
    print("Publisher interested in publishing on these topics: {}".format(my_topics))

    # get a handle to our publisher object
    print("Getting publisher object")
    pub = config.get_publisher()

    # get a handle to our registry object (will be a proxy)
    print("Getting registry proxy")
    registry = config.get_registry()

    # register with lookup
    registry.register(my_topics)

    # wait for kickstart event from registry
    print("Waiting for start from registry")
    pub.start()
    # consider adding a check for if message exists (not a timeout) before proceeding

    # now do the publication for as many iterations that we plan to do
    print("Start received.")
    iters = config.get_iterations()
    try:
        for x in range(iters):
            topic = random.sample(my_topics, 1)
            value = randrange(0, 500)
            pub.publish(topic[0], value)
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        registry.stop(my_topics)

###################################
#
# Main entry point
#
###################################


if __name__ == "__main__":
    main()
