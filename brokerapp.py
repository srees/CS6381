###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton code for the broker application
#
# Created: Spring 2022
#
###############################################

# Note that here I am lumping the discovery and dissemination into a 
# single capability. You could decide to keep the two separate to make
# the code cleaner and extensible

# The basic logic of the broker application will be as follows
#
# (1) Obtain a handle to the specialized broker object (which
# works only as a lookup service for the Direct dissemination
# strategy or the one that also is involved in dissemination)
#
# (2) Do any initialization steps as needed
#
# (3) Start the broker's event loop so that it keeps running forever
# accepting events and handling them at the middleware layer

import argparse  # for argument parsing
from configurator import Configurator  # factory class
# import any other packages you need.

###################################
#
# Parse command line arguments
#
###################################


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.

    parser.add_argument("-z", "--zookeeper", default="10.0.0.1", help="IP Address of Zookeeper")
    parser.add_argument("-k", "--zookeeper_port", default="2181", help="Port of Zookeeper")
    parser.add_argument("-b", "--bind", default="5560", help="Port to broker on")
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
    args.role = "broker"

    # get hold of the configurator, which is the factory that produces
    # many kinds of artifacts for us
    config = Configurator(args)

    # get a handle to our broker object
    print("Getting broker object")
    broker = config.get_broker()

    # get a handle to our registry proxy
    print("Getting registry proxy")
    registry = config.get_registry()

    # register with lookup
    registry.register([])  # no topics for all topics

    # broker needs to have access to the registry to get updates
    # yeah, there are cleaner ways to do this...
    broker.set_registry(registry)

    # wait for kickstart from registry
    print("Waiting for start from registry")
    broker.start()

    print("Exiting app.")
    broker.die = True


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    main()
