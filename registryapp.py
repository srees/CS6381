import argparse  # for argument parsing
from configurator import Configurator  # factory class
import logging
import asyncio

# import any other packages you need.

###################################
#
# Parse command line arguments
#
###################################


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Registry Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.
    parser.add_argument("-d", "--disseminate", choices=["direct", "broker"], default="direct",
                        help="Dissemination strategy: direct or via broker; default is direct")
    parser.add_argument("-z", "--zookeeper", default="10.0.0.1", help="IP of Zookeeper")
    parser.add_argument("-k", "--zookeeper_port", default="2181", help="Port of Zookeeper")
    parser.add_argument("-p", "--registry_port", default="5550", help="Port of this registry")
    parser.add_argument("-r", "--dht_port", default="8468", help="Port of the DHT ring")
    parser.add_argument("-v", "--debug", default=logging.WARNING, action="store_true", help="Logging level (see logging package): default WARNING else DEBUG")
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
    args.role = "registry"

    # get hold of the configurator, which is the factory that produces
    # many kinds of artifacts for us
    config = Configurator(args)

    # get a handle to our registry object
    print("Getting registry object")
    registry = config.get_registry()

    # start listening
    print("Registry start requested")
    #asyncio.run(registry.start())
    registry.start()

    print("Exiting registry.")
    registry.die = True


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    main()
