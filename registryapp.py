import argparse  # for argument parsing
from configurator import Configurator  # factory class
import logging


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
    parser.add_argument("-r", "--registry", default="127.0.0.1", help="IP of the registry bootstrap")
    parser.add_argument("-p", "--port", default="5550", help="Port of the registry")
    parser.add_argument("-o", "--override_port", default="5540", help="Port for kademlia DHT ring communications")
    parser.add_argument("-c", "--pubs", default=5, type=int, help="Number of publishers")
    parser.add_argument("-s", "--subs", default=5, type=int, help="Number of subscribers")
    parser.add_argument("-k", "--kademlia", action="store_true")
    parser.add_argument("-i", "--create", action="store_true")
    parser.add_argument("-v", "--debug", default=logging.WARNING, action="store_true",help="Logging level (see logging package): default WARNING else DEBUG")
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
    registry.start()


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    main()
