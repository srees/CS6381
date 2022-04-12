import time
import argparse


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Test Rig Delay Application")
    parser.add_argument("-d", "--delay", default="5", help="Number of seconds to delay")

    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    args = parseCmdLineArgs()
    print("Adding " + args.delay + " seconds...")
    time.sleep(int(args.delay))


if __name__ == "__main__":
    main()
