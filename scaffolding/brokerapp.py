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
