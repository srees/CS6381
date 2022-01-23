###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the subscriber proxy in the middleware layer
#
# Created: Spring 2022
#
###############################################

# A proxy for the subscriber will be used in a remote procedure call
# approach.  We envision its use on the broker side when
# it delegates the work to the proxy so the proxy can communicate with
# its real counterpart.  One may completely avoid this approach
# if pure message passing is going to be used and not have a higher level
# remote procedure call approach.
