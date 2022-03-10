###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose:
# This is an example showing a list of pre-defined topics from which publishers
# and subscribers can choose which ones they would like to use for publication
# and subscription, respectively.
#
# To be used by a publisher or subscriber application logic
#
# Created: Spring 2022
#
###############################################

# since we are going to publish or subscribe to a random sampling of topics,
# we need this package
import random


# define a helper class to hold all the topics that we support in our system
class TopicList:
    # some pre-defined topics from which a publisher or subscriber chooses
    # from
    topiclist = ["weather", "humidity", "airquality", "light",
                 "pressure", "temperature", "sound", "altitude",
                 "location", "smell"]

    # return a random subset of topics from this list, which becomes our interest
    # A publisher or subscriber application logic will invoke this method
    def interest(self):
        # here we just randomly create a subset from this list and return it
        return random.sample(self.topiclist, random.randint(1, len(self.topiclist)))
