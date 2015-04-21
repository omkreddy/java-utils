# java-utils
This repo contains some useful java utilities.

StripedExecutor : accepts StripedRunnable instances and make sure the tasks
submitted with the same key/stripe will be executed in the same order as task submission.
This code is borrowed/inspired from hazelcast opensource project and from 
http://www.javaspecialists.eu/archive/Issue206.html
