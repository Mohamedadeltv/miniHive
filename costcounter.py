import glob
import json

# Global variable to track the total cost (tuples processed)
total_cost = 0

def increment(n):
    """
    Increments the total cost by n.
    Called by MapReduce tasks to log their I/O or processing volume.
    """
    global total_cost
    total_cost += n

def reset():
    """
    Resets the total cost to 0.
    Required by the verification scripts to run multiple queries in one session.
    """
    global total_cost
    total_cost = 0

def compute_hdfs_costs():
    """
    Computes costs from temporary HDFS files (if used in that mode).
    """
    costs = 0
    files = glob.glob('./*.tmp')
    for file in files:
        f = open(file, 'r')
        for line in f:
            key, value = line.split('\t')
            json_tuple = json.loads(value)
            costs += len(json.dumps(json_tuple))
        f.close()
    return costs