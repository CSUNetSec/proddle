#!/usr/bin/python2
from deap import algorithms, base, creator, tools

import math
import multiprocessing
import random
import sys

import dbscan_cluster

IND_SIZE=2
POP_SIZE=50

measurements, distance_matrix = dbscan_cluster.build_distance_matrix("2017.04.03-2017.04.10-100.bin")

def evaluateInd(individual):
    eps = abs(individual[0]) * 100
    min_samples = math.ceil(abs(individual[1]) * 5)

    #perform dbscan algorithm
    labels = dbscan_cluster.compute(distance_matrix, eps, min_samples)

    #compute scores
    #iterate over measurements
    vantages = {}
    total_vantage_count = 0
    total_failure_count = 0
    for i in range(0, len(measurements)):
        label = labels[i]
        if label == -1:
            continue

        #add unique vantage
        vantage = measurements[i]["vantage_hostname"]
        if label not in vantages:
            vantages[label] = []

        if vantage not in vantages[label]:
            vantages[label].append(vantage)
            total_vantage_count += 1

        #add to failure count
        total_failure_count += 1

    #compute number of events
    event_count = len(set(labels)) - (1 if -1 in labels else 0)
    if event_count == 0:
        return (0, 0, 0)

    #compute average vantage count per event
    average_vantages = float(total_vantage_count) / event_count

    #compute average failure count per event
    average_failures = float(total_failure_count) / event_count

    #print "event_count:%d\tvantage:%d-%f\tfailure:%d-%f" % (event_count, total_vantage_count, average_vantages, total_failure_count, average_failures)

    return (average_vantages, 1, average_failures)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s filename" % sys.argv[0]

    #define fitness
    creator.create("FitnessMulti", base.Fitness, weights=(1.0, 1.0, 1.0))
    creator.create("Individual", list, fitness=creator.FitnessMulti)

    #define individual
    toolbox = base.Toolbox()
    toolbox.register("attr_float", random.random)
    toolbox.register("individual", tools.initRepeat, creator.Individual, 
            toolbox.attr_float, n=IND_SIZE)

    #define population
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)
    pop = toolbox.population(n=POP_SIZE)

    #define genetic algorithm operations
    toolbox.register("mate", tools.cxTwoPoint)
    toolbox.register("mutate", tools.mutGaussian, mu=0, sigma=1, indpb=0.2)
    toolbox.register("select", tools.selTournament, tournsize=3)
    toolbox.register("evaluate", evaluateInd)

    #run algorithms
    pool = multiprocessing.Pool()
    toolbox.register("map", pool.map)
    algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=0.2, ngen=3)

    #print pop
    print pop
