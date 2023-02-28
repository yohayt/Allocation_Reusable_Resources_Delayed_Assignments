#!/usr/bin/env python
# coding: utf-8

import sys
import csv
import random
from datetime import datetime
#import numpy as np
import pandas as pd
#import scipy.stats as stats
import time
import os
import signal

sys.dont_write_bytecode = True
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'Base'))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'SyntethicSimulator'))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'LP_matching_'))

# from xml.etree import ElementTree

from Base.Statistics import Statistics
from BaseManager import BaseManager
# from Settings import Settings
from WorkersManager import WorkersManager
from JobsManager import JobsManager
from Scheduler import Scheduler
from online_matching import scenarios_dict, COL_PARTICIPANT_ID, COL_SUFFIX_TIME
# from JobsGenerator import JobsGenerator
from SyntethicSimulator import SyntethicSimulator

# from MissionsWebServer import MissionsWebServer

from LP_matching_ import LP_matching


def run_scheduler(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath,
                  workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,
                  isSimulationMode, secondsToSleepAfterEachTick, solver_path,f, verboseMode=False):

    print("PARAMETERS: " "scid {} algo {} is_train {} isWt {} JDESCR PATH {} wdescpath {} timequal path {} stats {} compet {} C {} T {} is sim {} sleep after tick {} spath {}, verbose {}".format(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath, workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,isSimulationMode, secondsToSleepAfterEachTick, solver_path, verboseMode),file=f)
    sampled_times = None
    predicted_times = None
    epsilon = 0.15
    alpha = [0.1, 0.9, 0.0, 0, 0.2, 0.6]
    results, run_times, mean_tt_dict, max_tt_dict, num_hungarian_dict, simul_length_dict, h_avg_time_dict, h_tt_dict = {}, {}, {}, {}, {}, {}, {}, {}
    # if isWT and algorightmToUse != "algo1_general":
    #     continue

    if (verboseMode):
        print('Algorithm: ' + algorightmToUse,file=f)

    jobsManager = JobsManager(f)
    workersManager = WorkersManager()

    jobsManager.addJobsFromCSV(jobsDescriptionTableFilePath)
    workersManager.addWorkersFromCSV(workersDescriptionTableFilePath)

    #random.seed(5 * workersManager.getWorkersListLength())

    if (verboseMode):
        print("workers: ", workersManager.getWorkersList().keys(),file=f)

    if not sampled_times:
        predicted_qualities = {}
        predicted_times = {}  # from training

        if (not isTrainingMode):

            exp_data = pd.read_csv(timeQualityPath)
            for index, row in exp_data.iterrows():
                job_id = row['job_id']
                worker_id = row['worker_id']

                simulatedTime = -1

                if ('simulated_time' in row):
                    simulatedTime = int(row['simulated_time'])

                predicted_times[(worker_id, job_id)] = (int(row['expected_time']), simulatedTime)
                predicted_qualities[(worker_id, job_id)] = int(row['expected_quality'])

        else:
            for workerId, worker in workersManager.getWorkersList().items():
                for jobId, job in jobsManager.getJobsList().items():
                    predicted_times[(workerId, jobId)] = 0
                    predicted_qualities[(workerId, jobId)] = 5

    scheduler = Scheduler(isSimulationMode, statistics, f)

    if (not isSimulationMode):
        missionsWebServer = MissionsWebServer(secondsToSleepAfterEachTick, workersManager, jobsManager, scheduler,
                                              statistics)
    else:
        missionsWebServer = None

    singleOperatorDebugMode = isTrainingMode

    syntethicSimulator = SyntethicSimulator(missionsWebServer, jobsManager, workersManager, scheduler, isSimulationMode,
                                            singleOperatorDebugMode)

    start_time = time.time()
    alloc_val, mean_tt, max_tt, num_of_hungarian, simul_length, h_times = syntethicSimulator.run(alpha, epsilon,
                                                                                                 'reg',
                                                                                                 algorightmToUse,
                                                                                                 isWT,
                                                                                                 sampled_times,
                                                                                                 predicted_times,
                                                                                                 predicted_qualities,
                                                                                                 workersManager.getWorkersListLength(),
                                                                                                 '',
                                                                                                 jobsManager.getJobsListLength(),
                                                                                                 statistics,
                                                                                                 competitiveRatio,
                                                                                                 C, T,
                                                                                                 solver_path, 
                                                                                                 f,
                                                                                                 secondsToSleepAfterEachTick,
                                                                                                                verboseMode)
    
    print('alloc_val: ' + str(alloc_val),file=f)
    statsArr = {}
    statsArr['end_time'] = time.time()
    statsArr['h_sum_times'] = sum(h_times)
    statsArr['h_avg_time'] = statsArr['h_sum_times'] / len(h_times) if len(h_times) != 0 else 0
    statsArr['algo_name'] = algorightmToUse
    statsArr['algo_name'] += '' if isWT else '_noWT'
    statsArr['results.keys'] = str(list(results.keys())).replace(',', ';')
    statsArr['results'] = alloc_val
    statsArr['mean_tt_dict'] = mean_tt
    statsArr['max_tt_dict'] = max_tt
    statsArr['num_hungarian_dict'] = num_of_hungarian
    statsArr['simul_length_dict'] = simul_length
    statsArr['run_times'] = statsArr['end_time'] - start_time
    statsArr['h_avg_time_dict'] = statsArr['h_avg_time']
    statsArr['h_tt_dict'] = statsArr['h_sum_times']

    # cumulativeMeanRowStr = '\n'
    # cumulativeMeanRowStr += 'Cumulative waiting times mean,'
    # cumulativeMeanRowStr += str(np.amin(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.amax(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.mean(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.std(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.var(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(len(questionWaitingTimesCumulativeMean)) + ','

    statistics.writeExperimentSummaryStats(statsArr, 'w+')

    return statsArr

    # dir_path = 'results/{}_workers/'.format(num_workers)
    # if not os.path.exists(dir_path):
    #     os.makedirs(dir_path)
    # with open(dir_path + 'scenario_{}.csv'.format(xml_path), 'w') as ofile:
    #     writer = csv.writer(ofile)
    #     writer.writerow([""] + list(results.keys()))
    #     writer.writerow(["eval func"] + list(results.values()))
    #     writer.writerow(["max total time"] + list(max_tt_dict.values()))
    #     writer.writerow(["mean total time"] + list(mean_tt_dict.values()))
    #     writer.writerow(["run time"] + list(run_times.values()))
    #     writer.writerow(["number of Hungarian calls"] + list(num_hungarian_dict.values()))
    #     writer.writerow(["length of simulation"] + list(simul_length_dict.values()))
    #     writer.writerow(["avg hungarian time"] + list(h_avg_time_dict.values()))
    #     writer.writerow(["total hungarian time"] + list(h_tt_dict.values()))

def run(args):

        argv = args
        
        scenarioId = argv[0]
        algorightmToUse = argv[1]
        isTrainingMode = 'true' == argv[2]
        jobsDescriptionTableFilePath = argv[3]
        workersDescriptionTableFilePath = argv[4]
        timeQualityPath = argv[5]
        run_index = argv[8]
        solver_path = argv[10]
        Delta ='delta' if 'delta' in solver_path else ''
        isWT = 'WT' == argv[9]
        simul_index = scenarioId.split('_')[1].split('.')[0]

        with open(scenarioId+"_"+algorightmToUse+"_"+run_index +"_"+Delta+ ".output", "w") as f:
            print(argv,file=f)



            if (len(argv) > 6):
                C = int(argv[6])
            else:
                C = 6

            if (len(argv) > 7):
                T = int(argv[7])
            else:
                T = 720 * 24 + 5

            competitiveRatio = 0.5


            # Real time: 1 tick == 1 second
            secondsToSleepAfterEachTick = 0.00001

            # Whether the jobs completed by the simulator (true) or by MissionsWebServer (false)
            isSimulationMode = True

            verboseMode = True

            trainingModeDirNameStr = 'real_mode'

            if (isTrainingMode):
                trainingModeDirNameStr = 'training_mode'
            
            simul_index_TQ = timeQualityPath.split('_')[-1].split('.')[0]
            if simul_index == simul_index_TQ:
                print("PROBLEM! simul index number in ime quality is not correct", file=f)
            WT = "WT" if isWT else "_noWT"
            TQ ='sameMuExp' if 'sameMuExp' in timeQualityPath else 'diffMuExp'
            
            scenario_name = jobsDescriptionTableFilePath.split('/')[-1].split('.')[0]
            num_workers = workersDescriptionTableFilePath.split('/')[-1].split('_')[0]
            executionUniqueIndex = scenario_name+'/simulation '+simul_index+'/'+algorightmToUse + '_' + num_workers + 'w_' + str(C - 1) + 'queue_run_' + str(run_index) + '_' + Delta + '_' + WT + '_'+TQ +'_'+ datetime.utcnow().strftime('%d-%m-%y.%H-%M-%S.%f')[:-3]

            statistics = Statistics(executionUniqueIndex)

            statistics.writeWorkersOfflineStatuses({}, True, 'w+')
            statistics.writePauseEventsRecords({}, True, 'w+')
            statistics.writeAlgorithmDebugRow({}, True, 'w+')
            statistics.writeAssignmentLog({}, True, 'w+')
            statistics.writeWorkerOnlineStatusTitle('w+')
            statistics.writeWaitingJobsIdsTitle('w+')
            statistics.writeRejectedJobsTitle('w+')
            statistics.writeSystemPauseStatusTitle('w+')
            statistics.writeJobCompletionByOperatorFileTitle('w+')
           
            statsArr = run_scheduler(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath,
                                     workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,
                                     isSimulationMode, secondsToSleepAfterEachTick, solver_path, f, verboseMode)

            print(statsArr, file=f)

            statistics.writeTimeQualityStats(statsArr, 'w+')


        return 0
