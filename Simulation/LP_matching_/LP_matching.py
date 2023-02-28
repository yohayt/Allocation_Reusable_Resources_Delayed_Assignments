import json
import os
import random
import sys

sys.dont_write_bytecode = True

from Base.Assignment import Assignment

sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'Algorithm'))
from Algorithm.AlgorithmHelper import AlgorithmHelper

from prepare_data import create_C_lambda_probs, create_C_lambda, create_pjt

MAX_DELTA_WORKER_TPRIME = 0.0


class LP_matching_oleg:

    @staticmethod
    def init_data(f, T, C, workersIdsList, tripDataDirectoryIndex='1'):
        print("pjt T:", T, "C:", C, file=f)
        pjt = create_pjt(f, T, C)
        print("done pjt", file=f)

        LHS = len(workersIdsList)
        RHS = len(pjt[0])
        C_lambda_dict = create_C_lambda(workersIdsList)
        C_lambda_probs = create_C_lambda_probs(LHS, RHS, T, C, C_lambda_dict)
        print("done C_lambda", file=f)
        print("workers list sorted:", workersIdsList, file=f)

        return LHS, RHS, pjt, C_lambda_probs

    @staticmethod
    def solveLP(path, T, C, LHS, RHS, pjt):
        # read results of the solver

        Xopt = {}
        Xopt_norm = {}

        with open(path, 'r') as fp:

            X_results = fp.readlines()
            for line in X_results:
                if 't' in line:
                    continue
                vals = line.split(',')
                t = int(vals[0])
                t_prime = int(vals[1])
                i = int(vals[2])
                j = int(vals[3])
                val = float(vals[4].rstrip('\n'))
                Xopt[(t, t_prime, i, j)] = val
                Xopt_norm[(t, t_prime, i, j)] = val
        # Normalize probabilities by arrival probability pjt
        for t in range(T):
            for j in range(RHS):
                norm_factor = 0.0
                for t_prime in range(t, t + C):
                    if t_prime >= T:
                        break
                    for i in range(LHS):
                        if Xopt_norm[(t, t_prime, i, j)] <= 0.000000001:
                            continue
                        Xopt_norm[(t, t_prime, i, j)] /= pjt[t][j]
                        # Xopt_norm[(t, t_prime, i, j)] /= 1 - norm_factor
                        assert Xopt_norm[(t, t_prime, i,
                                          j)] < 1.001, f"prob is more than 1.001, got: {Xopt_norm[(t, t_prime, i, j)]}, for {t},{t_prime},{i},{j}, norm factor: {norm_factor}"
                        norm_factor = Xopt_norm[(t, t_prime, i, j)]

        return Xopt, Xopt_norm

    @staticmethod
    def new_sampleAssignment(firstTime, f, Xopt_norm, t_tag, i, assignmentsList, CompetitiveRatio, qit, pt,
                             jobsManager):
        items = [Xopt_norm[t, t_tag, i, jobsManager.getJobTypeIndexByJobId(jobsManager.getJobByIndex(j).getId())] for
                 t, j in
                 assignmentsList]

        sum_prob_wait = sum(items)  # + (CompetitiveRatio/qit)

        if sum_prob_wait <= 0.000000001:
            return None

        # if firstTime and all(x < MAX_DELTA_WORKER_TPRIME for x in
        #                      items):  # if all the probs are deltas from huristic, then skip and sample on second time when only if no other worker was chosen
        #     print("worker", i, ", jobs waiting:", assignmentsList, "probs:", items, "wasn't sampled", file=f)
        #     return None
        print("time", t_tag, "worker", i, ",waiting jobs:", assignmentsList, "probs:", items, file=f)

        r = random.uniform(0, 1)
        cur_sum = 0
        for t, jobIndex in assignmentsList:

            waitingJobTypeIndex = jobsManager.getJobTypeIndexByJobId(jobsManager.getJobByIndex(jobIndex).getId())
            # temp_u = (Xopt[(t, t_tag, i, waitingJobTypeIndex)] * CompetitiveRatio) / (qit * pt[waitingJobTypeIndex])
            # temp_u = (Xopt[(t, t_tag, i, waitingJobTypeIndex)]) / (pt[waitingJobTypeIndex])
            temp_u = (Xopt_norm[(t, t_tag, i, waitingJobTypeIndex)]) / sum_prob_wait

            cur_sum += temp_u
            if r <= cur_sum:
                return (t, t_tag, i, jobIndex)

        return None

    @staticmethod
    def sortWorkers(f, assignmentsList, currentTime, waitingJobs, LHS, Xopt, jobsManager):
        # sort workers by the biggest Xopt - allocation probability, return ordered workers indices
        all_probs = {}
        for i in range(LHS):
            for t, j_index in assignmentsList:
                j = jobsManager.getJobTypeIndexByJobId(jobsManager.getJobByIndex(j_index).getId())
                all_probs[(i, t, j)] = Xopt[t, currentTime, i, j]
        sorted_dict = {k: v for k, v in sorted(all_probs.items(), key=lambda item: item[1], reverse=True)}
        sorted_workers = []
        for k in sorted_dict.keys():
            i = k[0]
            if not i in sorted_workers:
                sorted_workers.append(i)
        print("time", currentTime, "all probs sorted:", sorted_dict, file=f)
        print("order workers:", sorted_workers, file=f)
        return sorted_workers

    @staticmethod
    def sortWorkersAndSample(firstTime, f, predicted_times, predicted_qualities, assignmentsList,
                             alreadyAssignedJobsIndices, currentTime,
                             waitingJobs, LHS, RHS, pjt, T, C_lambda_probs, competitiveRatio, C, Xopt, Xopt_norm,
                             jobsManager, workersManager, isWT, type_val_func, scheduler,
                             statistics):
        workers_new_indices = LP_matching_oleg.sortWorkers(f, assignmentsList, currentTime, waitingJobs, LHS, Xopt_norm,
                                                           jobsManager)

        # iterate all workers in new order and sample by SP algorithm
        for i in workers_new_indices:

            workerI = workersManager.getWorkerByIndex(i)

            assignment = LP_matching_oleg.new_SP(firstTime, f, jobsManager, workerI, i, currentTime, assignmentsList,
                                                 competitiveRatio, C, Xopt, Xopt_norm, C_lambda_probs, RHS, pjt,
                                                 scheduler,
                                                 statistics)
            # if not sampled, try next one in order
            if not assignment:
                continue

            # creat the assignment sampled and update all relevant
            t_ass, t_tag_ass, workerIndex, jobIndex = assignment
            print("time", currentTime, ",waiting jobs:", assignmentsList, file=f)
            assignmentsList.remove((t_ass, jobIndex))
            selectedWorker = workersManager.getWorkerByIndex(workerIndex)
            selectedJob = jobsManager.getJobByIndex(jobIndex)

            jobTime = AlgorithmHelper.predictTime(selectedJob, selectedWorker, predicted_times,
                                                  getSimulatedTime=True)

            # create assignment
            newAssignment = Assignment(selectedJob.getId(), selectedWorker.getId(),
                                       selectedJob.getArrivalTime(),
                                       jobTime + 1, currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS,
                                       currentTime + 1)

            scheduler.addAssignment(newAssignment)

            selectedJob.setIsAssigned(selectedWorker.getId(), currentTime + 1)
            selectedWorker.setAssignedJobId(selectedJob.getId())

            selectedWorker.setJobAssignedTime(currentTime + 1)
            selectedWorker.setBusyStartTime(currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            scheduler.updateJobFetchTime(selectedJob.getId(), selectedJob, currentTime + 1)

            scheduler.updateJobStartTime(selectedJob.getId(), selectedJob,
                                         currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            free_workers = AlgorithmHelper.getFreeWorkers(workersManager.getWorkersList(), currentTime)

            inputStatsArr = {}
            inputStatsArr['job'] = selectedJob
            inputStatsArr['worker'] = selectedWorker
            inputStatsArr['scheduler'] = scheduler
            inputStatsArr['type_val_func'] = type_val_func
            inputStatsArr['isWT'] = isWT
            inputStatsArr['currentTime'] = currentTime
            inputStatsArr['free_workers'] = free_workers
            inputStatsArr['predicted_times'] = predicted_times
            inputStatsArr['predicted_qualities'] = predicted_qualities

            statsArr = statistics.combineAlgorithmDebugStatsArr(inputStatsArr, C, f, 'a+')

            statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

            statsArr = {}
            statsArr['currentTick'] = currentTime
            statsArr['jobId'] = selectedJob.getId()
            statsArr['workerId'] = selectedWorker.getId()
            statsArr['jobArrivalTime'] = selectedJob.getArrivalTime()
            statsArr['jobTime'] = jobTime
            statsArr['freeWorkersCount'] = len(free_workers)

            statistics.writeAssignmentLog(statsArr, False, 'a+')
        print("print return values from iterateWorkersSample:", assignmentsList, alreadyAssignedJobsIndices, file=f)
        return assignmentsList, alreadyAssignedJobsIndices

    @staticmethod
    # Another option to iterate the workers without sorting
    def iterateWorkersAndSample(firstTime, f, predicted_times, predicted_qualities, assignmentsList,
                                alreadyAssignedJobsIndices, currentTime,
                                waitingJobs, LHS, RHS, pjt, T, C_lambda_probs, competitiveRatio, C, Xopt, Xopt_norm,
                                jobsManager, workersManager, isWT, type_val_func, scheduler,
                                statistics):

        if not firstTime:
            print("second round", file=f)
        for i in range(LHS):

            workerI = workersManager.getWorkerByIndex(i)

            assignment = LP_matching_oleg.new_SP(firstTime, f, jobsManager, workerI, i, currentTime, assignmentsList,
                                                 competitiveRatio, C, Xopt, Xopt_norm, C_lambda_probs, RHS, pjt,
                                                 scheduler,
                                                 statistics)

            if not assignment:
                continue

            t_ass, t_tag_ass, workerIndex, jobIndex = assignment
            print("time", currentTime, ",waiting jobs:", assignmentsList, file=f)
            assignmentsList.remove((t_ass, jobIndex))
            selectedWorker = workersManager.getWorkerByIndex(workerIndex)
            selectedJob = jobsManager.getJobByIndex(jobIndex)

            jobTime = AlgorithmHelper.predictTime(selectedJob, selectedWorker, predicted_times,
                                                  getSimulatedTime=True)

            # create assignment
            newAssignment = Assignment(selectedJob.getId(), selectedWorker.getId(),
                                       selectedJob.getArrivalTime(),
                                       jobTime + 1, currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS,
                                       currentTime + 1)

            scheduler.addAssignment(newAssignment)

            selectedJob.setIsAssigned(selectedWorker.getId(), currentTime + 1)
            selectedWorker.setAssignedJobId(selectedJob.getId())

            selectedWorker.setJobAssignedTime(currentTime + 1)
            selectedWorker.setBusyStartTime(currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            scheduler.updateJobFetchTime(selectedJob.getId(), selectedJob, currentTime + 1)

            scheduler.updateJobStartTime(selectedJob.getId(), selectedJob,
                                         currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            free_workers = AlgorithmHelper.getFreeWorkers(workersManager.getWorkersList(), currentTime)

            inputStatsArr = {}
            inputStatsArr['job'] = selectedJob
            inputStatsArr['worker'] = selectedWorker
            inputStatsArr['scheduler'] = scheduler
            inputStatsArr['type_val_func'] = type_val_func
            inputStatsArr['isWT'] = isWT
            inputStatsArr['currentTime'] = currentTime
            inputStatsArr['free_workers'] = free_workers
            inputStatsArr['predicted_times'] = predicted_times
            inputStatsArr['predicted_qualities'] = predicted_qualities

            statsArr = statistics.combineAlgorithmDebugStatsArr(inputStatsArr, C, f, 'a+')

            statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

            statsArr = {}
            statsArr['currentTick'] = currentTime
            statsArr['jobId'] = selectedJob.getId()
            statsArr['workerId'] = selectedWorker.getId()
            statsArr['jobArrivalTime'] = selectedJob.getArrivalTime()
            statsArr['jobTime'] = jobTime
            statsArr['freeWorkersCount'] = len(free_workers)

            statistics.writeAssignmentLog(statsArr, False, 'a+')
        print("print return values from iterateWorkersSample:", assignmentsList, alreadyAssignedJobsIndices, file=f)
        return assignmentsList, alreadyAssignedJobsIndices

    @staticmethod
    def new_ATT(f, predicted_times, predicted_qualities, assignmentsList, alreadyAssignedJobsIndices, currentTime,
                waitingJobs,
                LHS, RHS, pjt, T,
                C_lambda_probs, competitiveRatio, C, Xopt, Xopt_norm, jobsManager, workersManager, isWT, type_val_func,
                scheduler,
                statistics):

        # remove all jobs not waiting from assignmentsList
        to_remove = []
        for t, waitingJobIndex in assignmentsList:
            if not jobsManager.getJobByIndex(waitingJobIndex):
                to_remove.append((t, waitingJobIndex))

        for item in to_remove:
            assignmentsList.remove(item)

        # iterate all waiting jobs in Queue
        for jobId, job in waitingJobs.items():

            waitingJobIndex = jobsManager.getJobIndexByJobId(jobId)
            if (waitingJobIndex in alreadyAssignedJobsIndices):
                # if already assigned, continue
                continue

            assignmentsList.append((currentTime, waitingJobIndex))
            alreadyAssignedJobsIndices[waitingJobIndex] = True

        firstTime = True

        assignmentsList, alreadyAssignedJobsIndices = LP_matching_oleg.sortWorkersAndSample(firstTime, f,
                                                                                            predicted_times,
                                                                                            predicted_qualities,
                                                                                            assignmentsList,
                                                                                            alreadyAssignedJobsIndices,
                                                                                            currentTime, waitingJobs,
                                                                                            LHS, RHS, pjt, T,
                                                                                            C_lambda_probs,
                                                                                            competitiveRatio, C, Xopt,
                                                                                            Xopt_norm, jobsManager,
                                                                                            workersManager, isWT,
                                                                                            type_val_func, scheduler,
                                                                                            statistics)

        return assignmentsList, alreadyAssignedJobsIndices

    @staticmethod
    def calc_qit(cur_i, t, competitiveRatio, C, Xopt, C_lambda, RHS):
        cur_sum = 0
        for t_arr in range(t):
            for t_tag in range(t_arr, t_arr + C):
                if t_tag >= t:
                    continue
                for j in range(RHS):
                    usage_time = t - t_tag
                    cur_sum += competitiveRatio * Xopt[(t_arr, t_tag, cur_i, j)] * C_lambda[cur_i][j][usage_time]
        return 1 - cur_sum

    @staticmethod
    # cur_i - worker
    def new_SP(firstTime, f, jobsManager, worker, cur_i, t, assignmentsList, competitiveRatio, C, Xopt, Xopt_norm,
               C_lambda, RHS, pjt,
               scheduler, statistics):
        # if there is no assignments in queue
        if len(assignmentsList) == 0:
            return None


        isCurrentJobIsGoingToEndToday = False
        # check if unavailable worker is about to end the job
        if (not worker.isAvailable(t)):
            workerCurrentJobId = worker.getAssignedJobId()
            assignment = scheduler.getAssignmentByJobId(workerCurrentJobId)

            isCurrentJobIsGoingToEndToday = assignment.getRemaindTime() <= 1

        # extract is i busy
        if not worker.isAvailable(t) and not isCurrentJobIsGoingToEndToday:
            return None

        # calc probability qit - probability that worker i is avalable in time t
        qit = LP_matching_oleg.calc_qit(cur_i, t, competitiveRatio, C, Xopt, C_lambda, RHS)
        # sample an assignment for worker i
        assignment = LP_matching_oleg.new_sampleAssignment(firstTime, f, Xopt_norm, t, cur_i, assignmentsList,
                                                           competitiveRatio, qit,
                                                           pjt[t], jobsManager)
        return assignment

    @staticmethod
    def runAlgorithm(f, predicted_times, predicted_qualities, assignmentsList, alreadyAssignedJobsIndices, currentTime,
                     firstWaitingJob, LHS,
                     RHS, pjt, T, C_lambda_probs,
                     competitiveRatio, C, Xopt, Xopt_norm, jobsManager, workersManager, isWT, type_val_func, scheduler,
                     statistics):

        return LP_matching_oleg.new_ATT(f, predicted_times, predicted_qualities, assignmentsList,
                                        alreadyAssignedJobsIndices,
                                        currentTime,
                                        firstWaitingJob, LHS, RHS, pjt, T,
                                        C_lambda_probs, competitiveRatio, C, Xopt, Xopt_norm, jobsManager,
                                        workersManager, isWT,
                                        type_val_func, scheduler,
                                        statistics)
