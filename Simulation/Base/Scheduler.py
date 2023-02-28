# from graphviz import Digraph


class Scheduler:

    def __init__(self, isSimulationMode, statistics, file):

        self.mAssignments = {}
        self.mCompletedAssignments = {}
        self.mNumAsiignments = 0

        self.statistics = statistics

        self.isSimulationMode = isSimulationMode

    def addAssignment(self, assignment):

        if (not self.isSimulationMode):
            # Set max time for assignments in real mode
            assignment.setRemainedTime(5 * 3600)

        self.mAssignments[assignment.getId()] = assignment
        self.mNumAsiignments += 1

    def getAssignmentByJobId(self, jobId):

        for assignmentId, assignment in self.mAssignments.items():

            if (assignment.getJobId() == jobId):
                return assignment

        return None

    def getCompletedAssignmentByJobId(self, jobId):

        for assignmentId, assignment in self.mCompletedAssignments.items():

            if (assignment.getJobId() == jobId):
                return assignment

        return None

    def updateJobCompletionStatus(self, jobId, isCompleted, currentTick):

        assignment = self.getAssignmentByJobId(jobId)

        if (None != assignment and isCompleted):

            self.statistics.logJobCompletionByOperator(assignment.getJobId(), assignment.getWorkerId(),
                                                       assignment.getArrivalTime(), assignment.getAssignmentTime(),
                                                       assignment.getFetchTime(), assignment.getStartTime(),
                                                       assignment.getActiveTime(), assignment.getRemaindTime(),
                                                       currentTick, currentTick + 1)

            assignment.setRemainedTime(0)

        else:
            # print('job ' + jobId + ' not assigned')
            pass

    def updateJobFetchTime(self, jobId, job, currentTick):

        assignment = self.getAssignmentByJobId(jobId)

        if (None != assignment):

            assignment.setFetchTime(currentTick)

        else:
            print('job ' + jobId + ' not assigned', file=self.f)
            pass

        if (None != job):

            job.setFetchedTime(currentTick)

        else:
            print('job not found', self.f)
            pass

    def updateJobStartTime(self, jobId, job, currentTick):

        assignment = self.getAssignmentByJobId(jobId)

        if (None != assignment):

            assignment.setStartTime(currentTick)

        else:
            print('job ' + jobId + ' not assigned', file=self.f)
            pass

        if (None != job):

            job.setWorkStartedTime(currentTick)

        else:
            print('job not found', file=self.f)
            pass

    def getNumAssignments(self):
        return self.mNumAsiignments

    def getCompletedAssignments(self):
        return self.mCompletedAssignments

    def getAssignments(self):
        return self.mAssignments

    def onTick(self, currentTime, jobsManager, workersManager):

        completedAssignmentsKeys = []

        for assignmentId, assignment in self.mAssignments.items():

            assignment.onTick(currentTime)

            if (assignment.isJobCompleted()):
                # print(assignment.getJobId())

                job = jobsManager.getJobById(assignment.getJobId())

                assignment.setActiveTime(job.getActiveTime())

                completedAssignmentsKeys.append((assignmentId, assignment))

                # Not used:
                # if (job.getRepetative()):
                # else:

                job.setCompleted(currentTime)

                jobsManager.removeJob(assignment.getJobId())

                workerFreeAgain = -1
                worker = workersManager.getWorkerById(assignment.getWorkerId())
                if worker.getAssignedJobId() == job.getId():
                    worker.setNotBusy(currentTime)
                    workerFreeAgain = currentTime + 1

                self.statistics.logJobCompletionByOperator(assignment.getJobId(), assignment.getWorkerId(),
                                                           assignment.getArrivalTime(), assignment.getAssignmentTime(),
                                                           assignment.getFetchTime(), assignment.getStartTime(),
                                                           assignment.getActiveTime(), assignment.getRemaindTime(),
                                                           currentTime, workerFreeAgain)

        for assignmentId, assignment in completedAssignmentsKeys:
            del self.mAssignments[assignmentId]
            self.mCompletedAssignments[assignmentId] = assignment

        workers_active = []
        for assignmentId, assignment in self.mAssignments.items():
            workerId = assignmentId.split('_')[3]
            if workerId in workers_active:
                print("ERROR")
            workers_active.append(workerId)

    def __str__2(self):

        output = ''

        output += 'Active assignments (' + str(len(self.mAssignments)) + ') list:' + '\n'
        output += '--------' + '\n'

        for assignmentId, assignment in self.mAssignments.items():
            output += str(assignment) + '\n'

        output += '--------' + '\n'

        return output

    def __str__(self):

        output = ''
        for assignmentId, assignment in self.mAssignments.items():
            output += str(assignment) + '\n'

        return output

    def dot(self, dot):
        for assignmentId, assignment in self.mAssignments.items():
            assignment.dot(dot)
