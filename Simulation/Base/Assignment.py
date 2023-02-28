

class Assignment:

    def __init__(self, lJobId, lWorkerId, lArrivalTime, lDuration, lActualStartTick, lAssignmentTick = -1):
        self.mJobId = lJobId
        self.mWorkerId = lWorkerId

        self.mArrivalTime = lArrivalTime
        # self.mDuration = lDuration

        self.mActiveTime = 0

        self.mActualStartTick = lActualStartTick
        self.mRemainedTime = lDuration

        self.mFetchTime = -1
        self.mStartTime = -1

        self.mAssignmentTick = lAssignmentTick

        self.mId = str(self.mJobId) + '_' + str(self.mWorkerId) + '_' + str(self.mArrivalTime) + '_' + str(
            lDuration)

    def getId(self):
        return self.mId

    def getActiveTime(self):
        return self.mActiveTime

    def getAssignmentTime(self):
        return self.mAssignmentTick

    def getArrivalTime(self):
        return self.mArrivalTime

    def setActiveTime(self, activeTime):
        self.mActiveTime = activeTime

    def isJobCompleted(self):
        if (self.mRemainedTime <= 0):
            return True

        return False

        # return self.mEndTime >= currentTime
    # def getDuration(self):
    #     return self.mDuration

    def onTick(self, currentTime):

        if (self.mActualStartTick <= currentTime):
            self.mRemainedTime -= 1

            self.mActiveTime += 1

    def getJobId(self):
        return self.mJobId

    def getWorkerId(self):
        return self.mWorkerId

    def getRemaindTime(self):
        return self.mRemainedTime

    def setRemainedTime(self, remainedTime):
        self.mRemainedTime = remainedTime
    

    def getFetchTime(self):
        return self.mFetchTime

    def setFetchTime(self, fetchTime):
        self.mFetchTime = fetchTime
    

    def getStartTime(self):
        return self.mStartTime

    def setStartTime(self, startTime):
        self.mStartTime = startTime
    
    def dot(self, dot):
        jid = str(self.mJobId)
        wid = 'W ' + str(self.mWorkerId)

        dot.edge(wid, jid, color='red', constraint='False')

    def __str__(self):

        output = ''
        
        output += str(self.mJobId) + ' : '
        output += str(self.mWorkerId) + ' : '
        output += str(self.mArrivalTime) + ' : '
        output += str(self.mActiveTime) + ' : '
        output += str(self.mRemainedTime) + ' : '

        return output

    # def debug_assignments(j, w, scheduler, alphas, epsilon_precent, type_val_func, df_ass, isWT, currentTime, free_workers,
    #                   sampled_times, predicted_times, predicted_qualities, waiting_match, statistics):

    #     statsArr = {}
    #     # assignment time:
    #     statsArr['job ID'] = j.getId()
    #     statsArr['worker ID'] = w.getId()
    #     statsArr['value func type'] = str(type_val_func)
    #     statsArr['time predict'] = predicted_times[(w.getId(), j.getId())]
    #     statsArr['norm (alpha0 * time prediction)'] = norm_time(predicted_times[(w.getId(), j.getId())]) * alphas[0]
    #     statsArr['quality predict'] = predicted_qualities[(w.getId(), j.getId())]
    #     statsArr['norm (alpha1 * quality prediction)'] = norm_quality(predicted_qualities[(w.getId(), j.getId())]) * alphas[
    #         1]
    #     statsArr['urgency'] = j.getUrgency()
    #     statsArr['norm (alpha2 * urgency)'] = norm_urgancy(j.getUrgency()) * alphas[2]
    #     statsArr['alpha4 * exponent waiting time'] = pow((1 + delta), j.getWaitingTime()) * alphas[4]
    #     statsArr['norm(alpha5 * waiting time)'] = norm_waiting_time(j.getWaitingTime()) * alphas[5]
    #     statsArr['waiting time'] = j.getWaitingTime()
    #     statsArr['predict value func'] = value_func(j, w, scheduler, alphas, epsilon_precent, type_val_func, isWT,
    #                                                 currentTime, predicted_times, predicted_qualities, waiting_match)
    #     statsArr['arrival time'] = j.getArrivalTime()
    #     statsArr['assignment time'] = currentTime
    #     statsArr['available workers'] = free_workers

    #     # completion time:
    #     statsArr['real time'] = calc_real_time(j, w, scheduler)
    #     statsArr['norm (alpha0 * real time)'] = norm_time(calc_real_time(j, w, scheduler)) * alphas[0]
    #     statsArr['real quality'] = calc_real_quality(j, w, scheduler)
    #     statsArr['norm (alpha1 * quality)'] = norm_quality(calc_real_quality(j, w, scheduler)) * alphas[1]
    #     statsArr['eval func'] = eval_func(j, w, alphas, type_val_func, scheduler)
    #     statsArr['total time'] = j.getWaitingTime() + calc_real_time(j, w, scheduler)


    #     statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

    #     df_ass = df_ass.append(statsArr, ignore_index=True)

    #     return df_ass