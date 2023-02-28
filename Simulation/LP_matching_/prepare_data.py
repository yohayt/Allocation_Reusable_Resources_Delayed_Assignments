import collections
import os
import random
import numpy as np
import pandas as pd
from scipy.stats import norm
import json

COL_SUFFIX_TIME = "_time_participant"
COL_SUFFIX_QUALITY = "_quality"
COL_PARTICIPANT_ID = 'participantId'
scenarios_dict = {"scenario0": "park_scenario_a", "scenario1": "turn_left_scenario_a",
                  "scenario2": "slow_vehicle_scenario_a", "scenario3": "foreign_object_scenario_a",
                  "scenario4": "extreme_weather_scenario_a"}

MIN_TIME = 7 / 5
MAX_TIME = 110 / 5
alpha = [0.1, 0.9, 0.4, 0, 0.2, 0.3]
TICKS_IN_HOUR = 720
MIN_QUALITY = 0
MAX_QUALITY = 100


def create_pjt(f,T, C):
    return create_pjt_5_hours_4states_larger(f,T, C)


    
def create_pjt_5_hours_4states_larger(f,T=TICKS_IN_HOUR * 5, C=30):
    pjt_stats = pd.read_csv('LP_matching_/pjt_stats_4_states_larger_5Hours_noScenario0.csv')
    pjt = {}
    
    for t in range(T-C+1):
        hour = (t // 90) + 15
        print("t",t, "hour",hour, file=f)
        arrival_prob = pjt_stats[pjt_stats['hour'] == hour]['probability']._values[0]
        if t % 100 == 0:
            print("t = ",t,",", "probabiluty of arrival:", arrival_prob)
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            scenario_precent = pjt_stats[pjt_stats['hour'] == hour][scenario]._values[0]
            pjt[t][j] = arrival_prob * scenario_precent  # uniform distribution of 5 types and none option
    for t in range(T-C+1, T + 1):
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            pjt[t][j] = 0.0000001
    return pjt



def normalizeTime(x, mean=None, std=None):
    if x == 0:
        return 0

    x = np.log(x)

    return (x - np.log(MIN_TIME)) / (np.log(MAX_TIME) - np.log(MIN_TIME))


def create_C_lambda(workers):
    return create_C_lambda_real_times_reg(workers)


def create_C_lambda_real_times_reg(workers):
    C_lambda = dict()
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        C_lambda[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID.split('_')[0]]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            #if j == 0:
            #    C_lambda[i][j] = str(0) + ';' + str(0)
            #    continue
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            mu = mu
            std = 1
            C_lambda[i][j] = str(mu) + ';' + str(std)
    print(C_lambda)
    return C_lambda



def create_C_lambda_probs(LHS, RHS, T, C, C_lambda_dict):
    return create_C_lambda_probs_reg(LHS, RHS, T, C, C_lambda_dict)


def create_C_lambda_probs_reg(LHS, RHS, T, C, C_lambda_dict):
    cur_T = T - C + 1
    cur_C = C - 1
    C_lambda_probs = {}
    arr = np.asarray(range(cur_T + cur_C))
    for i in range(LHS):
        C_lambda_probs[i] = {}
        for j in range(RHS):
            C_lambda_probs[i][j] = {}
            parse = C_lambda_dict[i][j].split(";")
            mu = float(parse[0])
            sig = float(parse[1])
            cdf_all = norm.cdf(arr, loc=mu, scale=sig)  # temp code, should be replaced with something more interesting
            for item1, item2 in zip(arr, cdf_all):
                C_lambda_probs[i][j][item1] = 1 - item2
    return C_lambda_probs



if __name__ == '__main__':
    T = TICKS_IN_HOUR * 24
    C = 5
    pjt = create_pjt(T, C)

    # with open('pjt_dict_3_states.json','w') as f:
    #     json.dump(pjt, f)

    workers_ID = get_workers()
    W = create_W(workers_ID)
    C_lambda_dict = create_C_lambda(workers_ID)
    C_lambda_probs = create_C_lambda_probs(len(workers_ID), len(scenarios_dict), T, C, C_lambda_dict)
