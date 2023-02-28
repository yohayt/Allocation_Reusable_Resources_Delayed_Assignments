import os
import pandas as pd
from functools import reduce
import statistics
import numpy as np
import sys

dir_path = sys.argv[1]
MAX_TIME = 110 / 5
MAX_QUAL = 100
NUM_SIMUL = 5
alpha = [0.1, 0.9, 0.4, 0, 0.2, 0.3]
resultsLP = {'scenario1': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
             'scenario2': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
             'scenario3': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)}}
tt_resultsLP = {'scenario1': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                'scenario2': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                'scenario3': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)}}
num_rejects_LP = {'scenario1': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                  'scenario2': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                  'scenario3': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)}}
num_preformed_LP = {'scenario1': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                    'scenario2': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)},
                    'scenario3': {'simul' + str(i): {"reg": [], "heuristic": []} for i in range(NUM_SIMUL)}}
results_greedy = {'scenario1': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                  'scenario2': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                  'scenario3': {'simul' + str(i): 0 for i in range(NUM_SIMUL)}}
results_tt_greedy = {'scenario1': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                     'scenario2': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                     'scenario3': {'simul' + str(i): 0 for i in range(NUM_SIMUL)}}
num_rejects_greedy = {'scenario1': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                      'scenario2': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                      'scenario3': {'simul' + str(i): 0 for i in range(NUM_SIMUL)}}
num_preformed_greedy = {'scenario1': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                        'scenario2': {'simul' + str(i): 0 for i in range(NUM_SIMUL)},
                        'scenario3': {'simul' + str(i): 0 for i in range(NUM_SIMUL)}}
# keys_time = ['WT', 'noWT']

for dir_name_scenario in os.listdir(dir_path):
    num_scenario = int(dir_name_scenario.split('.')[0].split('_')[-1])
    stats_scenario_path = os.path.join(dir_path, dir_name_scenario)
    for dir_name_simul in os.listdir(stats_scenario_path):
        num_simul = int(dir_name_simul.split(' ')[-1])
        simul_stats_scenario_path = os.path.join(stats_scenario_path, dir_name_simul)
        for dir_algo in os.listdir(simul_stats_scenario_path):
            algo_path_logs = os.path.join(simul_stats_scenario_path, dir_algo)
            C = int(algo_path_logs.split('queue')[0].split('_')[-1])
            num_workers = int(algo_path_logs.split('w_')[0].split('_')[-1])

            if 'LP' in algo_path_logs:
                # if 'noWT' in dir_name:
                #     key_time = 'noWT'
                # else:
                #     key_time = 'WT'
                if 'delta' in algo_path_logs:
                    heurist = 'heuristic'
                else:
                    heurist = 'reg'

                for file_name in os.listdir(algo_path_logs):
                    if file_name == 'rejected_jobs.csv':
                        rej_df = pd.read_csv(os.path.join(algo_path_logs, file_name))
                        num_rejects_LP['scenario' + str(num_scenario)]['simul' + str(num_simul)][heurist].append(
                            rej_df.shape[0])

                    if file_name == 'jobs_completion_log.csv':
                        jobs_comp_df = pd.read_csv(os.path.join(algo_path_logs, file_name), index_col=False)
                        num_preformed_LP['scenario' + str(num_scenario)]['simul' + str(num_simul)][heurist].append(
                            jobs_comp_df.shape[0])

                    if file_name == 'algorithm_debug.csv':
                        alg_debug_df = pd.read_csv(os.path.join(algo_path_logs, file_name), index_col=False)
                        qual_col = alg_debug_df["real quality"].apply(lambda x: (alpha[1] * (int(x) / MAX_QUAL)))
                        real_time_col = alg_debug_df["real time"].map(
                            lambda x: x if isinstance(x, int) else int(x.replace(")", "")))
                        waiting_time_col = alg_debug_df['active time']
                        total_time_col = real_time_col + waiting_time_col
                        norm_real_time_col = real_time_col.apply(
                            lambda x: (-1) * alpha[0] * np.log(x) / np.log(MAX_TIME))
                        norm_total_time_col = total_time_col.apply(
                            lambda x: (-1) * alpha[0] * np.log(x) / np.log(MAX_TIME + C))
                        eval_col = norm_real_time_col + qual_col + 1.0
                        tt_eval_col = norm_total_time_col + qual_col + 1.0

                eval_result = eval_col.head(jobs_comp_df.shape[0]).sum()
                tt_eval_result = tt_eval_col.head(jobs_comp_df.shape[0]).sum()

                resultsLP['scenario' + str(num_scenario)]['simul' + str(num_simul)][heurist].append(eval_result)
                tt_resultsLP['scenario' + str(num_scenario)]['simul' + str(num_simul)][heurist].append(tt_eval_result)


            elif 'greedy' in algo_path_logs:

                for file_name in os.listdir(algo_path_logs):
                    if file_name == 'rejected_jobs.csv':
                        rej_df = pd.read_csv(os.path.join(algo_path_logs, file_name))
                        num_rejects_greedy['scenario' + str(num_scenario)]['simul' + str(num_simul)] = rej_df.shape[0]

                    if file_name == 'jobs_completion_log.csv':
                        jobs_comp_df = pd.read_csv(os.path.join(algo_path_logs, file_name), index_col=False)
                        num_preformed_greedy['scenario' + str(num_scenario)]['simul' + str(num_simul)] = \
                            jobs_comp_df.shape[0]

                    if file_name == 'algorithm_debug.csv':
                        alg_debug_df = pd.read_csv(os.path.join(algo_path_logs, file_name), index_col=False)
                        qual_col = alg_debug_df["real quality"].apply(lambda x: (alpha[1] * (int(x) / MAX_QUAL)))
                        real_time_col = alg_debug_df["real time"].map(lambda x: int(x.replace(")", "")))
                        waiting_time_col = alg_debug_df['active time']
                        total_time_col = real_time_col + waiting_time_col
                        norm_real_time_col = real_time_col.apply(
                            lambda x: (-1) * alpha[0] * np.log(x) / np.log(MAX_TIME))
                        norm_total_time_col = total_time_col.apply(
                            lambda x: (-1) * alpha[0] * np.log(x) / np.log(MAX_TIME + C))
                        eval_col = norm_real_time_col + qual_col + 1.0
                        tt_eval_col = norm_total_time_col + qual_col + 1.0

                eval_result = eval_col.head(jobs_comp_df.shape[0]).sum()
                tt_eval_result = tt_eval_col.head(jobs_comp_df.shape[0]).sum()
                results_greedy['scenario' + str(num_scenario)]['simul' + str(num_simul)] = eval_result
                results_tt_greedy['scenario' + str(num_scenario)]['simul' + str(num_simul)] = tt_eval_result

avg_results_greedy = {'scenario1': {'mean': statistics.mean(list(results_greedy['scenario1'].values())),
                                    'std': statistics.stdev(list(results_greedy['scenario1'].values()))},
                      'scenario2': {'mean': statistics.mean(list(results_greedy['scenario2'].values())),
                                    'std': statistics.stdev(list(results_greedy['scenario2'].values()))},
                      'scenario3': {'mean': statistics.mean(list(results_greedy['scenario3'].values())),
                                    'std': statistics.stdev(list(results_greedy['scenario3'].values()))}}
avg_tt_results_greedy = {'scenario1': {'mean': statistics.mean(list(results_tt_greedy['scenario1'].values())),
                                       'std': statistics.stdev(list(results_tt_greedy['scenario1'].values()))},
                         'scenario2': {'mean': statistics.mean(list(results_tt_greedy['scenario2'].values())),
                                       'std': statistics.stdev(list(results_tt_greedy['scenario2'].values()))},
                         'scenario3': {'mean': statistics.mean(list(results_tt_greedy['scenario3'].values())),
                                       'std': statistics.stdev(list(results_tt_greedy['scenario3'].values()))}}
avg_rejected_greedy = {'scenario1': {'mean': statistics.mean(list(num_rejects_greedy['scenario1'].values())),
                                     'std': statistics.stdev(list(num_rejects_greedy['scenario1'].values()))},
                       'scenario2': {'mean': statistics.mean(list(num_rejects_greedy['scenario2'].values())),
                                     'std': statistics.stdev(list(num_rejects_greedy['scenario2'].values()))},
                       'scenario3': {'mean': statistics.mean(list(num_rejects_greedy['scenario3'].values())),
                                     'std': statistics.stdev(list(num_rejects_greedy['scenario3'].values()))}}
avg_performed_greedy = {'scenario1': {'mean': statistics.mean(list(num_preformed_greedy['scenario1'].values())),
                                      'std': statistics.stdev(list(num_preformed_greedy['scenario1'].values()))},
                        'scenario2': {'mean': statistics.mean(list(num_preformed_greedy['scenario2'].values())),
                                      'std': statistics.stdev(list(num_preformed_greedy['scenario2'].values()))},
                        'scenario3': {'mean': statistics.mean(list(num_preformed_greedy['scenario3'].values())),
                                      'std': statistics.stdev(list(num_preformed_greedy['scenario3'].values()))}}

avg_results_LP_run_in_simul = {
    'scenario1': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in resultsLP['scenario1'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in resultsLP['scenario1'].values()])},
        'heuristic': {
            'mean': statistics.mean([statistics.mean(simul['heuristic']) for simul in resultsLP['scenario1'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in resultsLP['scenario1'].values()])}},
    'scenario2': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in resultsLP['scenario2'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in resultsLP['scenario2'].values()])},
        'heuristic': {
            'mean': statistics.mean([statistics.mean(simul['heuristic']) for simul in resultsLP['scenario2'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in resultsLP['scenario2'].values()])}},
    'scenario3': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in resultsLP['scenario3'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in resultsLP['scenario3'].values()])},
        'heuristic': {
            'mean': statistics.mean([statistics.mean(simul['heuristic']) for simul in resultsLP['scenario3'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in resultsLP['scenario3'].values()])}}}
avg_tt_results_LP_run_in_simul = {
    'scenario1': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario1'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario1'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario1'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario1'].values()])}},
    'scenario2': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario2'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario2'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario2'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario2'].values()])}},
    'scenario3': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario3'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in tt_resultsLP['scenario3'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario3'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in tt_resultsLP['scenario3'].values()])}}}
avg_rejected_LP_run_in_simul = {
    'scenario1': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario1'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario1'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario1'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario1'].values()])}},
    'scenario2': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario2'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario2'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario2'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario2'].values()])}},
    'scenario3': {
        'reg': {
            'mean': statistics.mean([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario3'].values()]),
            'std': statistics.stdev([statistics.mean(simul['reg']) for simul in num_rejects_LP['scenario3'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario3'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_rejects_LP['scenario3'].values()])}}}
avg_performed_LP_run_in_simul = {
    'scenario1': {
        'reg': {
            'mean': statistics.mean(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario1'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario1'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario1'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario1'].values()])}},
    'scenario2': {
        'reg': {
            'mean': statistics.mean(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario2'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario2'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario2'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario2'].values()])}},
    'scenario3': {
        'reg': {
            'mean': statistics.mean(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario3'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['reg']) for simul in num_preformed_LP['scenario3'].values()])},
        'heuristic': {
            'mean': statistics.mean(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario3'].values()]),
            'std': statistics.stdev(
                [statistics.mean(simul['heuristic']) for simul in num_preformed_LP['scenario3'].values()])}}}
settings_name = str(num_workers) + 'w' + str(C) + 'q'
# settings_name = settings_name.replace('_', '', 1)

with open('results/all_algos_' + settings_name + '.csv', 'w+')as fp:
    fp.write(
        'algo_name,scenario,mean_eval,mean_tt_eval,mean_num_rejects,std_eval,std_tt_eval,std_rejects\n')

    for scenario in avg_results_LP_run_in_simul.keys():
        fp.write('LP,')
        fp.write(scenario + ',')
        fp.write(str(avg_results_LP_run_in_simul[scenario]['reg']['mean']) + ',' + str(
            avg_tt_results_LP_run_in_simul[scenario]['reg']['mean']) + ',' + str(
            avg_rejected_LP_run_in_simul[scenario]['reg']['mean'])  + ',')
        fp.write(str(avg_results_LP_run_in_simul[scenario]['reg']['std']) + ',' + str(
            avg_tt_results_LP_run_in_simul[scenario]['reg']['std']) + ',' + str(
            avg_rejected_LP_run_in_simul[scenario]['reg']['std']) + '\n')
    for scenario in avg_results_LP_run_in_simul.keys():
        fp.write('LP_heuristic,')
        fp.write(scenario + ',')
        fp.write(str(avg_results_LP_run_in_simul[scenario]['heuristic']['mean']) + ',' + str(
            avg_tt_results_LP_run_in_simul[scenario]['heuristic']['mean']) + ',' + str(
            avg_rejected_LP_run_in_simul[scenario]['heuristic']['mean']) + ',')
        fp.write(str(avg_results_LP_run_in_simul[scenario]['heuristic']['std']) + ',' + str(
            avg_tt_results_LP_run_in_simul[scenario]['heuristic']['std']) + ',' + str(
            avg_rejected_LP_run_in_simul[scenario]['heuristic']['std']) + ',' + '\n')

    for scenario in avg_results_greedy.keys():
        fp.write('greedy,')
        fp.write(scenario + ',')
        fp.write(
            str(avg_results_greedy[scenario]['mean']) + ',' + str(avg_tt_results_greedy[scenario]['mean']) + ',' + str(
                avg_rejected_greedy[scenario]['mean'])  + ',')
        fp.write(
            str(avg_results_greedy[scenario]['std']) + ',' + str(avg_tt_results_greedy[scenario]['std']) + ',' + str(
                avg_rejected_greedy[scenario]['std']) + '\n')

    fp.write('LP,avg,')
    fp.write(str(statistics.mean([item['reg']['mean'] for item in avg_results_LP_run_in_simul.values()])) + ',' + str(
        statistics.mean([item['reg']['mean'] for item in avg_tt_results_LP_run_in_simul.values()])) + ',' + str(
        statistics.mean([item['reg']['mean'] for item in avg_rejected_LP_run_in_simul.values()])) + ',')
    fp.write(str(statistics.stdev([item['reg']['mean'] for item in avg_results_LP_run_in_simul.values()])) + ',' + str(
        statistics.stdev([item['reg']['mean'] for item in avg_tt_results_LP_run_in_simul.values()])) + ',' + str(
        statistics.stdev([item['reg']['mean'] for item in avg_rejected_LP_run_in_simul.values()])) + '\n')

    fp.write('LP_heuristic,avg,')
    fp.write(
        str(statistics.mean([item['heuristic']['mean'] for item in avg_results_LP_run_in_simul.values()])) + ',' + str(
            statistics.mean(
                [item['heuristic']['mean'] for item in avg_tt_results_LP_run_in_simul.values()])) + ',' + str(
            statistics.mean([item['heuristic']['mean'] for item in avg_rejected_LP_run_in_simul.values()])) + ',')
    fp.write(
        str(statistics.stdev([item['heuristic']['mean'] for item in avg_results_LP_run_in_simul.values()])) + ',' + str(
            statistics.stdev(
                [item['heuristic']['mean'] for item in avg_tt_results_LP_run_in_simul.values()])) + ',' + str(
            statistics.stdev([item['heuristic']['mean'] for item in avg_rejected_LP_run_in_simul.values()])) + '\n')

    fp.write('greedy,avg,')
    fp.write(str(statistics.mean([item['mean'] for item in avg_results_greedy.values()])) + ',' + str(
        statistics.mean([item['mean'] for item in avg_tt_results_greedy.values()])) + ',' + str(
        statistics.mean([item['mean'] for item in avg_rejected_greedy.values()])) + ',')
    fp.write(str(statistics.stdev([item['mean'] for item in avg_results_greedy.values()])) + ',' + str(
        statistics.stdev([item['mean'] for item in avg_tt_results_greedy.values()])) + ',' + str(
        statistics.stdev([item['mean'] for item in avg_rejected_greedy.values()])) + '\n')

with open('results/all_algos_allscenariosSimuls_' + settings_name + '.csv', 'w+')as fp:
    fp.write(
        'scenario,simul,run,eval_LP,eval_LP_heuristic,eval_greedy,tt_eval_LP,tt_eval_LP_heuristic,tt_eval_greedy,rejects_LP,rejects_LP_heuristic,rejects_greedy,prec_rejects_LP,prec_rejects_LP_heuristic,prec_rejects_greedy,num_workers,C\n')

    for scenario, simuls in resultsLP.items():
        for simul, heuristics in simuls.items():
            # for heuristic, eval_list in heuristics.items():
            for run in range(len(heuristics['reg'])):
                fp.write(scenario + ',' + simul + ',' + str(run + 1) + ',')
                fp.write(str(resultsLP[scenario][simul]['reg'][run]) + ',' + str(
                    resultsLP[scenario][simul]['heuristic'][run]) + ',' + str(results_greedy[scenario][simul]) + ',')
                fp.write(str(tt_resultsLP[scenario][simul]['reg'][run]) + ',' + str(
                    tt_resultsLP[scenario][simul]['heuristic'][run]) + ',' + str(
                    results_tt_greedy[scenario][simul]) + ',')
                fp.write(str(num_rejects_LP[scenario][simul]['reg'][run]) + ',' + str(
                    num_rejects_LP[scenario][simul]['heuristic'][run]) + ',' + str(
                    num_rejects_greedy[scenario][simul]) + ',')
                fp.write(str(num_rejects_LP[scenario][simul]['reg'][run]*100.0/(num_rejects_LP[scenario][simul]['reg'][run]+num_preformed_LP[scenario][simul]['reg'][run])) + ',' + str(
                    num_rejects_LP[scenario][simul]['heuristic'][run]*100.0/(num_rejects_LP[scenario][simul]['heuristic'][run]+num_preformed_LP[scenario][simul]['heuristic'][run])) + ',' + str(
                    num_rejects_greedy[scenario][simul]*100.0/(num_rejects_greedy[scenario][simul]+num_preformed_greedy[scenario][simul])) + ',')
                fp.write(str(num_workers) + ',' + str(C) + '\n')
