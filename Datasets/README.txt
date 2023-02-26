Supplementary Datasets for 'Allocation Problem in Remote Teleoperation: Online Matching with Offline Reusable Resources and Delayed Assignments'

there are 2 files in this folder:
1. pjt_stats_4_states_5Hours.csv - Contains the dataset of the arrival probabilities of the various tasks.
	The first column is the arrival time, the second is the arrival probability, and the 4 other columns are the probability of a
	particular task type when a task has arrived. The probabilities are adapted from the paper by Hampshire et. al. (Hampshire et al. 2020).
2. time_quality_teleop_scenario1_simul_1.csv - Contains an example of usage duration dataset (C_lambda).
 	For a given scenario and simulation, the table contains all expected times, simulated times and qualities for each task, operator, and arrival time.
	the first column is the task ID - including its type and arrival time, the second is the operator ID, next is the expected usage time to perform the task,
	next is the actual duration time sampled for the simulation, and latly is the quality assigned to the task type.
