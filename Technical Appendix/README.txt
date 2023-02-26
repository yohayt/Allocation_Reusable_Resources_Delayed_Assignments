Supplementary technical appendix for 'Allocation Problem in Remote Teleoperation: Online Matching with Offline Reusable Resources and Delayed Assignments'

There are 1 file in this folder:
1. NZ_LP_details.xlsx - Contains the details of the heuristic NZ_LP. As described in the paper, we added 2 constraints to the original LP to set all x_lambda to non-zero.
	For that we defined several parameters: Omega - the total expected reward achieved by the original LP, epsilon, and Delta. as described, in all settings epsilon = 0.1.
	However, the delta was determined by trial and error, in order to find the largest delta that does not harm the feasibility of the model.
	There are 3 tables in the file, one for each graph from figure 1. In each table we describe in every setting the original Omega value,
	the delta used in the heuristic and NZ_Omega - the total expected reward achieved by the NZ_LP.