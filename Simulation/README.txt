This code package runs a simulation comparing all different allocation algorithms mentioned in the paper for a specific configuration - 4 workers, waiting time of 45 time unites, and gamma 0.1.

The code uses the Solver output "X_final_normal_and_pjt.csv", "X_final_normal_and_pjt_delta.csv" as an input to the simulation.
The code's output is the evaluation function values and rejection rate for each scenario and algorithm compared.
The Input for the code is in config.txt file which contains all parameters for eavery scenario. Each line in the file contains 11  parameters for a specific run.
The runner file will create a new proccess to run each simulation for a line in "config.txt" seperatly.
the parameters:
	1. scenario ID
	2. Algorithm name
	3. Is training mode
	4. Path to mission description data file
	5. Path to time quality data file
	6. C - maximum waiting time
	7. T - time units
	8. serial number of run
	9. is using WT 
	10. Path to solver output
	11. Name of output file

To run the code:
	- make sure you have the path to the results of the Solver
	- update 'config.txt'
	- install requirements.txt in virtual environment
	- run: python3 runner.py

To create the analized result files in 'results' directory:
	- run: python3 analize_results.py
