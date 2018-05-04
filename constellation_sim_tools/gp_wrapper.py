import sys
from multiprocessing import Process, Queue

class GlobalPlannerWrapper:
    """wraps the global planner scheduling algorithm, makes it hopefully easy to call"""

    def __init__(self, sim_params):

        # these are inputs required for the GP
        self.orbit_prop_params = sim_params['orbit_prop_params']
        self.orbit_link_params = sim_params['orbit_link_params']
        self.gp_general_params = sim_params['gp_general_params']
        self.data_rates_params = sim_params['data_rates_params']

    def run_gp(self,gp_instance_params):
        gp_inputs = {
            "orbit_prop_inputs": self.orbit_prop_params,
            "orbit_link_inputs": self.orbit_link_params,
            "gp_general_params_inputs": self.gp_general_params,
            "gp_instance_params_inputs": gp_instance_params,
            "data_rates_inputs": self.data_rates_params,
            "rs_s1_pickle": None,
            "rs_s2_pickle": None,
            "as_pickle": None,
            "file_params":  {'new_pickle_file_name_pre': "const_sim_test_pickle"}
        }

        # note the use of multiprocessing's Process means (I think) that we'll be making a copy of all the data in gp_inputs every time we call this, including the large (and unchanging!) data rates inputs. Not super great, but I don't think it'll be problematic.
        # todo: figure out a way to avoid this, possibly with mp Pool that can keep a persistent worker

        sys.path.append ('/Users/ktikennedy/Dropbox (MIT)/MIT/Research/CIRCINUS/GlobalPlanner/python_runner')
        sys.path.append ('/Users/ktikennedy/Dropbox (MIT)/MIT/Research/CIRCINUS/GlobalPlanner')
        from runner_gp import PipelineRunner as GPPipelineRunner

        print('Run GP')
        print('note: running with local circinus_tools, not circinus_tools within GP repo')
        gp_pr = GPPipelineRunner()
        gp_output = gp_pr.run(gp_inputs,verbose=True)

        # if I want to make this a separate process at some point...
        # import runner_gp
        # queue = Queue()
        # queue.put(gp_inputs)
        # verbose = True
        # p = Process(target=runner_gp.remote_multiproc_run, args=(queue,verbose))
        # p.start()
        # gp_output = queue.get()
        # p.join()

        print(gp_output.keys())