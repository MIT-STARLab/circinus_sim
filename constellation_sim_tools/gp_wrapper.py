import os.path
import sys
from multiprocessing import Process, Queue
from copy import copy
import pickle
from datetime import datetime

from circinus_tools.scheduling.routing_objects import SimRouteContainer

# from circinus_tools.scheduling.routing_objects import 

EXPECTED_GP_OUTPUT_VER = '0.2'

class GlobalPlannerWrapper:
    """wraps the global planner scheduling algorithm, makes it hopefully easy to call"""

    def __init__(self, sim_params):

        # these are inputs required for the GP
        self.orbit_prop_params = sim_params['orbit_prop_params']
        self.orbit_link_params = sim_params['orbit_link_params']
        self.gp_general_params = sim_params['gp_general_params']
        self.data_rates_params = sim_params['data_rates_params']
        self.const_sim_params = sim_params['const_sim_params']
        self.gp_wrapper_params = self.const_sim_params['gp_wrapper_params']

    def run_gp(self):
        gp_instance_params = {
            "version": "0.4",
            "planning_params": {
                "_comment": "these are the bounds within which the beginning and end of activities must fall in order to be considered in current planning (route selection + scheduling) run",
                "planning_start" :  "2016-02-14T04:00:00.000000Z",
                "planning_end_obs_xlnk" :  "2016-02-14T06:00:00.000000Z",
                "planning_end_dlnk" :  "2016-02-14T06:00:00.000000Z"
            },
            "activity_scheduling_params": {
                "plot_activity_scheduling_results":   False
            },
            "gp_agent_ID": "gs_network"
        }

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

        # path to runner_gp
        sys.path.append (os.path.join(self.gp_wrapper_params['gp_path'],'python_runner'))
        # path to gp_tools
        sys.path.append (self.gp_wrapper_params['gp_path'])
        from runner_gp import PipelineRunner as GPPipelineRunner

        # unpickle gp outputs if desired
        if self.gp_wrapper_params['restore_gp_output_from_pickle']:
            print('Load GP output pickle')
            gp_output = pickle.load (open ( self.gp_wrapper_params['gp_output_pickle_to_restore'],'rb'))
        # run gp
        else:
            print('Run GP')
            print('note: running with local circinus_tools, not circinus_tools within GP repo')
            gp_pr = GPPipelineRunner()
            gp_output = gp_pr.run(gp_inputs,verbose=True)

        # pickle gp outputs if desired
        if self.gp_wrapper_params['pickle_gp_output'] and not self.gp_wrapper_params['restore_gp_output_from_pickle']:
            pickle_name ='pickles/gp_output_%s' %(datetime.utcnow().isoformat().replace (':','_'))
            with open('%s.pkl' % ( pickle_name),'wb') as f:
                pickle.dump(  gp_output,f)


        if not gp_output['version'] == EXPECTED_GP_OUTPUT_VER:
            raise RuntimeWarning("Saw gp output version %s, expected %s"%(gp_output['version'],EXPECTED_GP_OUTPUT_VER))

        scheduled_routes = gp_output['scheduled_routes']

        update_dt = 'now'

        sim_routes = []
        for dmr in scheduled_routes:
            sim_routes.append(
                SimRouteContainer(None,None,dmr,1.0,update_dt,ro_ID=copy(dmr.ID))
            )

        print(sim_routes)

        # if I want to make this a separate process at some point...
        # note the use of multiprocessing's Process means (I think) that we'll be making a copy of all the data in gp_inputs every time we call this, including the large (and unchanging!) data rates inputs. Not super great, but I don't think it'll be problematic.
        # todo: figure out a way to avoid this, possibly with mp Pool that can keep a persistent worker
        # import runner_gp
        # queue = Queue()
        # queue.put(gp_inputs)
        # verbose = True
        # p = Process(target=runner_gp.remote_multiproc_run, args=(queue,verbose))
        # p.start()
        # gp_output = queue.get()
        # p.join()

