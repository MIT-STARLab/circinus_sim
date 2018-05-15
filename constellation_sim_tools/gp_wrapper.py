import os.path
import sys
from multiprocessing import Process, Queue
from copy import copy
import pickle
from datetime import datetime,timedelta

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
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.gp_wrapper_params = self.const_sim_inst_params['gp_wrapper_params']
        self.gp_params = self.gp_wrapper_params['gp_params']

    def run_gp(self,curr_time_dt,existing_sim_rt_conts,gp_agent_ID,latest_gp_route_uid,sat_state_by_id):

        def get_input_time(time_dt,param_mins):
            new_time = curr_time_dt + timedelta(minutes=self.gp_params['planning_past_horizon_mins'])
            return new_time.isoformat()

        ##############################
        #  set up GP inputs

        sats_state = [{"sat_id":sat_id,"batt_e_Wh":sat_state['batt_e_Wh']} for sat_id,sat_state in sat_state_by_id.items()]

        gp_instance_params = {
            "version": "0.6",
            "planning_params": {
                "planning_start" :  get_input_time(curr_time_dt,self.gp_params['planning_past_horizon_mins']),
                "planning_fixed_end" :  get_input_time(curr_time_dt,self.gp_params['planning_horizon_fixed_mins']),
                "planning_end_obs_xlnk" :  get_input_time(curr_time_dt,self.gp_params['planning_horizon_obs_xlnk_mins']),
                "planning_end_dlnk" :  get_input_time(curr_time_dt,self.gp_params['planning_horizon_dlnk_mins'])
            },
            "activity_scheduling_params": {
                "plot_activity_scheduling_results": False
            },
            "gp_agent_ID": gp_agent_ID,
            "sats_state": sats_state
        }

        esrcs = existing_sim_rt_conts
        existing_route_data = {}
        existing_route_data['existing_routes'] = [dmr for esrc in esrcs for dmr in esrc.data_routes]
        #  utilization by DMR ID. We use data volume utilization here, but for current version of global planner this should be the same as time utilization
        existing_route_data['utilization_by_existing_route_id'] = {dmr.ID:esrc.dv_utilization_by_dr[dmr] for esrc in esrcs for dmr in esrc.data_routes}
        existing_route_data['latest_gp_route_uid'] = latest_gp_route_uid

        
        gp_inputs = {
            "orbit_prop_inputs": self.orbit_prop_params,
            "orbit_link_inputs": self.orbit_link_params,
            "gp_general_params_inputs": self.gp_general_params,
            "gp_instance_params_inputs": gp_instance_params,
            "data_rates_inputs": self.data_rates_params,
            "existing_route_data": existing_route_data,
            "rs_s1_pickle": None,
            "rs_s2_pickle": None,
            "as_pickle": None,
            "file_params":  {'new_pickle_file_name_pre': "const_sim_test_pickle"}
        }

        ##############################
        #  run the G

        #  do some funny business to get access to the global planner code
        # path to runner_gp
        sys.path.append (os.path.join(self.gp_wrapper_params['gp_path'],'python_runner'))
        # path to gp_tools
        sys.path.append (self.gp_wrapper_params['gp_path'])
        from runner_gp import PipelineRunner as GPPipelineRunner

        # unpickle gp outputs if desired (instead of running)
        if self.gp_wrapper_params['restore_gp_output_from_pickle']:
            print('Load GP output pickle')
            gp_output = pickle.load (open ( self.gp_wrapper_params['gp_output_pickle_to_restore'],'rb'))
        # run gp
        else:
            print('Run GP')
            print('note: running with local circinus_tools, not circinus_tools within GP repo')
            gp_pr = GPPipelineRunner()
            gp_output = gp_pr.run(gp_inputs,verbose=True)


        ##############################
        # handle output

        # pickle gp outputs if desired
        if self.gp_wrapper_params['pickle_gp_output'] and not self.gp_wrapper_params['restore_gp_output_from_pickle']:
            pickle_name ='pickles/gp_output_%s' %(datetime.utcnow().isoformat().replace (':','_'))
            with open('%s.pkl' % ( pickle_name),'wb') as f:
                pickle.dump(  gp_output,f)


        if not gp_output['version'] == EXPECTED_GP_OUTPUT_VER:
            raise RuntimeWarning("Saw gp output version %s, expected %s"%(gp_output['version'],EXPECTED_GP_OUTPUT_VER))

        scheduled_routes = gp_output['scheduled_routes']
        latest_gp_route_uid = gp_output['latest_dr_uid']

        update_dt = 'now'  # todo: update this...

        sim_routes = []
        for dmr in scheduled_routes:
            sim_routes.append(
                SimRouteContainer(None,None,dmr,1.0,update_dt,ro_ID=copy(dmr.ID))
            )

        return sim_routes, latest_gp_route_uid

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

