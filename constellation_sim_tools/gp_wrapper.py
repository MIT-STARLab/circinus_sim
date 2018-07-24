#   wraps the global planner, providing an API for use within simulation components
#
# @author Kit Kennedy
#
# Notes:  
# - currently, satellites' current data storage is not passed explicitly to the global planner. rather, it is passed in the form of existing data routes, which themselves levy a requirement that the data volume that they are storing on the satellites must be below the maximum buffer value for the satellite. I.e.,  we don't consider data volume in the same way as energy storage, a big pool of nameless data volume chunks, but rather a set of discrete routes that all have their own data volume. this works well for the current global planner/local planner paradigm, but may be too restrictive for future development. in that case, data storage should be passed to the global planner,  and somehow it will need to be clarified the global planner how the data storage number for each satellite relates to the data routes that it has planned

import os.path
import sys
import json
import pickle
# from multiprocessing import Process, Queue
from copy import copy
# import pickle
from datetime import datetime,timedelta

from .sim_routing_objects import SimRouteContainer

from circinus_tools import debug_tools

EXPECTED_GP_OUTPUT_VER = '0.2'

def datetime_to_iso8601(dt):
    """ Converts a Python datetime object into an ISO8601 string. (including trailing Z)"""
    #  better than datetime's built-in isoformat() function, because sometimes that leaves off the microseconds ( which of course, is stupid)

    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

class GlobalPlannerWrapper:
    """wraps the global planner scheduling algorithm, providing required inputs and converting outputs from/to constellation simulation"""

    def __init__(self, sim_params):

        # these are inputs required for the GP
        self.orbit_prop_params = sim_params['orbit_prop_params']
        self.orbit_link_params = sim_params['orbit_link_params']
        self.gp_general_params = sim_params['gp_general_params']
        self.data_rates_params = sim_params['data_rates_params']
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.gp_wrapper_params = self.const_sim_inst_params['gp_wrapper_params']
        self.gp_params = self.gp_wrapper_params['gp_params']

        self.sim_end_utc_dt = self.const_sim_inst_params['sim_run_params']['end_utc_dt']

        self.first_iter = True

    def run_gp(self,curr_time_dt,existing_rt_conts,gp_agent_ID,latest_gp_route_indx,sat_state_by_id):

        def get_inp_time(time_dt,param_mins):
            new_time = curr_time_dt + timedelta(minutes=param_mins)
            return new_time

        ##############################
        #  set up GP inputs

        sats_state = [{"sat_id":sat_id,"batt_e_Wh":sat_state['batt_e_Wh']} for sat_id,sat_state in sat_state_by_id.items()]

        

        if curr_time_dt >= self.sim_end_utc_dt:
            raise RuntimeWarning('should not be running GP after end of sim')

        gp_instance_params = {
            "version": "0.7",
            "planning_params": {
                "planning_start" :  datetime_to_iso8601(get_inp_time(curr_time_dt,self.gp_params['planning_past_horizon_mins'])),
                "planning_fixed_end" :  datetime_to_iso8601(min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.gp_params['planning_horizon_fixed_mins']))),
                "planning_end_obs" :  datetime_to_iso8601(min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.gp_params['planning_horizon_obs_mins']))),
                "planning_end_xlnk" :  datetime_to_iso8601(min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.gp_params['planning_horizon_xlnk_mins']))),
                "planning_end_dlnk" :  datetime_to_iso8601(min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.gp_params['planning_horizon_dlnk_mins']))),
                "max_num_dlnks_allowed_after_planning_end_xlnk": self.gp_params['max_num_dlnks_allowed_after_planning_end_xlnk']
            },
            "activity_scheduling_params": {
                "plot_activity_scheduling_results": False
            },
            "gp_agent_ID": gp_agent_ID,
            "initial_gp_route_indx": latest_gp_route_indx,
            "sats_state": sats_state
        }

        esrcs = existing_rt_conts
        esrcs_by_id = {rt_cont.ID:rt_cont for rt_cont in existing_rt_conts}
        existing_route_data = {}
        existing_routes = [dmr for esrc in esrcs for dmr in esrc.get_routes()]
        #  we need to copy all of the existing routes here, because we update the schedule data volume attributes for routes and windows in the global planner.  if we don't copy the routes, then we will be modifying the data route objects that satellites have in their Sim route containers ( and effectively propagating information instantaneously to the satellites - double plus ungood!). 
        # We don't deepcopy because we don't want to make copise of the (many!) window objects contained within the routes. This is just to save memory, not because we are dependent on the objects staying the same (note: this is at least the intent - any behavior not conforming is a bug, as of June 3 2018)
        existing_routes_copy = []
        for existing_route in existing_routes:
            existing_routes_copy.append(copy(existing_route))

        existing_route_data['existing_routes'] = existing_routes_copy

        #  utilization by DMR ID. We use data volume utilization here, but for current version of global planner this should be the same as time utilization
        existing_route_data['utilization_by_existing_route_id'] = {dmr.ID:esrc.get_dmr_utilization(dmr) for esrc in esrcs for dmr in esrc.get_routes()}
        
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

        # save off a json with these gp params so we can run this instance again (for debug)
        with open('pickles/most_recent_gp_instance_params.json','w') as f:
            json.dump(  gp_instance_params,f)

        # save off a pickle with these gp params so we can run this instance again (for debug)
        with open('pickles/most_recent_gp_existing_routes_input.pkl','wb') as f:
            pickle.dump(  existing_route_data,f)

        ##############################
        #  run the GP

        #  Note: the GP is the only place in the whole sim, currently, where scheduled data volume attributes for data routes and windows are allowed to be updated

        #  do some funny business to get access to the global planner code
        # path to runner_gp
        if sys.platform == 'win32':
            # todo: this is probably not the right way to support windows users...
            sys.path.append (os.path.join(self.gp_wrapper_params['gp_path'].replace('/','\\'),'python_runner'))
        else:
            sys.path.append (os.path.join(self.gp_wrapper_params['gp_path'],'python_runner'))

        # path to gp_tools
        sys.path.append (self.gp_wrapper_params['gp_path'])
        from runner_gp import PipelineRunner as GPPipelineRunner

        print('Run GP')
        print('note: running with local circinus_tools, not circinus_tools within GP repo')
        gp_pr = GPPipelineRunner()
        gp_output = gp_pr.run(gp_inputs,verbose=True)

        ##############################
        # handle output

        if not gp_output['version'] == EXPECTED_GP_OUTPUT_VER:
            raise RuntimeWarning("Saw gp output version %s, expected %s"%(gp_output['version'],EXPECTED_GP_OUTPUT_VER))

        scheduled_routes = gp_output['scheduled_routes']
        all_updated_routes = gp_output['all_updated_routes']
        latest_gp_route_indx = gp_output['latest_dr_uid']

        scheduled_routes_set = set(scheduled_routes)
        existing_routes_set = set(existing_routes)
        sim_routes = []
        for dmr in all_updated_routes:
            #  we only want to consider this route if it was actually scheduled (delivers real data volume) or is an existing route.  any new routes that were constructed in the global planner that don't get scheduled we can ignore ( there's no point in keeping track of them because they're useless -  and we haven't yet told any of the satellites about them so we can discard them now).  this is in contrast to existing routes which, even if they get unscheduled by the global planner, we have to keep track of because we could've already told the satellites about them after a previous global planning session ( so now we you tell them that decisions have changed)
            if not (dmr in scheduled_routes_set or dmr in existing_routes_set):
                continue

            dmr_dv_util = dmr.get_sched_utilization()


            # check if this sim route container already existed (and the data multi route already existed), and if so, grab the original creation time as well as determine if we have actually updated the simroutecontainer
            # Leave these times as None if (newly created,not updated) - in this case we'll update the times when we release the plans
            old_esrc = esrcs_by_id.get(dmr.ID,None)
            creation_dt = old_esrc.creation_dt if old_esrc else None
            update_dt = old_esrc.update_dt if (old_esrc and not old_esrc.updated_check(dmr,dmr_dv_util)) else None

            # we make an entirely new Sim route container for the route because that way we have a unique, new object, and we don't risk information sharing by inadvertantly updating the same object across satellites and ground network
            #   note only one Sim route container per DMR
            # honestly we probably could just use a copy() here...
            sim_routes.append(
                SimRouteContainer(dmr.ID,dmr,dmr_dv_util,creation_dt,update_dt,gp_agent_ID)
            )

        num_existing_routes_scheduled = len([dmr for dmr in scheduled_routes if dmr in existing_routes])
        existing_routes_scheduled_utilization = sum([dmr.get_sched_utilization() for dmr in scheduled_routes if dmr in existing_routes])
        existing_routes_utilization = sum(util for util in existing_route_data['utilization_by_existing_route_id'].values())

        print('fraction of existing routes kept in schedule: %d / %d '%(num_existing_routes_scheduled,len(existing_routes)))
        print('fraction of existing utilization kept in schedule: %f / %f '%(existing_routes_scheduled_utilization,existing_routes_utilization))
        print('to ease those copypasta blues:')
        print('%d %d '%(len(existing_routes),num_existing_routes_scheduled))
        print('%f %f '%(existing_routes_utilization,existing_routes_scheduled_utilization))

        self.first_iter = False

        return sim_routes, latest_gp_route_indx

        # if I want to make calling the GP a separate process at some point...
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

