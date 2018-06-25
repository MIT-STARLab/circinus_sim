#   wraps the global planner, providing an API for use within simulation components
#
# @author Kit Kennedy
#
# Notes:  
# - currently, satellites' current data storage is not passed explicitly to the global planner. rather, it is passed in the form of existing data routes, which themselves levy a requirement that the data volume that they are storing on the satellites must be below the maximum buffer value for the satellite. I.e.,  we don't consider data volume in the same way as energy storage, a big pool of nameless data volume chunks, but rather a set of discrete routes that all have their own data volume. this works well for the current global planner/local planner paradigm, but may be too restrictive for future development. in that case, data storage should be passed to the global planner,  and somehow it will need to be clarified the global planner how the data storage number for each satellite relates to the data routes that it has planned

import os.path
import sys
from copy import copy
# import pickle
from datetime import datetime,timedelta

from .sim_routing_objects import SimRouteContainer
from constellation_sim_tools.local_planner.runner_lp import PipelineRunner as LPPipelineRunner

from circinus_tools import debug_tools

EXPECTED_LP_OUTPUT_VER = '0.1'

def datetime_to_iso8601(dt):
    """ Converts a Python datetime object into an ISO8601 string. (including trailing Z)"""
    #  better than datetime's built-in isoformat() function, because sometimes that leaves off the microseconds ( which of course, is stupid)

    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

class LocalPlannerWrapper:
    """wraps the global planner scheduling algorithm, providing required inputs and converting outputs from/to constellation simulation"""

    def __init__(self, sim_params):

        # these are inputs required for the GP
        self.orbit_prop_params = sim_params['orbit_prop_params']
        # self.orbit_link_params = sim_params['orbit_link_params']
        self.gp_general_params = sim_params['gp_general_params']
        # self.data_rates_params = sim_params['data_rates_params']
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.lp_wrapper_params = self.const_sim_inst_params['lp_wrapper_params']
        self.lp_params = self.lp_wrapper_params['lp_params']

        self.sim_end_utc_dt = self.const_sim_inst_params['sim_run_params']['end_utc_dt']

    def run_lp(self,curr_time_dt,sat_indx,sat_id,lp_agent_id,existing_rt_conts,existing_data_conts,latest_lp_route_indx,sat_state):

        def get_inp_time(time_dt,param_mins):
            new_time = curr_time_dt + timedelta(minutes=param_mins)
            return new_time

        ##############################
        #  set up LP inputs

        # sats_state = [{"sat_id":sat_id,"batt_e_Wh":sat_state['batt_e_Wh']} for sat_id,sat_state in sat_state_by_id.items()]
        
        if curr_time_dt >= self.sim_end_utc_dt:
            raise RuntimeWarning('should not be running LP after end of sim')

        lp_instance_params = {
            "version": "0.1", # will need to add version checking at some point in the local planner code 
            "planning_params": {
                "planning_start_dt" :  curr_time_dt,
                "planning_leaving_flow_start_dt" :  min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.lp_params['planning_leaving_flow_start_mins'])),
                "planning_end_dt" :  min(self.sim_end_utc_dt,get_inp_time(curr_time_dt,self.lp_params['planning_horizon_mins']))
            },
            "sat_indx": sat_indx,
            "sat_id": sat_id,
            "lp_agent_ID": lp_agent_id,
            "latest_dr_uid": latest_lp_route_indx
            # "sats_state": sats_state
        }

        #  this holds all of the data about existing routes ( both planned and executed)
        existing_route_data = {}

        esrcs = existing_rt_conts
        esrcs_by_id = {rt_cont.ID:rt_cont for rt_cont in existing_rt_conts}
        existing_routes = [dmr for esrc in esrcs for dmr in esrc.get_routes()]

        existing_route_data['injected_route_ids'] = []

        # a note on copying below: we need to copy all of the existing routes here, because we update the schedule data volume attributes for routes and windows in the global planner.  if we don't copy the routes, then we will be modifying the data route objects that satellites have in their Sim route containers ( and effectively propagating information instantaneously to the satellites - double plus ungood!). We don't deepcopy because we DO want to retain the same window objects contained within the routes.  todo:  note that this passing around of the original window objects is not necessarily the best idea. We do it for now because it saves memory. note that this should not interfere with checking equality/comparing of data routes and windows later, because all of that is done using the ID for the routes and windows ( which is the same when copied)
        # planned routes are the currently planned routes -  anything that has an activity window occurring after curr_time_dt
        existing_route_data['planned_routes'] = [copy(existing_route) for existing_route in existing_routes]

        for rt in existing_route_data['planned_routes']:
            if rt.get_obs().injected:
                existing_route_data['injected_route_ids'].append(rt.ID)


        #  utilization by DMR ID. We use data volume utilization here, but for current version of global planner this should be the same as time utilization
        existing_route_data['utilization_by_planned_route_id'] = {dmr.ID:esrc.get_dmr_utilization(dmr) for esrc in esrcs for dmr in esrc.get_routes()}

        # deal with data containers (packets) on sat
        existing_route_data['utilization_by_executed_route_id'] = {}
        #  partial routes include all of those data containers on the satellite that are currently present. in the nominal situation, these routes are a subset of existing routes.  however, when off nominal behavior has happened, it could be that there is less or more data represented in these partial routes than was planned for in existing routes
        executed_routes = []
        for dc in existing_data_conts:
            # if there is currently a planned route for this data container, grab that ( note that the planned route should contain the executed route for the data container. we want to grab the planned route because it has the correct routing object ID)
            if dc.latest_planned_rt_cont:
                esrc = dc.latest_planned_rt_cont
                for dmr in esrc.get_routes():
                    executed_routes.append(copy(dmr))
                    existing_route_data['utilization_by_executed_route_id'][dmr.ID] = esrc.get_dmr_utilization(dmr)
            #  if there is no planned route for this data container (e.g. an injected observation), just go ahead and grab its executed route
            else:
                edr = dc.executed_data_route
                executed_routes.append(copy(edr))
                #  utilization for executed route is by definition 100%
                existing_route_data['utilization_by_executed_route_id'][edr.ID] = 1.0

                if dc.injected:
                    existing_route_data['injected_route_ids'].append(edr.ID)


        existing_route_data['executed_routes'] = executed_routes
        
        lp_inputs = {
            "orbit_prop_params": self.orbit_prop_params,
            "const_sim_inst_params": self.const_sim_inst_params,
            # "orbit_link_inputs": self.orbit_link_params,
            "gp_general_params": self.gp_general_params,
            "lp_instance_params": lp_instance_params,
            # "data_rates_inputs": self.data_rates_params,
            "existing_route_data": existing_route_data,
            # "file_params":  {'new_pickle_file_name_pre': "const_sim_test_pickle"}
        }

        ##############################
        #  run the LP

        #  is this still true?
        #  Note: the GP is the only place in the whole sim, currently, where scheduled data volume attributes for data routes and windows are allowed to be updated


        print('Run LP')
        lp_params = None
        lp_pr = LPPipelineRunner()
        lp_output = lp_pr.run(lp_inputs,verbose=True)

        ##############################
        # handle output

        if not lp_output['version'] == EXPECTED_LP_OUTPUT_VER:
            raise RuntimeWarning("Saw gp output version %s, expected %s"%(lp_output['version'],EXPECTED_LP_OUTPUT_VER))


        scheduled_routes = lp_output['scheduled_routes']
        all_updated_routes = lp_output['all_updated_routes']
        updated_utilization_by_route_id = lp_output['updated_utilization_by_route_id']
        latest_lp_route_indx = lp_output['latest_dr_uid']

        # sim_routes is only the updated SRCs
        sim_routes = []
        for dmr in all_updated_routes:

            # note that LP doesn't send us any routes that it freshly created but decided not to use (unlike GP, currently)

            # guard against epsilon differences allowing utilization to creep above 100%
            dmr_dv_util = min(updated_utilization_by_route_id[dmr.ID],1.0)


            # check if this sim route container already existed (and the data multi route already existed), and if so, grab the original creation time as well as determine if we have actually updated the simroutecontainer
            # Leave these times as None if (newly created,not updated) - in this case we'll update the times when we release the plans
            old_esrc = esrcs_by_id.get(dmr.ID,None)
            updated = True if not old_esrc else old_esrc.updated_check(dmr,dmr_dv_util)

            # don't make a new SRC if we're not updating anything 
            if not updated:
                continue
            
            # store nones in here if no old SRC - the nones will get replaced by the Sat scheduler after the new SRCs are released
            creation_dt = old_esrc.creation_dt if old_esrc else None
            update_dt = old_esrc.update_dt if old_esrc else None

            # we make an entirely new Sim route container for the route because that way we have a unique, new object, and we don't risk information sharing by inadvertantly updating the same object across satellites and ground network
            #   note only one Sim route container per DMR
            # honestly we probably could just use a copy() here...
            new_src = SimRouteContainer(dmr.ID,dmr,dmr_dv_util,creation_dt,update_dt,lp_agent_id)
            sim_routes.append(new_src)

            # debug_tools.debug_breakpt()

            # Figure out if this route container is intended to service an existing data container. if yes, then add that to the data container's plan history ( which is used elsewhere to make routing decisions for the data  container)
            matched_dcs = new_src.find_matching_data_conts(existing_data_conts,'executed')
            for dc in matched_dcs:
                dc.add_to_plan_hist(new_src)

        # if sat_id == 'sat1':
        #     for dmr in all_updated_routes:
        #         for wind in dmr.get_winds():
        #             if wind.window_ID == 42:
        #                 debug_tools.debug_breakpt()
                        
        if len(sim_routes) > 0:
            debug_tools.debug_breakpt()


        return sim_routes, latest_lp_route_indx
