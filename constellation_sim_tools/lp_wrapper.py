#   wraps the global planner, providing an API for use within simulation components
#
# @author Kit Kennedy
#
# Notes:  
# - currently, satellites' current data storage is not passed explicitly to the global planner. rather, it is passed in the form of existing data routes, which themselves levy a requirement that the data volume that they are storing on the satellites must be below the maximum buffer value for the satellite. I.e.,  we don't consider data volume in the same way as energy storage, a big pool of nameless data volume chunks, but rather a set of discrete routes that all have their own data volume. this works well for the current global planner/local planner paradigm, but may be too restrictive for future development. in that case, data storage should be passed to the global planner,  and somehow it will need to be clarified the global planner how the data storage number for each satellite relates to the data routes that it has planned

import os.path
import sys
from copy import copy, deepcopy
# import pickle
from datetime import datetime,timedelta

from .sim_routing_objects import SimRouteContainer
from constellation_sim_tools.local_planner.runner_lp import PipelineRunner as LPPipelineRunner

from circinus_tools import debug_tools, time_tools
from circinus_tools.scheduling.routing_objects import DataRoute, DataMultiRoute, RoutingObjectID
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from circinus_tools.activity_bespoke_handling import ActivityTimingHelper

EXPECTED_LP_OUTPUT_VER = '0.1'

def datetime_to_iso8601(dt):
    """ Converts a Python datetime object into an ISO8601 string. (including trailing Z)"""
    #  better than datetime's built-in isoformat() function, because sometimes that leaves off the microseconds ( which of course, is stupid)

    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

class LocalPlannerWrapper:
    """wraps the global planner scheduling algorithm, providing required inputs and converting outputs from/to constellation simulation"""

    def __init__(self, sim_params, metrics_calcs_obj):

        # these are inputs required for the GP
        self.orbit_prop_params = sim_params['orbit_prop_params']
        # self.orbit_link_params = sim_params['orbit_link_params']
        self.gp_general_params = sim_params['gp_general_params']
        # self.data_rates_params = sim_params['data_rates_params']
        self.const_sim_inst_params = sim_params['const_sim_inst_params']
        self.lp_wrapper_params = self.const_sim_inst_params['lp_wrapper_params']
        self.lp_params = self.lp_wrapper_params['lp_params']

        self.sim_end_utc_dt = self.const_sim_inst_params['sim_run_params']['end_utc_dt']

        lp_general_params = sim_params['const_sim_inst_params']['lp_general_params']

        self.use_self_replanner = lp_general_params['use_self_replanner']
        self.verbose_milp = lp_general_params['verbose_milp']
        self._lp_dv_epsilon = lp_general_params['dv_epsilon_Mb']
        self.existing_utilization_epsilon = lp_general_params['existing_utilization_epsilon']

        self.mc = metrics_calcs_obj
        sat_params = self.orbit_prop_params['sat_params']
        orbit_params = self.orbit_prop_params['orbit_params']
        # this is only imported for the SRP
        self.act_timing_helper = ActivityTimingHelper(sat_params['activity_params'],orbit_params['sat_ids_by_orbit_name'],sat_params['sat_id_order'],None)

    @property
    def lp_dv_epsilon(self):
        return self._lp_dv_epsilon

    def run_lp(self,curr_time_dt,sat_indx,sat_id,lp_agent_id,existing_rt_conts,existing_data_conts,latest_lp_route_indx,sat_schedule_arb,replan_type='nominal',sat_state=None):

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

        # a note on copying below: we need to copy all of the existing routes here, because we update the schedule data volume attributes for routes and windows in the global planner.  if we don't copy the routes, then we will be modifying the data route objects that satellites have in their Sim route containers ( and effectively propagating information instantaneously to the satellites - double plus ungood!). We don't deepcopy because we DO want to retain the same window objects contained within the routes.  
        # TODO:  note that this passing around of the original window objects is not necessarily the best idea. We do it for now because it saves memory. note that this should not interfere with checking equality/comparing of data routes and windows later, because all of that is done using the ID for the routes and windows ( which is the same when copied)
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
        # NOTE! these are tuples
        executed_routes = []

        # BIG FAT NOTE: technically, this is mildly broken due to DC forking. Even if two DCs actually orginated from the same obs, they will be accounted for as two different executed routes here. The negative effect of this should be small in practice.
        for dc in existing_data_conts:
            # if there is currently a planned route for this data container, grab that ( note that the planned route should contain the executed route for the data container. we want to grab the planned route because it has the correct routing object ID)
            if dc.latest_planned_rt_cont:
                esrc = dc.latest_planned_rt_cont
                for dmr in esrc.get_routes():
                    executed_routes.append((dc.ID,copy(dmr)))
                    existing_route_data['utilization_by_executed_route_id'][dmr.ID] = esrc.get_dmr_utilization(dmr)
            #  if there is no planned route for this data container (e.g. an injected observation), just go ahead and grab its executed route
            else:
                edr = dc.executed_data_route
                executed_routes.append((dc.ID,copy(edr)))
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


        
        lp_params = None
        # TODO: Major refactor needed here, technically 'nominal' injected obs are just a special type of 'disruption'
        if replan_type == 'disruption':
            if self.use_self_replanner:
                print('Running schedule disruption replanning')
                lp_output = run_schedule_disruption_planner(sat_schedule_arb,lp_inputs,self.act_timing_helper)
            else:
                # NOTE: sometimes running Kit's LP with no injected obs can cause problems downstream (oversubscribing windows)
                """ print('Self-Replanner not enabled, using just original LP, check lp_general_params')
                print('Run LP for schedule disruptions')

                lp_pr = LPPipelineRunner()
                lp_output = lp_pr.run(lp_inputs,verbose=self.verbose_milp)
                if not lp_output['version'] == EXPECTED_LP_OUTPUT_VER:
                    raise RuntimeWarning("Saw gp output version %s, expected %s"%(lp_output['version'],EXPECTED_LP_OUTPUT_VER)) """
                print('Self-Replanner not enabled, making no changes')
                lp_output = {
                    'scheduled_routes':lp_inputs['existing_route_data']['planned_routes'],
                    'all_routes_after_update':lp_inputs['existing_route_data']['planned_routes'],
                    'updated_utilization_by_route_id':lp_inputs['existing_route_data']['utilization_by_planned_route_id'],
                    'latest_dr_uid':lp_inputs['lp_instance_params']['latest_dr_uid'],
                    'dc_id_by_scheduled_rt_id':{}
                }
        elif replan_type == 'nominal':
            print('Run LP for injected obs AND schedule disruptions ')
            lp_pr = LPPipelineRunner()
            lp_output = lp_pr.run(lp_inputs,verbose=self.verbose_milp)
            if not lp_output['version'] == EXPECTED_LP_OUTPUT_VER:
                raise RuntimeWarning("Saw gp output version %s, expected %s"%(lp_output['version'],EXPECTED_LP_OUTPUT_VER))
        else:
            raise NotImplementedError('only disruption and nominal replans exist')
        
        ##############################
        # handle output
        scheduled_routes = lp_output['scheduled_routes']
        all_routes_after_update = lp_output['all_routes_after_update']
        updated_utilization_by_route_id = lp_output['updated_utilization_by_route_id']
        latest_lp_route_indx = lp_output['latest_dr_uid']
        dc_id_by_scheduled_rt_id = lp_output['dc_id_by_scheduled_rt_id']
        next_window_id = lp_output.get('next_window_uid',None)   # technically, this doesn't need to be an output since it directly written to the sat_schedule_arb.plan_db.sat_windows_dict

        # sim_routes is only the updated SRCs
        sim_routes = []
        dc_id_by_new_src_id = {}
        # note that these routes don't necessarily have to have anything changed
        for dmr in all_routes_after_update:

            # note that LP doesn't send us any routes that it freshly created but decided not to use (unlike GP, currently)

            # guard against epsilon differences allowing utilization to creep above 100%
            dmr_dv_util = min(updated_utilization_by_route_id[dmr.ID],1.0)


            # check if this sim route container already existed (and the data multi route already existed), and if so, grab the original creation time as well as determine if we have actually updated the simroutecontainer
            # Leave these times as None if (newly created,not updated) - in this case we'll update the times when we release the plans
            old_esrc = esrcs_by_id.get(dmr.ID,None)
            # the *2 factor in the below is because the LP allows up to (but really a tiny bit more than) self.existing_utilization_epsilon variation greater than existing utilization. So should check for a change bigger than that. 2 times thi small number should be safe
            updated = True if not old_esrc else old_esrc.updated_check(dmr,dmr_dv_util,2*self.existing_utilization_epsilon)

            # don't make a new SRC if we're not updating anything 
            if not updated:
                continue
            
            # store nones in here if need be - the nones will get replaced by the Sat scheduler after the new SRCs are released
            creation_dt = old_esrc.creation_dt if old_esrc else None
            update_dt = None

            # we make an entirely new Sim route container for the route because that way we have a unique, new object, and we don't risk information sharing by inadvertantly updating the same object across satellites and ground network
            #   note only one Sim route container per DMR
            # honestly we probably could just use a copy() here...
            new_src = SimRouteContainer(dmr.ID,dmr,dmr_dv_util,creation_dt,update_dt,lp_agent_id)
            sim_routes.append(new_src)

            if dmr.ID in dc_id_by_scheduled_rt_id.keys():
                dc_id_by_new_src_id[new_src.ID] = dc_id_by_scheduled_rt_id[dmr.ID] 


        do_metrics = True

        if do_metrics:
            pre_utilization_by_route_id = existing_route_data['utilization_by_planned_route_id']
            post_utilization_by_route_id = copy(updated_utilization_by_route_id)

            # to deal with fact that all_routes_after_update will not actually always have all the pre-existing planned routes included in it. If not, it's because they're not added in in lp_scheduling's extract_updated_routes (the LP just straight-up ignored them)
            all_post_routes = copy(all_routes_after_update)
            for rt in existing_route_data['planned_routes']:
                if not rt in all_post_routes:
                    all_post_routes.append(rt)
                    post_utilization_by_route_id[rt.ID] = pre_utilization_by_route_id[rt.ID]

            data_container_rts = [dc.executed_data_route for dc in  existing_data_conts]

            self.do_post_metrics(curr_time_dt,existing_route_data['planned_routes'], all_post_routes, pre_utilization_by_route_id,post_utilization_by_route_id,data_container_rts)


        # TODO: Determine how much "surprise" DV (included injected and schedule disruptions) is left over


        # if sat_id == 'sat1':
        #     for dmr in all_routes_after_update:
        #         for wind in dmr.get_winds():
        #             if wind.window_ID == 42:
        #                 debug_tools.debug_breakpt()
                        
        # if len(sim_routes) > 0:
        #     print(sat_id)
        #     print(curr_time_dt)
        #     print("sim_routes")
        #     print(sim_routes)
        #     debug_tools.debug_breakpt()


        return sim_routes, dc_id_by_new_src_id, latest_lp_route_indx

    def do_post_metrics(self,curr_time_dt,pre_planned_routes,post_planned_routes,pre_utilization_by_route_id,post_utilization_by_route_id,data_container_rts):

        def pre_rt_dv_getter(rt):
            return rt.data_vol * pre_utilization_by_route_id[rt.ID]

        def post_rt_dv_getter(rt):
            # the LP is allowed to add in some utlization fudge in order to avoid situations where route utilization is right on the edge of meeting route min DV requirement. This fudge isn't super great, but only affects route plans - it doesn't produce DV out of thin air in the sim, because dv is only created by executing obs windows
            pre_util = 1.0
            if rt.ID in pre_utilization_by_route_id.keys():
                pre_util = pre_utilization_by_route_id[rt.ID]

            return rt.data_vol * min(pre_util,post_utilization_by_route_id[rt.ID])


        pre_routes_regular = [rt for rt in pre_planned_routes if not rt.get_obs().injected and pre_rt_dv_getter(rt) > 50 ]
        pre_routes_injected = [rt for rt in pre_planned_routes if rt.get_obs().injected and pre_rt_dv_getter(rt) > 50 ]

        post_routes_regular = [rt for rt in post_planned_routes if not rt.get_obs().injected and post_rt_dv_getter(rt) > 50 ]
        post_routes_injected = [rt for rt in post_planned_routes if rt.get_obs().injected and post_rt_dv_getter(rt) > 50 ]

        print('##############################')
        print('LP Metrics')
        print('num pre regular routes: %d'%len(pre_routes_regular))
        print('num pre injected routes: %d'%len(pre_routes_injected))
        print('num post regular routes: %d'%len(post_routes_regular))
        print('num post injected routes: %d'%len(post_routes_injected))
        print('------------------------------')
        if self.verbose_milp:
            print('Pre')
            print('regular dv')
            self.mc.assess_dv_by_obs([],pre_routes_regular,rt_exec_dv_getter=pre_rt_dv_getter,verbose = True)
            print('-------')
            print('injected dv')
            self.mc.assess_dv_by_obs([],pre_routes_injected,rt_exec_dv_getter=pre_rt_dv_getter ,verbose = True)
            print('--------------')
            # print('regular latency')
            # self.mc.assess_latency_from_timepoint([],pre_routes_regular, rt_exec_dv_getter=pre_rt_dv_getter ,verbose = True)
            # print('-------')
            # print('injected latency')
            # inj_lat_stats = self.mc.assess_latency_by_obs([],pre_routes_injected, rt_exec_dv_getter=pre_rt_dv_getter ,verbose = True)
            print('regular rts dlnk latency from curr time')
            self.mc.assess_latency_from_timepoint(pre_routes_regular, curr_time_dt,verbose = True)
            print('-------')
            print('injected rts dlnk latency from curr time')
            self.mc.assess_latency_from_timepoint(pre_routes_injected, curr_time_dt,verbose = True)
            print('------------------------------')
            print('Post')
            print('regular dv')
            self.mc.assess_dv_by_obs([],post_routes_regular,rt_exec_dv_getter=post_rt_dv_getter,verbose = True)
            print('-------')
            print('injected dv')
            self.mc.assess_dv_by_obs([],post_routes_injected,rt_exec_dv_getter=post_rt_dv_getter,verbose = True)
            print('--------------')
            # self.mc.assess_latency_by_obs([],post_routes_regular, rt_exec_dv_getter=post_rt_dv_getter ,verbose = True)
            # print('-------')
            # print('injected latency')
            # self.mc.assess_latency_by_obs([],post_routes_injected, rt_exec_dv_getter=post_rt_dv_getter ,verbose = True)
            print('regular rts dlnk latency from curr time')
            self.mc.assess_latency_from_timepoint(post_routes_regular, curr_time_dt,verbose = True)
            print('-------')
            print('injected rts dlnk latency from curr time')
            self.mc.assess_latency_from_timepoint(post_routes_injected, curr_time_dt,verbose = True)
            # print('injected obs:')
            # data_container_rts
            # for rt in post_routes_injected:
            #     print(rt.get_obs())

            print('injected dv remaining:')
            for rt in data_container_rts:
                if rt.get_obs().injected:
                    print('%s: %f'%(rt.get_obs(),rt.data_vol))

            # debug_tools.debug_breakpt()

def run_schedule_disruption_planner(sat_schedule_arb,lp_inputs,act_timing_helper):
    ## Set up problem
    # extract planner start time
    t_step = lp_inputs['const_sim_inst_params']['sim_run_params']['sim_tick_s'] 
    # apparently the state_sim is 1 timestep ahead of the planning_start_dt
    cur_time_dt = lp_inputs['lp_instance_params']['planning_params']['planning_start_dt']+timedelta(seconds=t_step)
    steps_ahead = 2 # TODO: make this a sim parameter?
    plan_start_time_dt = lp_inputs['lp_instance_params']['planning_params']['planning_start_dt'] + timedelta(seconds=steps_ahead*t_step)

    # simulation base time
    sim_base_time = lp_inputs['const_sim_inst_params']['sim_run_params']['start_utc_dt']
    sim_end_time = lp_inputs['const_sim_inst_params']['sim_run_params']['end_utc_dt']

    neighborhood_replan_success = False
    
    failure_context = sat_schedule_arb.schedule_disruption_context
    # this is the data volume from the whole activity that failed
    activity_dv_failed = failure_context['act'].executable_data_vol - failure_context['act'].executed_data_vol
    
    # this calculation is for the data volume based on the remaining valid routes
    context_SRCs = failure_context['rt_conts']
    route_dv_failed = 0
    data_containers_dv_failed = 0
    for failed_SRC in context_SRCs:
        failed_data_container_dv = sum(edc.data_vol for edc in failure_context['executable_tx_data_conts_by_rt_cont'][failed_SRC])
        route_dv_failed += failed_SRC.data_vol
        data_containers_dv_failed += failed_data_container_dv
        # sometimes the data vol of a DC will be greater than its allocated route container 
        # I believe this occurs because the GP / LP is deciding not to dlnk all data in the DC because other things are higher priority in the current time horizon (such as injected obs)
        # I should be trying to only re-allocate the activity / route failed DV, NOT The sum of undelievered DC DV, because that was not planned to be delivered in the current GP

    print('===INFO FOR DOWNLINK DV vs. ROUTE TOTALS DV===')
    print('CUR TIME: %s' % str(cur_time_dt))
    print('FAILED ACTIVITY: %s' % str(failure_context['act']))
    print('ACTIVITY DV FAILED: %f Mb' % activity_dv_failed)
    print('SUM OF ROUTES DV FAILED: %f Mb' % route_dv_failed)
    print('DIFFERENCE: %f Mb' % (activity_dv_failed - route_dv_failed))
    print('SUM OF UNDELIVERED DATA CONT DV: %f Mb' % data_containers_dv_failed)
    print('==============================================')
    dv_eps_check = lp_inputs['gp_general_params']['activity_scheduling_params']['dv_epsilon_Mb'] # the data containers can differ from the execution context by up to 1 Mb

    calc_failed_dv_from_DCs = True 
    # NOTE: activity_dv_Failed can be less than route_dv failed IF an activity is bumped off the current executable acts for the GS
    # if this is the case, we should base the failed amount on the data_containers
    if activity_dv_failed + dv_eps_check >= route_dv_failed:
        dv_failed = data_containers_dv_failed
    else: # otherwise it can be a choice
        if calc_failed_dv_from_DCs:
            dv_failed = data_containers_dv_failed
        else:
            dv_failed = route_dv_failed

    # the route failures should all be less than or equal to the activity failure
    # because the activity can have multiple routes through it, but the route only goes through one activity at a time.
    # NOTE: If activity_dv_failed <  route_dv_failed: this means that the total failed data volume is 
    # less than the sum of the failed data containers.  This could only happen if a data 
    # container was partially allocated to a downlink.  I currently don’t think this is 
    # possible, since the relationship from data containers to routes is Many..1 and 
    # relation from routes to downlinks is Many..1 (so each downlink can have many data 
    # containers delivered, but each data container should only point to one downlink, 
    # otherwise it should be forked into a new data container that points to a new route 
    # container).
    
    # NOTE: calculating failed dv from DCs will allow possible more DV to be allocated than the original failed activity had planned
    

    # Retrieve all currently planned actions (past and present)
    planned_acts = sat_schedule_arb.sim_executive_agent.exec.scheduler.get_scheduled_executable_acts()
    # reduce to those actions only in the future
    future_planned_acts = []
    for exec_act in planned_acts:
        if exec_act.act.end > cur_time_dt:
            # by checking against end time, we avoid double scheduling the current failed downlink
            future_planned_acts.append(exec_act.act)

    ############# Check data levels into the future to set planning horizon #############
    DS_data_planned_mjd = get_planned_data_state(sat_schedule_arb,future_planned_acts,cur_time_dt,sim_base_time,t_step)
    # check if data storage will be a problem 
    # predicted data increases are instanenous, and we deconflicted activities already, so
    # if there is room at the start of the window, there will be room throughout
    # need to ensure that the chosen downlink
    DS_data_min_margin = min(DS_data_planned_mjd, key=lambda x: sat_schedule_arb.state_sim.DS_max- x[1] )
    if DS_data_min_margin[1] > sat_schedule_arb.state_sim.DS_max:
        #Too much data at some point in the future, shortening self-replanning horizon
        self_replan_horizon_dt = time_tools.mjd2datetime(DS_data_min_margin[0])
    else:
        # No limit, can self replan until end of simulation
        self_replan_horizon_dt = sim_end_time
    ############# ############# ############# ############# ############# ############# 

    ############# Find all valid future downlink windows for this satellite #############
    # Note: we could get this from act_timing_helper, but we need to pass it a valid window .  this helper inherits from the dict below anyway, so same effect
    min_dlnk_duration = lp_inputs['orbit_prop_params']['sat_params']['activity_params']['min_duration_s']['dlnk'] # seconds   
    
    # get ALL potential downlink window for this sat from the plan_db
    potential_downlink_winds = sat_schedule_arb.plan_db.sat_windows_dict['dlnk_winds']

    # Remove "potential_activity_windows" and just use potential_downlink_winds

    
    valid_candidate_windows = get_candidate_downlink_windows(potential_downlink_winds,future_planned_acts,plan_start_time_dt,self_replan_horizon_dt, min_dlnk_duration)
    ############# ############# ############# ############# ############# ############# 

    # compare valid_candidate_windows (in MJD (start, stop)) to potential_downlink_winds to cut down on potential_dlnk_winds
    # NOTE: this approach could lose a lot of potential windows if there are active crosslinks during the potential downlink
    ################# Get planned downlinks based on rule #######################)
    rule = 'greedy' # attempts to schedule as much as possible as early as possible
    # this should return a list of two element tuples: (downlink window ID, additional_DV_to_add)
    new_downlinks = get_replanned_downlinks(valid_candidate_windows,dv_failed,rule)
    total_dv_in_new_downlinks = sum([new_dlnk[1] for new_dlnk in new_downlinks])
    print('DV in new downlinks: %.2f Mb' % total_dv_in_new_downlinks)
    if new_downlinks and total_dv_in_new_downlinks >= (dv_failed - dv_eps_check)/2:
        print('new downlinks developed for schedule recovery')
        new_downlinks_found = True
    else:
        print('No new downlinks available or not enough DV in new downlinks')
        new_downlinks_found = False
    ############# ############# ############# ############# ############# #############

    ############# Initialize Outputs ############# 
    # extract data from previous routes
    existing_route_data = lp_inputs['existing_route_data']
    # NOTE: if the route was already executed, then it is in "executed" and not "planned" existing route data
    # Setup lp_outputs values
    # NOTE that already "executed" routes do not appear in existing route data
    # NOTE: "Self-replanner" does NOT affect existing routes in ANY way, only attemptes to create new downlink, so I need to copy existing routes here

    # Slice lists so that they create new objects, instead of pointing back to the same ones!
    scheduled_routes = lp_inputs['existing_route_data']['planned_routes'][:]
    all_routes_after_update = lp_inputs['existing_route_data']['planned_routes'][:]
    # includes utilization for both scheduled routes and existing routes that have been "un-scheduled"
    updated_utilization_by_route_id = existing_route_data['utilization_by_planned_route_id'].copy()
    dc_id_by_scheduled_rt_id = {}   # THIS ONLY INCLUDES NEW ROUTES CREATED!
    ############# ############# ############# ############# ############# #############

    ############# Build DataMultiRoute for new Downlink ############# 
    # now we have all the data needed to build the new sim objects (output as DataMultiRoutes)
    # extract data for new RouteObjectID
    planning_agent_id = lp_inputs['lp_instance_params']['lp_agent_ID']

    latest_dr_uid = lp_inputs['lp_instance_params']['latest_dr_uid']
    # Update failed downlink route utilization
    next_window_id = sat_schedule_arb.plan_db.sat_windows_dict['next_window_uid']
    # check: IF NOT a downlink then return, we currently assume only downlinks fail this way
    if not isinstance(failure_context['act'],DlnkWindow):
        print('Xlnk or Obs has failed! Current disruption planner not configured to recover, skipping recovery')
        print('Failed Activity:')
        print(failure_context['act'])
        lp_output = {
            'scheduled_routes':scheduled_routes,
            'all_routes_after_update':all_routes_after_update,
            'updated_utilization_by_route_id':updated_utilization_by_route_id,
            'latest_dr_uid':latest_dr_uid,
            'dc_id_by_scheduled_rt_id':dc_id_by_scheduled_rt_id,
            'next_window_uid': next_window_id  # No change
        }
        return lp_output

    
    
    scheduled_rt_ids = []
    # IF NEED TO ACCESS DC INDEX -> sat_schedule_arb.sim_executive_agent.exec._curr_dc_indx
    all_cur_dcs = sat_schedule_arb.state_sim.data_store.get_curr_data_conts()
    failed_dcs_list = []
    failed_dv_check = 0
    
    for SRC in context_SRCs:
        if SRC.data_vol <= 0: 
            continue
            # unused SRCs will have negative data volume
        dmrs_dict = SRC.dmrs_by_id
        for dmr_id in dmrs_dict.keys():
            all_simple_data_routes = dmrs_dict[dmr_id].data_routes
            for dr in all_simple_data_routes:
                dlnk_wind = dr.route[-1]
                if dlnk_wind == failure_context['act']:
                    # NOTE: if any of these have been updated by the LP, then the scheduled dv in the "data_routes"
                    # may not match the scheduled_dv in the scheduled_dv_by_dr, so need to update
                    dr.scheduled_dv = dmrs_dict[dmr_id].scheduled_dv_by_dr[dr]
                    #remake_simple_data_routes.append(dr) DEPRECATED
        for dc in all_cur_dcs:
            if SRC == dc.latest_planned_rt_cont:
                failed_dcs_list.append(dc)
                failed_dv_check += dc.data_vol
    # need to find data_conts to remove from data_store
    
    # the current method assumes that total dv_failed = sum of the amount in the failed_dcs list
    if failed_dv_check == 0:
        if len(context_SRCs) > 0:
            print('Failed SRC no longer has Data Container anyway, returnning')
            # This would happen when a satelite receives an update for a new route that contains a downlink, but has already passed the observation window for the obs
            print(SRC)
        else:
            print('No route container active during failure anyway')
        print('DV Failed Total: %d Mb' %dv_failed)
        lp_output = {
            'scheduled_routes':scheduled_routes,
            'all_routes_after_update':all_routes_after_update,
            'updated_utilization_by_route_id':updated_utilization_by_route_id,
            'latest_dr_uid':latest_dr_uid,
            'dc_id_by_scheduled_rt_id':dc_id_by_scheduled_rt_id,
            'next_window_uid': next_window_id # No change / not used
        }
        return lp_output

    if abs(dv_failed- failed_dv_check) <=  dv_eps_check*len(failed_dcs_list):
        pass

    # need to make a data structure that shows for each failed DC, how they are allocated to the new downlinks
    # TODO: there should be a priority mechanism for which DC is allocated "first"
    new_downlinks_dict = {} # keys are downlinks, values are a list of new_dcs
    # it is ONE downlink to 1..Many DCs, NOT the other way around.  One DataContainer can't have multiple downlinks!
    # while that is true, it is also 1 failed DC to potentially many new DMRs (will be split into new data containers upstream)
    # While this is technically the correct thing to do, forking takes place upstream (after LP return, so I shouldn't be forking here and only associate with previous data containers)
    remaining_dcs = failed_dcs_list[:]
    overflow_dv = 0 # overflow from the previous failed_dc
    for new_downlink in new_downlinks:
        remaining_dv = new_downlink[1]
        new_downlinks_dict[new_downlink] = []
        # go through and allocate all of the downlink DV to the failed data containers
        while remaining_dv >= dv_eps_check*len(failed_dcs_list):
            # pop data container off list since it will be fully allocated (others may be added)
            if overflow_dv == 0: 
                # then we can pop a new failed_DataContainer off the list
                failed_dc = remaining_dcs.pop(0)
                if calc_failed_dv_from_DCs:
                    dv_to_allocate = failed_dc.data_vol  
                else:
                    dv_to_allocate = failed_dc.data_vol if not failed_dc.latest_planned_rt_cont else failed_dc.latest_planned_rt_cont.data_vol # NOTE: we are allocating based on the last route container for the DC (if it exists), not the DC itself!
                # sometimes the data vol of a DC will be greater than its allocated route container (and vice versa)
                # I believe this occurs because the GP / LP is deciding not to dlnk all data in the DC because other things are higher priority in the current time horizon (such as injected obs)
            else:
                # otherwise, there was a data container that has overflow DV to go to a new downlink
                dv_to_allocate = overflow_dv

            if dv_to_allocate <= remaining_dv + dv_eps_check:
                # then the entirity of the DC can fit in the downlink still
                remaining_dv -= dv_to_allocate
                new_downlinks_dict[new_downlink].append((failed_dc,dv_to_allocate))
                overflow_dv = 0 # trigger to pop a new DC off the list
            else:
                # then the DC needs to be forked into one that fits in the remaining space and another one added to list
                # DC that will go into current downlink 
                new_downlinks_dict[new_downlink].append((failed_dc,remaining_dv))
                # DC that will go into next downlink on the list
                overflow_dv = dv_to_allocate - remaining_dv # overflows for the same data container to the next downlink
                remaining_dv = 0 # DV fully allocated now for this downlink
            
    
    if new_downlinks_found and total_dv_in_new_downlinks >= dv_failed:
        # Sanity check to make sure that there are no DCs remaining (only applies if new downlinks were actually found and the total DV available can handle all failed dv, otherwise can't deal with all failed DCs anyway)
        assert(len(remaining_dcs) == 0)
    self_replan_success = False
    # Now, we can use the downlinks dict to build the new downlinks and associate the right DCs with the new routes
    # loop through all of the "new_downlinks", make a single "downlink window" for them,  and make a new DMR for each unique datacontainer
    # One Downlink window can have multiple data routes in it!
    for new_downlink in new_downlinks:
        dlnk = new_downlink[0]
        new_dv_in_dlnk = new_downlink[1]
        duration_s = timedelta(SEC_TO_DAY*new_dv_in_dlnk / dlnk.ave_data_rate)

        if duration_s.total_seconds() < min_dlnk_duration:
            # don't make a new route for this downlink, it will be ignored by the GP
            new_downlinks_found = False if not self_replan_success else True # reset this flag since now not valid, unless we already have made new DMRs
            continue

        new_downlinks_found = True # reset again if we made it here
        if dlnk.scheduled_data_vol <= 0: # then this dlnk is not used
            dlnk.modified_by_LP = "Setup New Active Window and Scheduled DV, mod at: %s" %str(cur_time_dt)
            # set scheduled time around center
            datetime_start = dlnk.center - duration_s/2
        else: # currently scheduled
            # NOTE: can't just add directly because if it is not scheduled, then scheduled_data_vol = -999
            dlnk.modified_by_LP = "Increased Existing Active Window and Scheduled More DV, mod at: %s" %str(cur_time_dt)
            datetime_start = dlnk.start - duration_s/2

        # then  the cur_downlink was not in the current plan anyway, so we can use it 
        # (still might fail if other sats are using the GS at that time)
        # just need to update start and end times, but keep original values
        assert(datetime_start >= dlnk.original_start) # new start should always be after physical limit of window start
        dlnk.modify_time(new_dt = datetime_start) # this sets the data_vol property!
        
        # NOTE: the in place modification of the window changes the window itself, so it modified in the sat_windows_dict['dlnk_winds'] , but NOT the flat downlink winds, since it is a seperate deepcopy
        # this information will propagate to the ground station network later in the main sim loop
        # Note: some old routes still use the downlink
        dlnk.scheduled_data_vol = dlnk.data_vol # Mb
        # Update next_window_uid for the satellite, which it will communicate to the GS at the next pass
        sat_schedule_arb.plan_db.sat_windows_dict['next_window_uid'] = next_window_id

        # replace old downlink window with new downlink window in downlink_winds_flat as well
        ID_mod = dlnk.window_ID
        dlnk_to_replace = [x for x in sat_schedule_arb.plan_db.sat_windows_dict['dlnk_winds_flat'] if x.window_ID == ID_mod].pop()
        dlnk_to_replace_ind = sat_schedule_arb.plan_db.sat_windows_dict['dlnk_winds_flat'].index(dlnk_to_replace)
        sat_schedule_arb.plan_db.sat_windows_dict['dlnk_winds_flat'].remove(dlnk_to_replace)
        sat_schedule_arb.plan_db.sat_windows_dict['dlnk_winds_flat'].insert(dlnk_to_replace_ind,dlnk)

        # Make a new DataMultiRoute for each DataContainer associated with the downlink
        dcs_and_dv_in_cur_downlink = new_downlinks_dict[new_downlink] # this returns a list of tuples (failed_dc, DV to allocate for this DMR)
        for dc_and_dv in dcs_and_dv_in_cur_downlink:
            dc = dc_and_dv[0]
            # the data volume limit is already checked above when the new DCs were created
            dv_limit = min(dc_and_dv[1], dlnk.scheduled_data_vol) # sometimes dc_and_dv is larger by < 20 Mb (1 sec of downlinking)

            # this is probably the proper way to access the latest route container
            old_dmrs = list(dc.latest_planned_rt_cont.dmrs_by_id.values())
            # this code assumes that there is only 1 DMR per SRC
            assert(len(old_dmrs) == 1)
            old_drs = old_dmrs[0].data_routes
            # this code assumes that there is only 1 DR per DMR
            assert(len(old_drs) == 1)
            old_dr = old_drs[0]
            route = old_dr.route[:]
            # change out the old downlink
            route[-1] = dlnk

            window_start_sats = deepcopy(old_dr.window_start_sats)
            del window_start_sats[failure_context['act']] # delete the old downlink
            window_start_sats[dlnk] = sat_schedule_arb.sim_sat.index

            new_route = DataRoute( planning_agent_id, latest_dr_uid , route =route, window_start_sats=window_start_sats,dv=dv_limit)
            new_route.scheduled_dv = dv_limit
            # APPARENTLY, EACH DMR ONLY HAS ONE DATA ROUTE... (might be for this example case only)
            new_ro_ID = RoutingObjectID(planning_agent_id, latest_dr_uid )

            latest_dr_uid += 1   # this only increments for each new DMR, and we are only creating one new DMR (increment after new_ro_ID created!)
            fresh_DMR = DataMultiRoute(new_ro_ID,[new_route])
            drm_frac = dv_limit/fresh_DMR.data_vol
            fresh_DMR.set_scheduled_dv_frac(drm_frac) 
            dv_utilization = fresh_DMR.scheduled_dv / fresh_DMR.data_vol # this can be a float if only one DMR is passed (typical case)
            
            # validate new data route
            new_route.validate(act_timing_helper)

            assert(dv_utilization <= 1)
            # add to outputs
            scheduled_routes.append(fresh_DMR)
            all_routes_after_update.append(fresh_DMR)
            updated_utilization_by_route_id[fresh_DMR.ID] = dv_utilization
            scheduled_rt_ids.append(fresh_DMR.ID)
            # link to data container via executed_dr
            dc_id_by_scheduled_rt_id[fresh_DMR.ID] = dc.ID  # this dictionary means there is AT MOST 1 DC per DMR.  But two different DMRs could point to the same OLD DC (will be forked after returning)
            self_replan_success = True

    """ # remove last element of route history in failed DCs (failed downlink), since it didn't happen successfully
    # since I am no longer accessing the latest SRC via this method, shouldn't pop
    # NOTE: this is not done insight the loop because the same DC can appear in multiple downlink windows, so it will cause errors for the later windows
    for dc in failed_dcs_list:
        dc._planned_rt_hist.pop()  """  


    # either both should be true or both should be false
    assert(new_downlinks_found == self_replan_success)  

    if self_replan_success:
        print('planned downlinks developed into sim Route Containers')
        # end disruption state
        sat_schedule_arb.sim_sat.state_recorder.log_event(cur_time_dt,'lp_wrapper.py','LP v1 success','SRP recovered from %s failing by modifing these downlinks: %s' % (failure_context['act'], new_downlinks))
        sat_schedule_arb.latest_lp_route_indx = latest_dr_uid

    

    if not self_replan_success:
        print('self-Replan failed, intiating local neighborhood recovery')
        # initiate neighborhood replanning
        #you can definitely query the STN object to see if a current access between two objects exists, but you’d need to overlay that with the planning DB info if you want to work with bulk data routing plan (I assert the STN is sufficient to check for low rate plan sharing)
        # TODO: Implement neighborhood replan methods (BFS and DFS w/ two diff heuritics)
        

    if not neighborhood_replan_success and not self_replan_success:
        sat_schedule_arb.sim_sat.state_recorder.log_event(cur_time_dt,'lp_wrapper.py','LP v1 failure','Both SRP and neighborhood RP failed to recovery from %s failing' % failure_context['act'])
        print('Schedule disruption recovery failed, aborting replanning')
        # Need to package output in the following way:

    ''' lp_output - Dictionary Info:
    schedule_routes: list of DMRs for routes that have data scheduled
    all_routes_after_update: list of all DMRs, even if they now don't have schedule data
    updated_utilization_by_route_id: dictionary with the new utilization level for each DMR, this is the same as 
        calling .get_sched_utilization() on each of the DMRs in all_routes_after_update
    latest_lp_route_indx: the new minimum index for any new local planner routes for this satellite
        this is equal to the index of the new route created + 1
    dc_id_by_scheduled_rt_id: dictionary keys as the route id and data containers as the values for new scheduled routes only
    '''
    lp_output = {
        'scheduled_routes':scheduled_routes,
        'all_routes_after_update':all_routes_after_update,
        'updated_utilization_by_route_id':updated_utilization_by_route_id,
        'latest_dr_uid':latest_dr_uid,
        'dc_id_by_scheduled_rt_id':dc_id_by_scheduled_rt_id,
        'next_window_uid': next_window_id # not used in return
    }
    return lp_output

# HELPER FUNCTIONS FOR SELF-REPLANNER
SEC_TO_DAY = 1/(24*60*60)
def get_planned_data_state(sat_schedule_arb,future_planned_acts,cur_time_dt,sim_base_time,t_step):
    # (TODO: move this (DS_data_planned generation) to the state_sim, should be known as soon as schedule is known)
    # Get remaining data
    cur_data_remaining = sat_schedule_arb.state_sim.DS_max - sat_schedule_arb.state_sim.DS_state
    cur_steps = int((cur_time_dt - sim_base_time).seconds / t_step ) # maybe +1?
    total_steps =int((sat_schedule_arb.sim_sat.sim_end_dt- sim_base_time).seconds / t_step )
    rem_steps = total_steps - cur_steps

    DS_state_temp = sat_schedule_arb.state_sim.state_recorder.DS_state_hist[:]
    DS_data_planned = DS_state_temp[:-1] + [(DS_state_temp[-1][0]+t_step*idx,DS_state_temp[-1][1]) for idx in range(rem_steps+1)]
    # convert to lists since we are mutating elements below
    DS_data_planned = [list(el) for el in DS_data_planned]
    # just do a rough step function, where planned data levels increments 
    for future_act in future_planned_acts:
        start_ind = int((future_act.start - sim_base_time).seconds / t_step )
        if type(future_act) == XlnkWindow:
            # add or decrease data depending on rx sat or tx sat
            if sat_schedule_arb.sim_sat.index == future_act.tx_sat: # then we are transmitting
                # decrease data at start time until end 
                for idx in range(start_ind, total_steps):
                    DS_data_planned[idx][1] -= future_act.executable_data_vol
            else: # we are receiving
                for idx in range(start_ind, total_steps):
                    DS_data_planned[idx][1] += future_act.executable_data_vol
        elif type(future_act) == DlnkWindow:
            # decrease data at start time until end 
            for idx in range(start_ind, total_steps):
                DS_data_planned[idx][1] -= future_act.executable_data_vol
        elif type(future_act) == ObsWindow:
            # increase data at start
            for idx in range(start_ind, total_steps):
                DS_data_planned[idx][1] += future_act.executable_data_vol

    # convert DS_data_planned into mjd units
    start_mjd = time_tools.datetime2mjd(sim_base_time)

    DS_data_planned_mjd = []
    for idx,_ in enumerate(DS_data_planned):
        DS_data_planned_mjd.append((start_mjd+t_step*SEC_TO_DAY*idx,DS_data_planned[idx][1]))

    return DS_data_planned_mjd

def get_candidate_downlink_windows(potential_downlink_winds,future_planned_acts,plan_start_time_dt,self_replan_horizon_dt, min_dlnk_duration ):
    '''
    returns a dictionary with gs_IDs as keys that has a list of tuples: (downlink_window, earliest start, latest end, available_DV)
    THIS CODE ASSUMES WE ARE LOOKING FOR ACTIVE WINDOWS CENTERED ON PHYSICAL WINDOWS
    '''
    valid_candidate_windows = {}
    for gs_indx,gs_list in enumerate(potential_downlink_winds):
        # try to schedule into this window 
        valid_candidate_windows[gs_indx] = []
        for dlnk in gs_list:
            data_rate = dlnk.ave_data_rate
            window = (dlnk.original_start, dlnk.original_end)
            available_DV = dlnk.original_data_vol # this will be overwritten & lowered if an existing downlink already has data scheduled
            if plan_start_time_dt > window[1]:
                # check if plan start time is after window has ended
                continue 
            elif self_replan_horizon_dt < window[0]:
                # check if planning horizon ends before window starts
                continue
            else:
                if plan_start_time_dt < window[0] and self_replan_horizon_dt > window[1]:
                    # window starts after plan starts and ends before plan ends
                    candidate_window = window
                elif plan_start_time_dt > window[0] and self_replan_horizon_dt > window[1]:
                    # window starts before plan starts, but still ends before plan ends 
                    # change window start time to planning window start time
                    # Need to keep window centered on original center of physical window
                    center_delta = dlnk.center - plan_start_time_dt
                    candidate_window = (plan_start_time_dt,dlnk.center + center_delta)
                elif plan_start_time_dt < window[0] and self_replan_horizon_dt < window[1]:
                    # window starts after plan starts, ends after plan ends
                    # change window end time to planning end time
                    center_delta = self_replan_horizon_dt - dlnk.center
                    candidate_window = (dlnk.center - center_delta,self_replan_horizon_dt)
                else:
                    # window starts before plan starts and ends after plan ends
                    # need to recenter:
                    start_center_delta = dlnk.center - plan_start_time_dt
                    end_center_delta = self_replan_horizon_dt - dlnk.center

                    if start_center_delta > end_center_delta:
                        # more time at the front, move to match end
                        candidate_window = (dlnk.center - end_center_delta, self_replan_horizon_dt)
                    else:
                        # more time at the back, move to match front
                        candidate_window = (plan_start_time_dt, dlnk.center + start_center_delta)

                available_DV = data_rate*(candidate_window[1] - candidate_window[0]).total_seconds()
                # TIME AVAILABILITY CHECK -- need to check candidate window, it could be already scheduled, check future_planned_acts
                candidate_valid = True
                for future_act in future_planned_acts:
                    # need to be clear of actions at this time
                    act_dt_window = (future_act.start, future_act.end)
                    if candidate_window[1] < act_dt_window[0] or candidate_window[0] > act_dt_window[1]:
                        # ends before other activity starts or starts after other activity ends  
                        pass
                        # TODO: implement partial overlap check and move candidate window to non-overlapped area
                    else:
                        if type(future_act) == DlnkWindow:
                            # new downlink could extend the existing downlink activity
                            available_DV = future_act.original_data_vol - future_act.scheduled_data_vol
                            
                        else: 
                            # obs or xlnk occuring, don't schedule new downlink
                            candidate_valid = False
                
                # MINIMUM DURATION CHECK
                window_duration_s = (candidate_window[1] - candidate_window[0]).total_seconds()
                # Also Do a min DV check as well to remove very incrementing only a small amount
                min_DV = data_rate * min_dlnk_duration
                if window_duration_s < min_dlnk_duration or available_DV < min_DV :
                    candidate_valid = False

                if candidate_valid:
                    valid_candidate_windows[gs_indx].append((dlnk,candidate_window[0],candidate_window[1],available_DV))
    
    return valid_candidate_windows

def get_replanned_downlinks(valid_candidate_windows,dv_failed,rule, dv_eps=0.1):
    if rule == 'greedy':
        # change dict into a list of tuples, sorted by window start time, with 3rd lement the GS.ID
        sorted_windows = []
        for gs_indx in valid_candidate_windows.keys():
            for window in valid_candidate_windows[gs_indx]:
                sorted_windows.append(window)
        
        sorted_windows = sorted(sorted_windows,key=lambda x: x[1]) # sort by earliest start time
    else:
        raise NotImplementedError('Only greedy sorting of new dlnks remaining')

    remaining_dv = dv_failed
    
    planned_downlinks = []
    # list of tuples with (mjd_start, mjd_end, dv, gs_ID)
    for window in sorted_windows:
        dlnk = window[0]
        dlnk_rate = dlnk.ave_data_rate

        next_dv = min(window[-1],remaining_dv) # last element of the window contains max available dv through that downlink

        planned_downlinks.append((window[0],next_dv))

        remaining_dv -= next_dv

        if remaining_dv <= dv_eps:
            break

    return planned_downlinks
