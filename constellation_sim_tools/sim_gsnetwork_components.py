#  Contains objects for different software components running on  ground stations and the ground station network
#
# @author Kit Kennedy

from collections import namedtuple
from copy import deepcopy

from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import Executive,PlannerScheduler,StateRecorder
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools

import pickle as pkl
import time

from central_global_planner.IP_Client import IP_Client

class GroundNetworkPS(PlannerScheduler):
    """Handles calling of the GP, and ingestion of plan and state updates from satellites"""

    def __init__(self,sim_gsn,sim_start_dt,sim_end_dt,gsn_ps_params,act_timing_helper,simulation):
        #  initialize from superclass first
        super().__init__(sim_start_dt,sim_end_dt,sim_gsn,act_timing_helper)

        # holds ref to the containing sim ground network
        self.sim_gsn = sim_gsn

        #  time to wait since last plans were released before rerunning planning
        self.replan_interval_s = gsn_ps_params['replan_interval_s']


        # This keeps track of the latest data route index created. (along with the agent ID,
        # the DR "uid" in the GP algorithm)
        self.latest_gp_route_indx = 0

        # holds ref to GroundNetworkStateRecorder
        self.state_recorder = None

        # see superclass for documentation
        self.replan_release_wait_time_s = gsn_ps_params['replan_release_wait_time_s']
        self.release_first_plans_immediately = gsn_ps_params['release_first_plans_immediately']

        # Set on first iteration (not upon initilization)
        self.all_windows_dict = {} # holds all activity windows over the sim duration

        self.outsource = simulation.params['sim_gen_config']['use_standalone_gp']  # False
        if self.outsource:
            # default removed server is in localhost, else follow IP address
            address = simulation.params['sim_gen_config']['rem_gp_server_address'] or "localhost"
            
            self.gp_client = IP_Client({"cgp":address},54202,".")  # Perhaps if connection fails,
                                                                   # attempt to spin one up here.

            scenario_params = {
                'sim_case_config'   : { 
                    'scenario_params' : simulation.params['sim_case_config'],
                    'sim_gsn_ID' : self.sim_gsn.ID
                },
                'orbit_prop_params' : simulation.params['orbit_prop_params'],
                'orbit_link_params' : simulation.params['orbit_link_params'],
                'data_rates_params' : simulation.params['data_rates_params'],
                'sim_run_params'    : {
                    "start_utc_dt" : simulation.params['start_utc_dt'],
                    "end_utc_dt"   : simulation.params['end_utc_dt'] 
                },
                'sat_config'        : simulation.params['sat_config'],
                'sim_gen_config'    : simulation.params['sim_gen_config'],
                'gp_general_params' : simulation.params['gp_general_params']
            }
            scenario_params['orbit_prop_params']['gs_params']['elevation_cutoff_deg'] = \
                simulation.const_sim_inst_params["sim_gs_params"]["elevation_cutoff_deg"]

            msg = {
                "req_type" : 'updateParams',
                "payload"  : {
                    'scenario_params' : scenario_params             # To start based on what is loaded here
                }
            }

            self.gp_client.transmit( "cgp", pkl.dumps(msg) )  # TODO - get ack, else error?

            # Same pattern as requesting plan, but just the propagation & windows in this case.
            rec_str = self.gp_client.transmit( "cgp", pkl.dumps(
                {
                    "req_type" : 'updateWindows',
                    "payload" : None
                }
            ) )
            dta = pkl.loads(rec_str)
            while ( dta['payload'] is None): # 5 seconds to kick off a plan gen  -- maybe add a max as well--60s?
                time.sleep(1)
                print('.',end='',flush=True)
                rec_str = self.gp_client.transmit( "cgp", pkl.dumps(
                    {
                        "req_type" : 'updateWindows',
                        "payload" : None
                    }
                ) )

                dta = pkl.loads(rec_str)

            self.all_windows_dict = dta['payload'] # ['all_windows_dict']

    @property
    def last_replan_time_dt(self):
        return self._last_replan_time_dt
    

    def _check_internal_planning_update_req(self):

         #  see if we need to replan at this time step
        # add in consideration for lots of received updated planning information from satellites causing a need to
         # rerun global planner?
        replan_required = False
        if self._last_replan_time_dt is None:
            replan_required = True
        elif (self._curr_time_dt - self._last_replan_time_dt).total_seconds() >= self.replan_interval_s:
            replan_required = True
        #  if we already have plans waiting to be released
        if len(self._replan_release_q) > 0:
            replan_required = False

        return (replan_required, 'nominal')

    def _schedule_cache_update(self):
        #  don't need to do anything with generating a schedule cache because the ground station network is
        #  not an executive planner ( it doesn't execute activities)
        pass

    def retrieve_updated_all_windows_dict(self):  # TODO, add second param to allow for LP feedback to windows
        if self.outsource:
            rec_str = self.gp_client.transmit( "cgp", pkl.dumps(
                {
                    "req_type" : 'updateWindows',
                    "payload" : None
                }
            ) )

            dta = pkl.loads(rec_str)

            while ( dta['payload']['pending'] or dta['payload']['all_windows_dict'] is None): # 5 seconds to kick off a
                # plan gen  -- maybe add a max as well--60s?
                time.sleep(1)
                print('.',end='',flush=True)
                rec_str = self.gp_client.transmit( "cgp", pkl.dumps(
                    {
                        "req_type" : 'updateWindows',
                        "payload" : None
                    }
                ) )

                dta = pkl.loads(rec_str)

            self.all_windows_dict = dta['payload']['all_windows_dict']


    def _run_planner(self,gp_wrapper,new_time_dt):
        #  see superclass for docs

        #  get already existing sim route containers that need to be fed to the global planner
        existing_rt_conts = self.plan_db.get_filtered_sim_routes(self._curr_time_dt,filter_opt='partially_within')

        #  get the satellite states at the beginning of the planning window
        sat_state_by_id = self.plan_db.get_sat_states(self._curr_time_dt)

        #  run the global planner
        # debug_tools.debug_breakpt()

        if self.outsource:

            msg = {
                "req_type" : 'regenPlan',
                "payload"  : 
                    [
                        self._curr_time_dt         
                    ]
            }

            rec_str = self.gp_client.transmit( "cgp", pkl.dumps(msg) )

            dta = pkl.loads(rec_str)
            while ( dta['payload']['pending'] or dta['payload']['plan'] is None): # 5 seconds to kick off a plan gen
                # -- maybe add a max as well--60s?
                time.sleep(1)
                print('.',end='',flush=True)
                rec_str = self.gp_client.transmit( "cgp", pkl.dumps(
                    {
                        "req_type" : 'reqPlan',
                        "payload"  : None
                    }
                ) )

                dta = pkl.loads(rec_str)

            # if dta payload is not none, but it should not be none
            new_rt_conts         = dta['payload']['plan'][0]
            latest_gp_route_indx = dta['payload']['plan'][1]

        else:
            new_rt_conts, latest_gp_route_indx = gp_wrapper.run_gp(self._curr_time_dt,existing_rt_conts,self.sim_gsn.ID,
                                                                   self.latest_gp_route_indx,sat_state_by_id,
                                                                   self.all_windows_dict)


        
        #  I figure this can be done immediately and it's okay -  immediately updating the latest route index
        #  shouldn't be bad. todo:  confirm this is okay
        self.latest_gp_route_indx = latest_gp_route_indx

        return new_rt_conts

    def _process_updated_routes(self,rt_conts,update_time_dt):
        #  see superclass for docs

        #  mark all of the route containers with their release time
        for rt_cont in rt_conts:
            rt_cont.set_times_safe(update_time_dt)

        # update plan database
        self.plan_db.update_routes(rt_conts,update_time_dt)

        #  save off the executable activities seen by the global planner so they can be looked at at the end of the sim
        #  filter rationale: want any route containers that have at least one window overlapping past start time
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within')

        # distill the activities out of the route containers
        #  filter rationale:  only want windows that are completely past the start time, because we don't want to
        #  update our planned activity history with activities from the past
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,
                                                     filter_opt='totally_within',
                                                     act_timing_helper=self.act_timing_helper)

        # update any acts that have changed within our plans (note: activity plans CAN change up to right
        # before they get executed, though the GP is incentivized to keep planned activities the same once it
        # chooses them)
        for exec_act in executable_acts:        
            self.state_recorder.add_planned_act_hist(exec_act.act)


class GroundNetworkStateRecorder(StateRecorder):
    """Convenient interface for storing state history for ground network"""
    # todo: is "state" the right word here?

    def __init__(self,sim_start_dt,num_sats,num_gs):
        # store all these activities in a dictionary indexed by the ID, so that we can update activities later
        self.act_hist_by_wind_id = {}
        self.num_sats = num_sats
        self.num_gs = num_gs

        super().__init__(sim_start_dt)    

    def add_planned_act_hist(self,act):
        #  note: implicitly update any entry that might've been in this dictionary before for this activity
        # deepcopy the act though so that no changes to any of its attributes elsewhere can affect this history
        # (though it can still be updatd by calling this function again)
        self.act_hist_by_wind_id[act.window_ID] = deepcopy(act)

    def get_all_sats_planned_act_hists(self):
        # all_sats_act_hists = []
        sats_obs = [[] for indx in range(self.num_sats)]
        sats_dlnks = [[] for indx in range(self.num_sats)]
        sats_xlnks = [[] for indx in range(self.num_sats)]

        for act in self.act_hist_by_wind_id.values():
            if type(act) == ObsWindow:
                sats_obs[act.sat_indx].append(act)
            elif type(act) == DlnkWindow:
                sats_dlnks[act.sat_indx].append(act)
            elif type(act) == XlnkWindow:
                sats_xlnks[act.sat_indx].append(act)
                sats_xlnks[act.xsat_indx].append(act)

        for sat_indx in range(self.num_sats):
            sats_obs[sat_indx].sort(key= lambda wind: wind.start)
            sats_dlnks[sat_indx].sort(key= lambda wind: wind.start)
            sats_xlnks[sat_indx].sort(key= lambda wind: wind.start)

        return sats_obs,sats_dlnks,sats_xlnks

    def get_all_gs_planned_act_hists(self):
        gs_dlnks = [[] for indx in range(self.num_gs)]

        for act in self.act_hist_by_wind_id.values():
            if type(act) == DlnkWindow:
                gs_dlnks[act.gs_indx].append(act)

        for gs_indx in range(self.num_gs):
            gs_dlnks[gs_indx].sort(key= lambda wind: wind.start)

        return gs_dlnks

