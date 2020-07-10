#  Contains objects for different software components running on a satellite.  these components handle schedule arbitration, activity execution decision-making, and state simulation
#
# @author Kit Kennedy
#
# Notes:
# The intended flow of update steps for these components is:
# 1. state simulator
# 2. schedule arbiter
# 3. executive
# It's ultimately a design decision which one comes first, but the logic of this order is following: first the satellite figures out what state it is in, then it figures out what it's supposed to do (its schedule), then it figures out what it's actually going to do (executive). We update the internal clocks on each of these components in that order. if the order of update steps is changed, you'll  have to go in and muss with the internals of the steps to make sure they're all still consistent.
#
# A note on the state update steps: state is first updated using the current internal time, and then the current time is updated to the new (input) time.

from random import normalvariate
from collections import namedtuple

from circinus_tools  import io_tools
from circinus_tools.scheduling.base_window  import find_windows_in_wind_list
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import StateSimulator,Executive,ExecutiveAgentPlannerScheduler,ExecutiveAgentStateRecorder,DataStore
from .sim_routing_objects import SimDataContainer,ExecutableDataContainer
from .schedule_tools  import synthesize_executable_acts, ExecutableActivity
from .lp_wrapper import LocalPlannerWrapper 
from sprint_tools.Sprint_Types import AgentType #from .sim_agents import AgentType

from circinus_tools import debug_tools
import copy
import logging 

class SatStateSimulator(StateSimulator):
    """Simulates satellite system state, holding internally any state variables needed for the process. This state includes things like energy storage, ADCS modes/pointing state (future work)"""

    def __init__(self,sim_sat,sim_start_dt,state_simulator_params,sat_power_params,sat_data_storage_params,sat_initial_state,dv_epsilon=0.01):
        #  should probably add more robust physical units checking
        super().__init__(sim_sat)

        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        ################
        #  Data storage stuff

        #  note that data generated is not really simulated here; the state simulator just provides a coherent API for storage ( the executive handles data generation)

        #  data storage is assumed to be in Mb
        #  assume we start out with zero data stored on board
        self.DS_state = 0
        self.DS_max = sat_data_storage_params['d_max'] * 1000  # convert to megabits
        # note:  assuming no minimum data storage limit - it's just 0!
        self.DS_min = 0

        ################
        #  energy storage stuff

        self.ES_state = sat_initial_state['batt_e_Wh']

        self.sat_edot_by_mode,self.sat_batt_storage,power_units,_,_ = io_tools.parse_power_consumption_params(sat_power_params)

        if not power_units['power_consumption'] == 'W':
            raise NotImplementedError
        if not power_units['battery_storage'] == 'Wh':
            raise NotImplementedError

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        self.es_update_add_noise = state_simulator_params['es_state_update']['add_noise']
        self.es_noise_params = state_simulator_params['es_state_update']['noise_params']

        # we track current eclipse window index here because they're not actually scheduled, they're just events that happen
        self._curr_ecl_windex = 0

        # holds ref to SatExecutive
        self.sat_exec = None
        # holds ref to SatStateRecorder
        self.state_recorder = None
        # holds ref to SatDataStore
        # note that we include the data_store in the simulator because we may want, in future, to simulate data corruptions or various other time dependent phenomena in the data store. So, we wrap it within this sim layer. Also it's convenient to be able to store DS_state here alongside ES_state
        self.data_store = DataStore()

        # the "effectively zero" data volume number
        self.dv_epsilon = dv_epsilon # Mb

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

    def update(self,new_time_dt):
        """ Update state to new time by propagating state forward from last time to new time. Note that we use state at self._curr_time_dt to propagate forward to new_time_dt"""

        # If first step, just record state then return
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self.state_recorder.add_ES_hist(self._curr_time_dt,self.ES_state)
            self.state_recorder.add_DS_hist(self._curr_time_dt,self.DS_state)
            self._first_step = False
            return

        # this is consistent with power units above
        delta_t_h = (new_time_dt - self._curr_time_dt).total_seconds()/3600

        current_acts = self.sat_exec.get_acts_at_time(self._curr_time_dt)


        ##############################
        # Energy storage update

        act_edot = 0
        for act in current_acts:
            sat_indx = self.sim_sat.sat_indx
            act_edot = self.sat_edot_by_mode[act.get_e_dot_codename(sat_indx)]

        #  base-level satellite energy usage (not including additional activities)
        base_edot = self.sat_edot_by_mode['base']

        #  check if we're in eclipse in which case were not charging
        charging = True
        if self.in_eclipse(self._curr_time_dt):
            charging = False

        # add in charging energy contribution (if present)
        # assume charging is constant in sunlight
        charging_edot = self.sat_edot_by_mode['orbit_insunlight_average_charging'] if charging else 0

        noise_mult = 1.0
        # update states based on resource usage rate in current activity
        if self.es_update_add_noise:
            if self.es_noise_params['noise_type'] == 'fractional_normal_edot': 
                noise_mult = normalvariate(self.es_noise_params['average'], self.es_noise_params['std'])
                noise_mult = min(noise_mult, self.es_noise_params['max'])
                noise_mult = max(noise_mult, self.es_noise_params['min'])
            else:
                raise NotImplementedError
            
        self.ES_state =  (base_edot + charging_edot + act_edot) * delta_t_h * noise_mult + self.ES_state

        # deal with cases where charging and/or noise puts us above max batt storage
        self.ES_state = min(self.ES_state,self.sat_batt_storage['e_max'])

        # note: don't treat min battery storage the same way - but do raise warning if less than 0...
        if self.ES_state < 0:
            raise RuntimeWarning('ES_state went below 0 for sat %s'%(self))


        self._curr_time_dt = new_time_dt

        ##############################
        # update state recorder
    
        self.state_recorder.add_ES_hist(self._curr_time_dt,self.ES_state)
        self.state_recorder.add_DS_hist(self._curr_time_dt,self.DS_state)

        # todo: add whatever else needed

    def nominal_state_check(self):
        # if we're below lower energy bound, state is off nominal
        if self.ES_state < self.sat_batt_storage['e_min']:
            return False

        return True

    def in_eclipse(self,time_dt):
        ecl_winds = self.sim_sat.get_ecl_winds()
        curr_ecl_winds,ecl_windices = find_windows_in_wind_list(time_dt,self._curr_ecl_windex,ecl_winds)
        self._curr_ecl_windex = ecl_windices[1]

        if len(curr_ecl_winds) > 1:
            raise RuntimeWarning('Found more than one valid eclipse window at current time')

        if len(curr_ecl_winds) == 1:
            return True
        else:
            return False

    def get_available_data_storage(self,time_dt,dv_desired=None):
        # note: for the satellite we don't care about dv_desired

        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to get available data volume from state sim off-timestep')

        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        # TODO: so dv_epsilon for creating a new data container is a 1 Mb threshold, but the dv_epislon for validating 
        # data storage against collected data containers is a 0.01 Mb threshold.  
        # I'm going to raise the data storage threshold up to 0.1 Mb, but I think an overall 
        # thresholding re-work is in order, maybe something that scales with sim timescale
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon*10)

        return self.DS_max - self.DS_state

    def update_data_storage(self,delta_dv,data_conts,time_dt):
        """ add an amount of data volume to data storage state"""

        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to add data volume to state sim off-timestep')

        # add any data containers that the data store doesn't know about yet
        self.data_store.add(data_conts)

        new_DS_state = self.DS_state + delta_dv

        if new_DS_state > self.DS_max + self.dv_epsilon:
            raise RuntimeWarning('Attempting to add more to data storage than than is available. Current DS state: %f, delta dv %f, DS  max: %f'%(self.DS_state,delta_dv,self.DS_max))
        if new_DS_state < self.DS_min - self.dv_epsilon:
            raise RuntimeWarning('Attempting to go below minimum data storage limit. Current DS state: %f, delta dv %f, DS min: %f'%(self.DS_state,delta_dv,self.DS_min))

        self.DS_state = new_DS_state

        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        # TODO: so dv_epsilon for creating a new data container is a 1 Mb threshold, but the dv_epislon for validating 
        # data storage against collected data containers is a 0.01 Mb threshold.  
        # I'm going to raise the data storage threshold up to 0.1 Mb, but I think an overall 
        # thresholding re-work is in order, maybe something that scales with sim timescale
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon*10)

    def cleanup_empty_data_conts(self,data_conts,time_dt,dv_epsilon=0.001):
        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to cleanup data conts in state sim off-timestep')

        dv_dropped = self.data_store.remove_empty_dcs(data_conts,dv_epsilon)
        self.DS_state -= dv_dropped

    def remove_stale_data_conts(self,exec_act,time_dt,dv_epsilon=0.001):
        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to cleanup data conts in state sim off-timestep')

        for dc in self.data_store.get_curr_data_conts():
            if dc.is_stale(time_dt):
                self.data_store.drop_dc(dc)
                self.DS_state -= dc.data_vol
                self.state_recorder.log_event(self._curr_time_dt,'sim_sat_components.py','data cont drop','dc has become stale, dropping: %s, latest_planned_rt_cont: %s. Exec act being cleaned up: %s'%(dc,dc.latest_planned_rt_cont,exec_act))


class SatScheduleArbiter(ExecutiveAgentPlannerScheduler):
    """Handles ingestion of new schedule artifacts from ground planner and their deconfliction with onboard updates. Calls the LP"""

    def __init__(self,sim_sat,sim_start_dt,sim_end_dt,sat_arbiter_params,act_timing_helper):
        super().__init__(sim_sat,sim_start_dt,sim_end_dt,act_timing_helper)

        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        self.state_sim = None

        #  used to indicate the scheduler that the local planner needs to be run
        # self.lp_replan_required = False

        # This keeps track of the latest data route index created. (along with the agent ID, the DR "uid" in the LP algorithm)
        self.latest_lp_route_indx = 0

        # indicates that an activity was injected in the executive
        self.act_was_injected = False

        # see superclass for documentation
        self.replan_release_wait_time_s = sat_arbiter_params['replan_release_wait_time_s']

        #  can be used to disable running of the LP 
        # TODO: curently sat_arbiter_params is specified in the satellite model, should move somewhere else
        self.allow_lp_execution = sat_arbiter_params['allow_lp_execution'] 

        #  time to wait since last plans were released before rerunning planning
        self.replan_interval_s = sat_arbiter_params['replan_interval_s']

        # threshold for activity failur - triggers schedule disruption replanner (TODO: should this be a property of the exec instead?)
        self.frac_dv_lost_for_activity_failure_threshold = sat_arbiter_params["frac_dv_lost_for_activity_failure_threshold"]
        # override the None that this was initialized to in superclass
        self._last_replan_time_dt = sim_start_dt

        # Addd state for schedule disruption (failed activity execution)
        self.schedule_disruption_occurred = False
        self.schedule_disruption_context = None
        self.schedule_disruption_replan_communicated = True
        self.SRP_runs = 0 # counter for number of times the SRP ran
        self.SRP_successes = 0 # counter for number of times the SRP was successful
        self.SRP_matches_MILP = 0 # counter for number of times the SRP matches the milp solution

        # satellites to which we've shared the current consensed plan
        self.cur_sats_propped_to = []   # only the latest planning info, refreshed once planning info updated
        sats_propped_to = {}
        for sat_id in self.sim_sat.GlobalSim.sat_id_order:
            sats_propped_to[sat_id] = [] 
        self.sats_propped_to = sats_propped_to

    def _check_internal_planning_update_req(self):

        # todo: update this code

         #  only run the local planner if it's been flagged as needing to be run. reasons for this:
        # 1.  executive has executed an "injected activity" and we need to re-plan to deal with the effects of that activity (e.g. observation data was collected and there is no plan currently to get it to ground)
        # 2.  the scheduled activities were not executed in the way expected (e.g. activity was canceled) - NOT YET IMPLEMENTED
        # 3.  sufficient time has passed since last run. (good to run ever so often because might have run before and not been able to route any injected activity data due to no outflows)

        # todo: do we need this check?
        # if self._planning_info_updated_external:
        #     ...

        replan_required = False
        replan_type = 'nominal'
        if self.act_was_injected:
            replan_required = True

        
        # todo: removed for now. should add back in sometime
        # if (self._curr_time_dt - self._last_replan_time_dt).total_seconds() >= self.replan_interval_s:
        #     replan_required = True

        # note that the below conditions trump the other ones

        #  if we already have plans waiting to be released
        if len(self._replan_release_q) > 0:
            replan_required = False

        if not self.allow_lp_execution:
            replan_required = False

        # TODO: Generalize mods here into LP
        # this test implementation assumes that allow_lp_execution has been set to False, so only way this will be true is from schedule disruptions
        # TODO: this sets priority of replanning to schedule disruptions, should be able to run both
        if self.schedule_disruption_occurred:
            replan_required = True
            replan_type = 'disruption'

        return (replan_required,replan_type)

    def _clear_replan_reqs(self):
        """Clear all of the flags that require a replan"""

        # note:  this gets called immediately after running the local planner, even if we still need to wait to release the plans from the Q. so it is possible that these flags will be set high again before the plans are even released -  that should be fine,  we will just replan again when we are able to
        # todo:  verify the above logic


        self.act_was_injected = False

    def get_executable_acts(self):
        #  get relevant sim route containers for deriving a schedule
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within',sat_id=self.sim_sat.sat_id)

        #  synthesizes the list of unique activities to execute, with the correct execution times and data volumes on them
        #  the list elements are of type ExecutableActivity
        #  filter rationale:  we may be in the middle of executing an activity, and we want to preserve the fact that that activity is in the schedule. so if a window is partially for current time, but ends after current time, we still want to consider it an executable act
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,filter_opt='partially_within',sat_indx=self.sim_sat.sat_indx,act_timing_helper=self.act_timing_helper)

        # TODO: functionalize this - get_impossible_acts
        # get past actions that the satellite has completed:
        act_hist_dict = self.sim_sat.state_recorder.get_act_hist()
        past_obs = act_hist_dict['obs']
        past_xlnks = act_hist_dict['xlnk']
        past_dlnks = act_hist_dict['dlnk']
        all_past_acts =past_obs + past_xlnks + past_dlnks
        wind_frac_failed = {} # for tracking upstream failures
        # Implement a "failed_dict" that contains a list actions that failed / will fail for the following keys
        # ['Planning info received after action end time'] : (self-explanatory)
        # [Action relies on another action that failed']: (for upcoming crosslinks and downlinks that are no longer physically possible because their routes have broken upstream dependencies in the first field)
        for rt_cont in rt_conts:
            for dmr in rt_cont.dmrs_by_id.values():
                all_winds_in_dmr = list(dmr.get_winds())
                # need to sort winds in chronological order for logic below to work
                all_winds_in_dmr.sort(key=lambda x: x.end)
                upstream_wind_failed = False
                for wind in all_winds_in_dmr:
                    # only care about windows that have this satellite as a participant
                    if wind.sat_indx != self.sim_sat.index:
                        continue
                    if isinstance(wind, XlnkWindow):
                        if wind.tx_sat != self.sim_sat.index and wind.rx_sat != self.sim_sat.index:
                            continue
                    
                    if upstream_wind_failed:
                        # all remaining winds 
                        # ONLY add to failed list if the data volume is 100% accounted for by the DMR, otherwise need to check other routes that use this window
                        
                        # note: sometimes the windows have 0 scheduled dv
                        if wind.scheduled_data_vol > 0:
                            if wind not in wind_frac_failed:
                                wind_frac_failed[wind] = dmr.scheduled_dv / wind.scheduled_data_vol
                            else:
                                wind_frac_failed[wind] += dmr.scheduled_dv / wind.scheduled_data_vol

                    if wind.end < self._curr_time_dt and wind not in all_past_acts:
                        self.sim_sat.state_recorder.failed_dict['non-exec']['Planning info received after action end time'].add(wind)
                        upstream_wind_failed = True
                        # Note: an act could appear here AND also fail because the upstream window(s) failed
                        
        
        # check wind_frac_failed dict
        for wind in wind_frac_failed.keys():
            if wind_frac_failed[wind] >= .99:
                # add to failure dictionary
                self.sim_sat.state_recorder.failed_dict['non-exec']['Action relies on another action that occurs in the past'].add(wind)
                
                # remove from executable acts
                # TODO: it is debateable if we should do this, or if we leave it as an executable act with no data volume flowing across,
                # we should add logic to allow the satellite to send other data containers over the downlink if the planned data containers don't exist
                # see issue #13 on SPRINT repo
                if wind in executable_acts:
                    executable_acts.remove(wind)

        return executable_acts

    def _run_planner(self,lp_wrapper,new_time_dt,replan_type = 'nominal'):
        #  see superclass for docs

        # first clean out any basically empty routes - don't want to consider them in planning
        self.state_sim.cleanup_empty_data_conts(self.state_sim.get_curr_data_conts(),new_time_dt,dv_epsilon= lp_wrapper.lp_dv_epsilon)

        #  get current data containers for planning
        existing_data_conts = self.state_sim.get_curr_data_conts()
        existing_data_conts_by_id = {dc.ID:dc for dc in existing_data_conts}

        # update the latest planned rt conts for each dc, because the GP may have changed their utilization (or another LP or something)
        for dc in existing_data_conts:
            if dc.latest_planned_rt_cont is None:
                continue

            dc_rt_cont_id = dc.latest_planned_rt_cont.ID
            rt_conts_updated = self.plan_db.get_filtered_sim_routes(specified_src_ids=[dc_rt_cont_id])
            assert(len(rt_conts_updated) == 1)
            dc.add_to_plan_hist(rt_conts_updated[0])


        #  get relevant sim route containers for planning
        # todo:  probably need to do a little bit more work to preprocess these route containers -  to provide updated utilization numbers.  not sure where this is best done -  maybe in the executive?
        existing_rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within',sat_id=self.sim_sat.sat_id)

        # todo: add getting of sat state
        #  get the satellite states at the beginning of the planning window
        # sat_state_by_id = self.plan_db.get_sat_states(self._curr_time_dt)
        sat_state = None
        sat_schedule_arb = self

        #  run the global planner
        # debug_tools.debug_breakpt()
        temp = self.latest_lp_route_indx
        new_rt_conts, dc_id_by_new_src_id, latest_lp_route_indx = lp_wrapper.run_lp(self._curr_time_dt,self.sim_sat.sat_indx,self.sim_sat.sat_id,self.sim_sat.lp_agent_id,existing_rt_conts,existing_data_conts,self.latest_lp_route_indx,sat_schedule_arb,replan_type,sat_state)


        #####
        # do any required cleanup

        dcs_to_cleanup = set()
        for rt_cont in new_rt_conts:
            # note: check by ID
            if rt_cont in existing_rt_conts:
                continue

            #####
            # split any data containers (as required) to reflect the new routes

            dc_id = dc_id_by_new_src_id.get(rt_cont.ID,None)        

            # todo: umm this is probably not the right way to go about matching. Should return a dict from the LP saying which DCs were matching. This is too ambiguous here. Also, why am I trying to match every rt_cont?
            # Figure out if this route container is intended to service an existing data container. 
            # matched_dcs = rt_cont.find_matching_data_conts(existing_data_conts,'executed')

            # # if this new route was not for servicing an existing data cont
            # if len(matched_dcs) == 0:
            #     continue
            
            # # it's possible multiple dcs match the route. Grab one of them that has enough DV
            # # todo: it might be possible for this to cause problems, if say there's one big matched dc and a small one, and the big one was the one that was intended to be used by the rt_cont. Check this out more in future?
            # matched_dc = None
            # for mdc in matched_dcs:
            #     if rt_cont.data_vol - lp_wrapper.lp_dv_epsilon <= mdc.data_vol:
            #         matched_dc = mdc

            if dc_id is None:
                continue
            # only deal with dc if route was created to handle a dc
            else:
                matched_dc = existing_data_conts_by_id[dc_id]

            # fork a new data container off of the previously existing one. This is so each route can have its own slice of dv to operate on. Take min to get rid of epsilon errors
            dv_forked = min(rt_cont.data_vol,matched_dc.data_vol)
            assert(dv_forked <= matched_dc.data_vol)
            new_dc = matched_dc.fork(
                self.sim_executive_agent.dc_agent_id,
                # note: bad form here on the abstraction crossing...
                new_dc_indx=self.sim_executive_agent.exec._curr_dc_indx,
                dv= dv_forked
            )
            self.sim_executive_agent.exec._curr_dc_indx += 1
            # new_rx_dc.add_to_route(act,tx_sat_indx)

            matched_dc.remove_dv(dv_forked)

            # add to the data container's plan history ( which is used elsewhere to make routing decisions for the data  container)
            new_dc.add_to_plan_hist(rt_cont)

            # update data storage with new data cont. Note that our delta dv here is 0 because we didn't create any new dv.
            self.state_sim.update_data_storage(0,[new_dc],new_time_dt)

            dcs_to_cleanup.add(matched_dc)


        # now need to cleanup any empty data conts (The None below is for the exec_act, which is not relevant here)
        # total_dv_to_cleanup = sum(dc.data_vol for dc in dcs_to_cleanup)
        self.state_sim.cleanup_empty_data_conts(dcs_to_cleanup,new_time_dt,dv_epsilon= lp_wrapper.lp_dv_epsilon)
        # even if our delta dv should be 0, it can have finite value ~ lp epsilon. Make sure to remove that from data storage record
        # self.state_sim.update_data_storage(-1*total_dv_to_cleanup,[],new_time_dt)

        # if self.sim_executive_agent.sat_id == 'sat1':
        #     debug_tools.debug_breakpt()

        #  I figure this can be done immediately and it's okay -  immediately updating the latest route index shouldn't be bad. todo:  confirm this is okay
        self.latest_lp_route_indx = latest_lp_route_indx

        self._clear_replan_reqs()

        return new_rt_conts

    def _process_updated_routes(self,rt_conts,update_time_dt):
        #  see superclass for docs

        #  mark all of the route containers with their release time
        for rt_cont in rt_conts:
            rt_cont.set_times_safe(update_time_dt)

        # update plan database
        self.plan_db.update_routes(rt_conts,update_time_dt)

    def flag_act_injected(self): 
        self.act_was_injected = True


class SatExecutive(Executive):
    """Handles execution of scheduled activities, with the final on whether or not to adhere exactly to schedule or make changes necessitated by most recent state estimates """

    def __init__(self,sim_sat,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        # info sharing radius
        self.k_neigh = 2    # TODO - to config!

        # State saving and propagation
        self.states_by_satsID           = {} # ex: 'sat3':{'ES':80, 'DS': 55, 'hop_count':1, 'last_update':timestamp}
        self.last_time_shared_to_satsID = {} # ex: 'sat5':timestamp

        super().__init__(sim_sat,sim_start_dt,dv_epsilon)

    def inject_obs(self,obs_list):
        """ add a set of observation windows for execution and routing without advance planning. note that this should only be performed once, at the beginning of the simulation"""

        obs_list.sort(key=lambda wind:wind.start)

        for obs in obs_list:
            #  only allowing injection of observation windows for the time being
            if not type(obs) is ObsWindow:
                raise NotImplementedError

            obs.set_executable_properties(dv_used=obs.data_vol)

            #  add an executable activity with no route containers. there are no route containers because the addition of this observation is serendipitous/unplanned/opportunistic... so there is no plan currently what to do with the window ( that's the LP's job)
            # Mark the activity as injected, so that the executive can handle it appropriately
            self._injected_exec_acts.append(ExecutableActivity(obs,rt_conts=[],dv_used=obs.data_vol))

        if len(obs_list) > 0:
            self._last_injected_exec_act_windex = 0

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ sets up context dictionary for activity execution on the satellite"""

        # todo: should probably add tracking of route containers that are intended to be executed, but no data containers can be found for them. this tracking should not necessarily require any response, but will be useful for  diagnostics/debug

        curr_exec_context = super()._initialize_act_execution_context(exec_act,new_time_dt)

        # if there was an error in initialization, return None
        if curr_exec_context is None:
            return None

        act = exec_act.act

        #  transmitted data containers (packets) for this activity ( if transmitting)
        curr_exec_context['tx_data_conts'] = []

        #  get a lookup table of data containers to send for each route container, given that we are transmitting
        curr_exec_context['executable_tx_data_conts_by_rt_cont'] = None
        is_tx = type(act) == DlnkWindow 
        is_tx |= type(act) == XlnkWindow and (not act.is_rx(self.sim_sat.sat_indx))
        if is_tx:
            exec_tx_dcs = self.get_tx_executable_routing_objs(
                act,
                self.sim_sat.sat_indx,
                exec_act.rt_conts,
                self.state_sim.get_curr_data_conts(),
                self.dv_epsilon
            )
            curr_exec_context['executable_tx_data_conts_by_rt_cont'] = exec_tx_dcs

        #  for receiving, we should keep track of which data container the transmitting satellite last sent
        if type(act) is XlnkWindow and not is_tx:
            curr_exec_context['curr_txsat_data_cont'] = None

        # stipulates how much data volume is available for an observation
        if type(act) is ObsWindow:
            # this is ugly, and kinda hacky. todo: make it better.  
            # because we are not actively adding the data volume to data tstorage during observations in _execute_act() below, we need to account for the data volume claimed by any other observations that might be going on right here.
            dv_avail = self.state_sim.get_available_data_storage(new_time_dt)
            for other_exec_act in self._curr_exec_acts:
                if type(other_exec_act) == ObsWindow:
                    dv_avail -= self._execution_context_by_exec_act[other_exec_act]['obs_dv_available']
            #  this should never go negative, because the available data volume for each observation maxes out at the observations data volume, and we shouldn't be subscribing for more DV than is available
            assert(dv_avail >= 0)

            # limit our claim to the maximum data volume for the observation
            curr_exec_context['obs_dv_available'] = min(dv_avail,act.executable_data_vol)

        #  returning this not because it's expected to be used, but to be consistent with superclass
        return curr_exec_context

    def _cleanup_act_execution_context(self,exec_act,new_time_dt):

        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        # flag any empty data conts that we transmitted for removal, and drop any ones that didn't get transmitted, but were supposed to.
        self.state_sim.cleanup_empty_data_conts(curr_exec_context['tx_data_conts'],new_time_dt)
        # todo: discluded the dropping of stale data conts for now until LP is more trustworthy
        # self.state_sim.remove_stale_data_conts(self,exec_act,time_dt)

        #  if the activity was injected, then we need to run the local planner afterwards to deal with the unplanned effects of the activity
        if exec_act.injected:
            self.scheduler.flag_act_injected()

        #  for this version of code, we handle the addition of data containers for observation activity data collected after executing the full observation. the code below adds these.
        curr_act_wind = curr_exec_context['act']

        if type(curr_act_wind) == ObsWindow:
            #  these are the route containers that were planned for collection
            collected_rt_conts = [rt_cont for rt_cont in  curr_exec_context['rt_conts'] if rt_cont.data_vol > self.dv_epsilon]

            #  this is the amount of observation data volume that was executed in _execute_act() below
            obs_dv_collected = curr_exec_context['dv_used']
            remaining_obs_dv_collected = obs_dv_collected

            collected_dcs = []


            #  for every route container, we create a data container for all of the data volume intended be collected for that route container
            for rt_cont in collected_rt_conts:
                if remaining_obs_dv_collected < self.dv_epsilon:
                    break

                dc_dv = min(rt_cont.data_vol_for_wind(curr_act_wind),remaining_obs_dv_collected)
                dc = SimDataContainer(self.sim_sat.dc_agent_id,self.sim_sat.sat_indx,self._curr_dc_indx,route=curr_act_wind,dv=dc_dv)
                self._curr_dc_indx += 1
                collected_dcs.append(dc)
                # Add the planned route container to the data container's history
                dc.add_to_plan_hist(rt_cont)
                #  make sure that if we are route containers, the activity is not marked as injected
                assert(not exec_act.injected)
                
                remaining_obs_dv_collected -= dc_dv


            #  if we still have data volume remaining, then go ahead and create a new data container
            if remaining_obs_dv_collected > self.dv_epsilon: 
                dc = SimDataContainer(self.sim_sat.dc_agent_id,self.sim_sat.sat_indx,self._curr_dc_indx,route=curr_act_wind,dv=remaining_obs_dv_collected)
                self._curr_dc_indx += 1
                collected_dcs.append(dc)
                # Add a null route container to the data container's history -  signifying that there was no route planned for this collected observation data.  this can happen, for example, in the case where this is an injected observation
                dc.add_to_plan_hist(None)
                remaining_obs_dv_collected -= remaining_obs_dv_collected

            # if self.sim_executive_agent.ID == 'sat1':
            #     debug_tools.debug_breakpt()

            #  need to use the new time here because the state sim has already advanced in timestep ( shouldn't be a problem)
            self.state_sim.update_data_storage(obs_dv_collected,collected_dcs,new_time_dt)

        # failures are only tracked and addressed for satellites, not GS agents, so code is here 
        # Check the curr_activity_window was successful or not
        data_disruption_vol = curr_act_wind.executable_data_vol - curr_exec_context['dv_used']
        
        if data_disruption_vol >= curr_act_wind.executable_data_vol * self.sim_sat.arbiter.frac_dv_lost_for_activity_failure_threshold: 
            print('================= ACTIVITY FAILED =================')
            print('Current Time is %s' %self._curr_time_dt)
            print('Expected %d data to be executed, but only %d was!' %(curr_act_wind.executable_data_vol,curr_exec_context['dv_used']))
            # But only trigger a " schedule disruption " if it was a downlink AND transmission failed 
            # otherwise, things can fail because the route doesn't have all the data that was originally executable (due to other in route failures)
            print(curr_act_wind)
            if isinstance(curr_act_wind,DlnkWindow) and not curr_exec_context['tx_success']:
                self.scheduler.schedule_disruption_occurred = True
                self.scheduler.schedule_disruption_context = curr_exec_context
                # add to failure dictionary (if a satelli
                if curr_act_wind in self.state_recorder.anamoly_dict['Invalid geometry at transmission time']:
                    self.state_recorder.failed_dict['exec']['Invalid geometry at transmission time'].add(curr_act_wind)
                    print('Downlink Activity Execution Failed Due invalid geometry at transmission time')
                elif curr_act_wind in self.state_recorder.anamoly_dict['No tx data containers associated with route']:
                    # log to failure dict
                    self.state_recorder.failed_dict['exec']['No tx data containers associated with route'].add(curr_act_wind)
                    print('Downlink Activity Execution Failed Due no tx data containers') # NOTE: schedule disruption replanner will not fix this
                else:
                    # TODO: THIS CAN OCCUR IF THE ACTIVITY IS NOT ONE OF THE EXECUTABLE ACTIVITIES FOR THE GS!
                    # this should only occur if there is an actual disruption event (not just Dlnk failed because of no access for other reasons)
                    print('Downlink Activity Execution Failed Due to schedule disruption')
                    self.state_recorder.failed_dict['exec']['Schedule disruption at receiving end'].add(curr_act_wind)
            elif isinstance(curr_act_wind,ObsWindow):
                obs_fail_str = 'Obs Activity Execution Failed from: '
                # ObsWindows can fail because   
                # - Obs window received in plan after execution period (logged when new schedule brought in)
                # - receiving satellite does not have room (logged below)
                if self.state_sim.DS_state >= self.state_sim.DS_max - self.state_sim.dv_epsilon:
                    self.state_recorder.failed_dict['exec']['Actual Data state does not support activity'].add(curr_act_wind)
                    obs_fail_str += 'DATA STORAGE FULL'
                else:
                    self.state_recorder.failed_dict['exec']['Unknown'].add(curr_act_wind)
                    obs_fail_str += 'UNKNOWN'
                print(obs_fail_str)
            elif isinstance(curr_act_wind,XlnkWindow):
                xlnk_fail_str = 'Xlnk Activity Execution Failed from: '
                # crosslinks can fail because:
                # - receiving satellite does not have room (logged below)
                # - receiving OR transmitting satellite does not have the crosslink in their current plan, but the other does
                #   NOTE: to check condition #2, we have to break abstraction and access the global sim info otherwise it is unknowable by the sat
                # check receiving sat failure modes:
                
                if curr_act_wind.rx_sat == self.sim_sat.index: 
                    rx_sat = self.sim_sat
                    tx_sat = self.sim_executive_agent.all_sim_sats[curr_act_wind.tx_sat]
                else:
                    rx_sat = self.sim_executive_agent.all_sim_sats[curr_act_wind.rx_sat]
                    tx_sat = self.sim_sat

                if rx_sat.state_sim.DS_state >= rx_sat.state_sim.DS_max - rx_sat.state_sim.dv_epsilon:
                    # not enough data onboard at rx
                    self.state_recorder.failed_dict['exec']['Actual Data state does not support activity'].add(curr_act_wind)
                    xlnk_fail_str += 'DATA STORAGE FULL AT RX'
                elif curr_act_wind not in tx_sat.exec._scheduled_exec_acts:
                    # transmission of crosslink not in current plan
                    self.state_recorder.failed_dict['exec']['Not in plan at execution time for transmitter'].add(curr_act_wind)
                    xlnk_fail_str += 'NOT IN PLAN AT TX'
                elif curr_act_wind not in rx_sat.exec._scheduled_exec_acts:
                    # reception of crosslink not in current plan
                    self.state_recorder.failed_dict['exec']['Not in plan at execution time for receiver'].add(curr_act_wind)
                    xlnk_fail_str += 'NOT IN PLAN AT RX'
                elif curr_act_wind in tx_sat.state_recorder.anamoly_dict['Invalid geometry at transmission time']:
                    tx_sat.state_recorder.failed_dict['exec']['Invalid geometry at transmission time'].add(curr_act_wind)
                    # transmission failed because of invalid geometry 
                    xlnk_fail_str += 'INVALID GEOMETRY AT TRANSMISSION TIME'
                elif curr_act_wind in tx_sat.state_recorder.anamoly_dict['No tx data containers associated with route']:
                    tx_sat.state_recorder.failed_dict['exec']['No tx data containers associated with route'].add(curr_act_wind)
                    xlnk_fail_str += 'NO TX DATA CONTAINERS'
                else:
                    xlnk_fail_str += 'UNKNOWN'
                    self.state_recorder.failed_dict['exec']['Unknown'].add(curr_act_wind)
                print(xlnk_fail_str)
            else:
                print('Dlnk failed from other in-route failure')
                self.state_recorder.failed_dict['exec']['Unknown'].add(curr_act_wind)

        #  need to call this last because it has final takedown responsibilities
        super()._cleanup_act_execution_context(exec_act,new_time_dt)

    @staticmethod
    def get_tx_executable_routing_objs(act,sat_indx,rt_conts,data_conts,dv_epsilon):
        """Given a set of planned routes (rt_conts) for act, determine which data packets (in data_conts) to actually transmit"""

        # dictionary of executable data packets for each route container, after de conflicting data packets across route containers
        # each of the underlying objects is a ExecutableDataContainer
        executable_data_conts_by_rt_cont = {}

        # Dictionary of possible data packets for each route container, before we actually pick which packets to send
        #  note that in general, the throughput in the route containers should be fully subscribed by existing data
        poss_data_conts_by_rt_cont = {rt_cont:rt_cont.find_matching_data_conts(data_conts) for rt_cont in rt_conts}

        #  sanity check that all of the route containers are meant to transmit data for this activity
        for rt_cont in rt_conts: assert(rt_cont.contains_wind(act))

        #  remaining data volumes by containers
        remaining_dv_by_data_cont = {dc:dc.data_vol for dcs in poss_data_conts_by_rt_cont.values() for dc in dcs}
        remaining_dv_by_rt_cont = {rc:rc.data_vol_for_wind(act) for rc in rt_conts}

        #  go through each route container ( in arbitrary order, shouldn't matter) and pick which data packets to send
        for curr_rc in rt_conts:
            executable_data_conts_by_rt_cont.setdefault(curr_rc,[])
            dcs = poss_data_conts_by_rt_cont[curr_rc]

            #  look through the list of data containers for this route container and pick those that have data volume to send him
            while len(dcs) > 0 and remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
                curr_dc = dcs.pop(0)

                delta_dv = min(remaining_dv_by_data_cont[curr_dc],remaining_dv_by_rt_cont[curr_rc])

                if delta_dv > dv_epsilon:
                    execdc = ExecutableDataContainer(data_cont=curr_dc,remaining_dv=delta_dv)
                    executable_data_conts_by_rt_cont[curr_rc].append(execdc)

                remaining_dv_by_rt_cont[curr_rc] -= delta_dv
                remaining_dv_by_data_cont[curr_dc] -= delta_dv 

            # todo: this is for debug, but probably ought to be logged somewhere...
            # if remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
            #     raise RuntimeWarning('insufficient data packet data volume to send over route containers. Routes might be stale...')

        return executable_data_conts_by_rt_cont


    def _execute_act(self,exec_act,new_time_dt):
        """ Execute the executable activity input, taking any actions that this satellite is responsible for initiating """

        #  this execution code assumes constant average data rate for every activity, which is not necessarily true. In general this should be all right because the planner schedules from the center of every activity, but in future this code should probably be adapted to use the actual data rate at a given time
        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        curr_act_wind = exec_act.act

        #############
        # Figure out time window for execution

        act_start_dt = curr_exec_context['start_dt']
        act_end_dt = curr_exec_context['end_dt'] 
        ts_start_dt, ts_end_dt = self.find_act_start_end_times(act_start_dt,act_end_dt, self._last_time_dt, self._curr_time_dt,new_time_dt,self.sim_sat.time_epsilon_td)
            
        #  time delta
        delta_t_s = (ts_end_dt - ts_start_dt).total_seconds()

        #  determine how much data volume can be produced or consumed based upon the activity average data rate ( the statement is valid for all types ObsWindow, XlnkWindow, DlnkWindow)
        delta_dv = curr_act_wind.ave_data_rate * delta_t_s


        #  the data volume that actually gets collected or lost
        executed_delta_dv = 0

        #############
        #  execute activity

        # if it's an observation window, we are just receiving data
        if type(curr_act_wind) == ObsWindow:

            #  for now we just mark off that we collected observation data, and will deal with creating data containers for the observation data in the cleanup phase, in _cleanup_act_execution_context() above. this means that the observation data is not released until after the activity has finished. this is okay for the current code version because no activities should depend upon the execution of another activity halfway in progress.
            #  note that it would be possible to create data containers right here, and iterate through the route containers as those data containers are created. however, when I started incrementing the logic for this it got very hairy very quickly
            # todo: this is kinda a hacky way of doing this, needs cleanup

            #  reduce this number by any restrictions due to data volume storage availability
            #  note that storage availability is affected by activity execution order at the current time step ( if more than one activity is to be executed)
            delta_dv = min(delta_dv,curr_exec_context['obs_dv_available'])

            curr_exec_context['dv_used'] += delta_dv
            curr_exec_context['obs_dv_available'] -= delta_dv
            #  we don't consider any data volumes have been executed here - will handle the addition of executed data volume in the cleanup stage ( for observations only)
            executed_delta_dv += 0


        #  deal with cross-link if we are the transmitting satellite
        #  note: this code only deals with execution by the transmitting satellite. execution by the receiving satellite is dealt with within xlnk_receive_poll()
        elif type(curr_act_wind) == XlnkWindow:

            xsat_indx = curr_act_wind.get_xlnk_partner(self.sim_sat.sat_indx)
            xsat = self.sim_sat.get_sat_from_indx(xsat_indx)
            xsat_exec = xsat.get_exec()
            is_tx = not curr_act_wind.is_rx(self.sim_sat.sat_indx)

            #  if we're the transmitting satellite, then we have the responsibility to start the data transfer transaction
            if is_tx:
                # tx_delta_dv = curr_act_wind.ave_data_rate * delta_t_s
                tx_delta_dv = delta_dv

                # while we still have delta dv to transmit
                while tx_delta_dv > self.dv_epsilon:
                
                    # figure out data cont, amount of data to transmit, the planned route container that specified which data container and amount of data to send
                    tx_dc, dv_to_send, tx_rc = self.select_tx_data_cont(curr_exec_context['executable_tx_data_conts_by_rt_cont'],tx_delta_dv,self.dv_epsilon)

                    # if we don't have anything to tx
                    if tx_dc is None:
                        # add to failure history
                        self.state_recorder.anamoly_dict['No tx data containers associated with route'].add(curr_act_wind)
                        break

                    # Add the planned route container to the data container's history
                    tx_dc.add_to_plan_hist(tx_rc)


                    tx_payload = {                              # TODO - Rolled & unrolled in different classes, why? (see sim_agent_components)
                        'type':'BDT',   # Bulk data transfer

                        'tx_ts_start_dt'    : ts_start_dt,
                        'tx_ts_end_dt'      : ts_end_dt,
                        'new_time_dt'       : new_time_dt,
                        'proposed_act'      : curr_act_wind,
                        'tx_sat_indx'       : self.sim_sat.sat_indx,  # TODO - Validate or correct this use of index
                        'txsat_data_cont'   : tx_dc,
                        'proposed_dv'       : dv_to_send
                    }

                    # send data to receiving satellite
                    #  note that once this receive poll returns a failure, we should not attempt to continue transmitting to the satellite ( subsequent attempts will not succeed in sending data)

                    dv_txed,tx_success = self.sim_sat.fst.transmit(xsat.ID, tx_payload)

                    #  if there was a successful reception of the data,
                    if tx_success:
                        tx_dc.remove_dv(dv_txed)
                        if not tx_dc in curr_exec_context['tx_data_conts']:
                            curr_exec_context['tx_data_conts'].append(tx_dc)

                        planning_info_send_option = 'all' if not curr_exec_context['tx_success'] else 'ttc_only'
                        
                        # mark your own tt&C as updated before sending
                        self.sim_sat.get_plan_db().update_self_ttc_time(new_time_dt)  
                        # this is where we do planning info update 
                        self.sim_sat.send_planning_info(xsat,planning_info_send_option)

                        # go ahead and assume bidirectional planning update....todo: this is hacky, should we actually explicitly model an activity for this?
                        xsat.get_plan_db().update_self_ttc_time(new_time_dt)  
                        xsat.send_planning_info(self.sim_sat,planning_info_send_option)

                        curr_exec_context['tx_success'] = True

                    #  if we did not successfully transmit, the receiving satellite is unable to accept more data at this time
                    else:
                        self.state_recorder.log_event(self._curr_time_dt,'sim_sat_components.py','act execution anomaly','failure to transmit to sat %s during xlnk activity %s'%(xsat_exec.sim_sat.sat_id,curr_act_wind))
                        break

                    tx_delta_dv -= dv_txed

                    curr_exec_context['dv_used'] += dv_txed

                    #  because  we are transmitting, we are losing data volume
                    executed_delta_dv -= dv_txed

        # transmitting over dlnks
        elif type(curr_act_wind) == DlnkWindow:
            
            gs_indx = curr_act_wind.gs_indx

            # Check if gs is available during this dlnk window:
            gs = self.sim_sat.get_gs_from_indx(gs_indx)
            gs_exec = gs.get_exec()

            # note that sat is always transmitting, if it's a downlink. So sat has responsibility to start the data transfer transaction
            tx_delta_dv = delta_dv

            # while we still have delta dv to transmit
            while tx_delta_dv > self.dv_epsilon:
            
                # figure out data cont, amount of data to transmit
                tx_dc, dv_to_send, tx_rc = self.select_tx_data_cont(curr_exec_context['executable_tx_data_conts_by_rt_cont'],tx_delta_dv,self.dv_epsilon)

                # if we don't have anything to tx
                if tx_dc is None:
                    self.state_recorder.log_event(self._curr_time_dt,'sim_sat_components.py','act execution anomaly','No data container onboard for this route to GS %s during dlnk activity %s'%(gs_exec.sim_gs.ID,curr_act_wind))
                    # add to failure history
                    self.state_recorder.anamoly_dict['No tx data containers associated with route'].add(curr_act_wind)
                    break

                # Add the planned route container to the data container's history
                tx_dc.add_to_plan_hist(tx_rc)

                tx_payload = {
                    'type':'BDT',   # Bulk data transfer

                    'tx_ts_start_dt'    : ts_start_dt,
                    'tx_ts_end_dt'      : ts_end_dt,
                    'new_time_dt'       : new_time_dt,
                    'proposed_act'      : curr_act_wind,
                    'tx_sat_indx'       : self.sim_sat.sat_indx,  # TODO - Validate or correct this use of index
                    'txsat_data_cont'   : tx_dc,
                    'proposed_dv'       : dv_to_send
                }

                # send data to receiving satellite
                #  note that once this receive poll returns a failure, we should not attempt to continue transmitting to the satellite ( subsequent attempts will not succeed in sending data)
                dv_txed,tx_success = self.sim_sat.fst.transmit(gs.ID, tx_payload)

                #  if there was a successful reception of the data,
                if tx_success:
                    tx_dc.remove_dv(dv_txed)
                    if not tx_dc in curr_exec_context['tx_data_conts']:
                        curr_exec_context['tx_data_conts'].append(tx_dc)

                    planning_info_send_option = 'all' if not curr_exec_context['tx_success'] else 'ttc_only'

                    # mark your own tt&C as updated before sending
                    self.sim_sat.get_plan_db().update_self_ttc_time(new_time_dt)  

                    # this is where we do planning info update. Order doesn't matter. Notice we assume an uplink is present, so we can get data from gs as well
                    self.sim_sat.send_planning_info(gs)

                    gs.get_plan_db().update_self_ttc_time(new_time_dt) # this is needed for the cmd_aoi plot metrics
                    gs.send_planning_info(self.sim_sat)

                    curr_exec_context['tx_success'] = True

                #  if we did not successfully transmit, the receiving satellite is unable to accept more data at this time
                else:
                    # WG - ADDED THIS HERE
                    curr_exec_context['tx_success'] = False 
                    self.state_recorder.log_event(self._curr_time_dt,'sim_sat_components.py','act execution anomaly','failure to transmit to gs %s during dlnk activity %s'%(gs_exec.sim_gs.gs_indx,curr_act_wind))
                    break

                tx_delta_dv -= dv_txed

                curr_exec_context['dv_used'] += dv_txed

                #  because  we are transmitting, we are losing data volume
                executed_delta_dv -= dv_txed

        #  otherwise, we don't know what to do with this activity type
        else:
            raise NotImplementedError

        all_data_conts = curr_exec_context['tx_data_conts'] + curr_exec_context['rx_data_conts']
        self.state_sim.update_data_storage(executed_delta_dv,all_data_conts,self._curr_time_dt)


    @staticmethod
    def select_tx_data_cont(exec_data_conts_by_rt_cont,tx_dv_possible,dv_epsilon):
        """Determines which data container (packet) to send based on plans (route containers) for given execution context"""

        #  figure out which data container to send based upon planned data transmission
        exec_dc_choice = None
        #  this stores the route container that was the justification for sending the chosen data container. i.e., plan that leads to the transmission
        rt_cont_choice = None
        for rc,exec_data_conts in exec_data_conts_by_rt_cont.items():
            # each exec_dc is of type ExecutableDataContainer -  a record of how much data volume to transmit from a given data container for a given route container
            for exec_dc in exec_data_conts:
                #  if there is still remaining data volume for this data container, then choose it
                if exec_dc.remaining_dv > dv_epsilon:
                    exec_dc_choice = exec_dc
                    rt_cont_choice = rc
                    break

            if exec_dc_choice:
                break

        # there's no data we have to send!
        if not exec_dc_choice:
            return None,0.0,None

        #  the amount of data volume to send is the minimum of how much we can send during current activity, how much we plan to send for a given data container, and how much data is available from that data container
        dv_to_send = min(tx_dv_possible,exec_dc_choice.remaining_dv,exec_dc_choice.data_vol)

        # sanity check -  if we had planned to send a certain amount of data volume from a data container, we should have that data volume present in the data container
        if exec_dc_choice.remaining_dv > exec_dc_choice.data_vol:
            raise RuntimeWarning('Thought there was more data to send than is actually present in data container')

        #  this subtracts data volume from the executable data container ( not the data container itself!)
        exec_dc_choice.remaining_dv -= dv_to_send

        return exec_dc_choice.data_cont,dv_to_send,rt_cont_choice


    def get_acts_at_time(self,time_dt):

        if abs(time_dt - self._curr_time_dt) < self.sim_sat.time_epsilon_td:
            return [exec_act.act for exec_act in self._curr_exec_acts]
        else:
            raise NotImplementedError

    # Sender and receiver for sharing propagated data (including self)
    # TODO - consider downlinking as well (only xlink prop right now)
    def state_x_prop(self,global_time):
        self_record = {     # always add 0-th order self-state
            'state' : {
                'ES' : self.sim_sat.state_sim.ES_state,         # Can arbitrarily add more states here.
                'DS' : self.sim_sat.state_sim.DS_state, 
            },
            'sch' : self.sim_sat.arbiter._schedule_cache,       # A mask/schedule of a sat, to be used for forming candidate plans.
            'api' : '',                                         # a-priori plan-info
            'hop_count' : 0,   # generated here!
            'timestamp' : global_time
        }

        xlnks_completed = 0
        xlinkable_sats = self.sim_sat.local_stn_knowledge.get_graph_neighbors(self.sim_sat.ID, time=global_time, neigh_type=AgentType.SAT) # TODO - enforce getting sats only
        for neigh_sat_ID in xlinkable_sats:                             # For all sats we can see right now
            records_to_send = { self.sim_sat.ID : self_record }             # Always share own state
            for rec_id, record in self.states_by_satsID.items():            # sift records for which to share
                if ( (record['hop_count'] < self.k_neigh)                      # share if record is newer than last share to this neighbor, and it hasn't made too many hops
                        and ( (neigh_sat_ID not in self.last_time_shared_to_satsID)
                                or (self.last_time_shared_to_satsID[neigh_sat_ID] < record['timestamp']) )
                        ):
                    records_to_send[rec_id] = record        # add to record to send
                    self.last_time_shared_to_satsID[neigh_sat_ID] = record['timestamp']
            msg = {
                'type'          : 'STATES',
                'state_records' : records_to_send
            }

            if(self.sim_sat.fst.transmit(neigh_sat_ID, msg)):   # count if at least one completed
                xlnks_completed += 1

        if len(xlinkable_sats)==xlnks_completed:  # count the effort if there's nothing to send
            return True                             # TODO - clumsy, b/c causes resending to all if one doesn't make it
        else:
            return False


    def state_x_rec(self,state_data_msg):
        records_rec = state_data_msg['state_records']
        for sat_ID in records_rec.keys():
            if ( (sat_ID not in self.states_by_satsID) # This condition first!
                    or (records_rec[sat_ID]['timestamp'] > self.states_by_satsID[sat_ID]['timestamp']) ):
                records_rec[sat_ID]['hop_count'] += 1
                if( (records_rec[sat_ID]['hop_count'] <= self.k_neigh) and
                        (sat_ID != self.sim_sat.ID) ):
                    self.states_by_satsID[sat_ID] = records_rec[sat_ID]
                else:
                    self.states_by_satsID.pop(sat_ID, None)   # if it's too far, stop tracking (e.g., saw it on an inter-plane xlink, then your neighbor saw it and passed it along, but you don't care anymore...depending on your k_neigh setting)

        # TODO - if older than [1 planning horizon?] discard?
        # TODO - immediate propagation if newer info than last propagated?
        return True



class SatStateRecorder(ExecutiveAgentStateRecorder):
    """Convenient interface for storing state history for satellites"""

    def __init__(self,sim_start_dt):
        self.ES_state_hist = [] # assumed to be in Wh

        super().__init__(sim_start_dt)    

    def add_ES_hist(self,t_dt,val):
        # todo: add decimation?
        t_s = (t_dt - self.base_time).total_seconds()
        self.ES_state_hist.append((t_s,val))

    def get_act_hist(self):
        acts_exe = super().get_act_hist()

        #  add in observations and cross-links to the downlinks obtained 
        acts_exe['obs'] = []
        acts_exe['xlnk'] = []
        for act in self.act_hist:
            if type(act) == ObsWindow: acts_exe['obs'].append(act)
            if type(act) == XlnkWindow: acts_exe['xlnk'].append(act)

        return acts_exe

    def get_ES_hist(self,out_units='minutes'):

        if not out_units == 'minutes':
            raise NotImplementedError

        t = []
        e = []
        for pt in self.ES_state_hist:
            t.append(pt[0]/60.0) # converted to minutes
            e.append(pt[1])

        return t,e


