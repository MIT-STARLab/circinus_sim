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

from circinus_tools  import io_tools
from circinus_tools.scheduling.base_window  import find_window_in_wind_list
from circinus_tools.scheduling.schedule_tools  import synthesize_executable_acts
from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import PlannerScheduler,StateRecorder,PlanningInfoDB
from .sim_routing_objects import SimDataContainer

class SatStateSimulator:
    """Simulates satellite system state, holding internally any state variables needed for the process. This state includes things like energy storage, ADCS modes/pointing state (future work)"""

    def __init__(self,sim_sat,sim_start_dt,state_simulator_params,sat_power_params,sat_data_storage_params,sat_initial_state):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        self.ES_state = sat_initial_state['batt_e_Wh']

        self.sat_edot_by_mode,self.sat_batt_storage,power_units = io_tools.parse_power_consumption_params(sat_power_params)

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

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

    def update(self,new_time_dt):
        """ Update state to new time by propagating state forward from last time to new time. Note that we use state at self._curr_time_dt to propagate forward to new_time_dt"""

        # If first step, just record state then return
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self.state_recorder.add_ES_hist(self._curr_time_dt,self.ES_state)
            self._first_step = False
            return

        # this is consistent with power units above
        delta_t_h = (new_time_dt - self._curr_time_dt).total_seconds()/3600

        current_act = self.sat_exec.get_act_at_time(self._curr_time_dt)


        ##############################
        # Energy storage update

        act_edot = 0
        if current_act:
            sat_indx = self.sim_sat.sat_indx
            act_edot = self.sat_edot_by_mode[current_act.get_code(sat_indx)]

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


        # todo: add in DS stuff?


        self._curr_time_dt = new_time_dt

        ##############################
        # update state recorder
    
        self.state_recorder.add_ES_hist(self._curr_time_dt,self.ES_state)
        # todo: add whatever else needed

    def nominal_state_check(self):
        # if we're below lower energy bound, state is off nominal
        if self.ES_state < self.sat_batt_storage['e_min']:
            return False

        return True

    def in_eclipse(self,time_dt):
        ecl_winds = self.sim_sat.get_ecl_winds()
        curr_ecl_wind,self._curr_ecl_windex = find_window_in_wind_list(time_dt,self._curr_ecl_windex,ecl_winds)

        if curr_ecl_wind:
            return True


class SatScheduleArbiter(PlannerScheduler):
    """Handles ingestion of new schedule artifacts from ground planner and their deconfliction with onboard updates. Calls the LP"""

    def __init__(self,sim_sat,sim_start_dt,sim_end_dt):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        #  this records whether or not the information in the planning database has been updated. we use this planning information to derive a schedule.  we assume that we start with no planning information available, so this is false for now ( we can't yet derive a schedule from the planning info)
        self._planning_info_updated = False

        #  whether or not schedule instance has been updated since it was last grabbed by executive
        self._schedule_updated = False

        #  cached schedule, regenerated every time we receive new planning info. the elements in this list are of type circinus_tools.scheduling.routing_objects.ExecutableActivity
        self._schedule_cache = []

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

        super().__init__(sim_start_dt,sim_end_dt)

    @property
    def schedule_updated(self):
        return self._schedule_updated

    def flag_planning_info_update(self):
        self._planning_info_updated = True

    # def ingest_routes(self,rt_conts):
    #     #  add to database
    #     self.plan_db.update_routes(rt_conts)

    def update(self,new_time_dt):
        # If first step, check the time
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self._first_step = False

        # rest of the code is fine to execute in first step, because we might have planning info from which to derive a schedule

        #  if planning info has not been updated in the schedule has already been updated, then there is no reason to update schedule
        if not self._planning_info_updated and self._schedule_updated:
            return

        #  get relevant sim route containers for deriving a schedule
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within',sat_id=self.sim_sat.sat_id)

        #  get all the windows that are executable from all of the route containers, filtered for this satellite
        # using a set to ensure unique windows ( there can be duplicate windows across route containers)
        # todo: Need to do a better job here of handling different t utilization numbers for windows from each route -  what if two routes expect different start and end times for the window based on utilization?
        # executable_acts = set()
        # for rt_cont in rt_conts:
            # executable_acts = executable_acts.union(rt_cont.get_winds_executable(filter_start_dt=self._curr_time_dt,sat_indx=self.sim_sat.sat_indx))

        #  synthesizes the list of unique activities to execute, with the correct execution times and data volumes on them
        #  the list elements are of type circinus_tools.scheduling.routing_objects.ExecutableActivity
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,sat_indx=self.sim_sat.sat_indx)

        # sort executable activities by start time
        executable_acts.sort(key = lambda ex_act: ex_act.act.executable_start)

        self._schedule_updated = True
        self._planning_info_updated = False
        self._schedule_cache = executable_acts

        self._curr_time_dt = new_time_dt

    def get_scheduled_executable_acts(self):
        self._schedule_updated = False
        return self._schedule_cache


class SatExecutive:
    """Handles execution of scheduled activities, with the final on whether or not to adhere exactly to schedule or make changes necessitated by most recent state estimates """

    def __init__(self,sim_sat,sim_start_dt):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        #  scheduled activities list- a sorted list of the activities for satellite to perform ( obtained from schedule arbiter)
        #  these are of type circinus_tools.scheduling.routing_objects.ExecutableActivity
        self.scheduled_exec_acts = []

        # these keep track of which activity we are currently executing. The windox index (windex)  is the location within the scheduled activities list
        self._curr_exec_act = None
        self._curr_act_windex = None

        #  maintains a record of of whether or not the current activity is canceled ( say, due to an off nominal condition)
        self._curr_cancelled_act = None

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        # holds ref to SatStateSimulator
        self.sat_state_sim = None
        # holds ref to SatScheduleArbiter
        self.sat_arbiter = None
        # holds ref to SatStateRecorder
        self.state_recorder = None
        # holds ref to SatDataStore
        self.data_store = None

        # keeps track of which data container (data packet) we're on
        self.curr_dc_indx = 0

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

    def _pull_schedule(self):

        # if schedule updates are available, blow away what we had previously (assumption is that arbiter handles changes to schedule gracefully)
        if self.sat_arbiter.schedule_updated:
            self.scheduled_exec_acts = self.sat_arbiter.get_scheduled_executable_acts()
        else:
            return

        # if we updated the schedule, need to deal with the consequences

        # if we're currently executing an act, figure out where that act is in the new sched. That's the "anchor point" in the new sched. (note in general the current act SHOULD be the first one in the new sched. But something may have changed...)
        if not self._curr_exec_act is None:
            try:
                curr_act_indx = self.scheduled_exec_acts.index(self._curr_exec_act)
            except ValueError:
                # todo: i've seen funky behavior here before, not sure if totally resolved...
                # todo: add cleanup?
                # - what does it mean if current act is not in new sched? cancel current act?
                raise NotImplementedError("Haven't yet implemented schedule changes mid-activity")

            self._curr_act_windex = curr_act_indx

        # if there are activities in the schedule, then assume we're starting from beginning of it
        elif len(self.scheduled_exec_acts) > 0:
            self._curr_act_windex = 0

        # no acts in schedule, for whatever reason (near end of sim scenario, a fault...)
        else:
            # todo: no act, so what is index? Is the below correct?
            self._curr_act_windex = None

    @staticmethod
    def executable_time_accessor(exec_act,time_prop):
        if time_prop == 'start':
            return exec_act.act.executable_start
        elif time_prop == 'end':
            return exec_act.act.executable_end

    def update(self,new_time_dt):
        """ Update state of the executive """
        #  todo: should add handling in here for executing multiple activities at one time

        # If first step, check the time (first step time should be the sim start time)
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')

        ##############################
        # execute current activity

        # if it's not the first step, then we can execute current activity
        if not self._first_step:
            self._execute_curr_act(new_time_dt)

        ##############################
        # determine next activity

        # update schedule if need be
        self._pull_schedule()

        # figure out current activity, index of that act in schedule (note this could return None)
        next_exec_act,next_act_windex = find_window_in_wind_list(new_time_dt,self._curr_act_windex,self.scheduled_exec_acts,self.executable_time_accessor)

        # todo: probably need to add handling to avoid time-step sized gaps between activities

        #  if our state is off-nominal,  then we should protect ourselves by electing not to perform any activity
        #  maintain a record of if we canceled an activity or not. if we previously canceled it, then don't try to perform it on a subsequent time step
        act_is_cancelled = (self._curr_cancelled_act and next_exec_act) and self._curr_cancelled_act == next_exec_act
        #  if the next_exec_act ( activity at next time step) was already recorded as canceled, or it needs to be canceled
        if act_is_cancelled or not self.sat_state_sim.nominal_state_check():
            if next_exec_act: self._curr_cancelled_act = next_exec_act
            next_exec_act = None
            #  note: do not overwrite next act window index, because we need to maintain that record

        #  if we've changed from last activity, then need to clean up the context for that activity
        stopped_curr_act = (self._curr_exec_act is not None) and (next_exec_act is None or next_exec_act != self._curr_exec_act)
        if stopped_curr_act:
            self._cleanup_act_execution_context()

        #  if we've started a new activity, then need to setup up the context for that activity
        started_new_act = (next_exec_act is not None) and (self._curr_exec_act is None or next_exec_act != self._curr_exec_act)
        if started_new_act:
            self._initialize_act_execution_context(next_exec_act,new_time_dt)

        self._curr_exec_act = next_exec_act
        self._curr_act_windex = next_act_windex

        self._curr_time_dt = new_time_dt

        # mark no longer first step
        if self._first_step:
            self._first_step = False

    def _initialize_act_execution_context(self,next_exec_act,new_time_dt):
        act = next_exec_act.act
        self._curr_execution_act = next_exec_act.act
        #  using the dictionary here both to keep the number of attributes for self down, and to make extension to multiple current execution acts not terrible todo (hehe, sneaky todo there)
        self._curr_exec_data = {}
        self._curr_exec_data['start'] = new_time_dt
        self._curr_exec_data['rx_data_conts'] = []
        self._curr_exec_data['tx_data_conts'] = []
        self._curr_exec_data['dv_used'] = 0
        #  this stores the route containers (i.e. data routes) that are planned to be executed during this activity
        # planned_route_conts_queue = [rt_cont for rt_cont in next_exec_act.rt_conts]
        self._curr_exec_data['curr_route_cont'] = None
        # self._curr_exec_data['planned_route_conts_queue'] = planned_route_conts_queue

        #  get a lookup table of data containers to send for each route container, given that we are transmitting
        is_tx = type(act) == DlnkWindow 
        is_tx |= type(act) == XlnkWindow and (not act.is_rx(self.sim_sat.sat_indx))
        if is_tx:
            todo implement get_curr_data_conts
            self._curr_exec_data['executable_tx_data_conts_by_rt_cont'] = self.get_tx_executable_routing_objs(act,self.sim_sat.sat_indx,next_exec_act.rt_conts,self.data_store.get_curr_data_conts())
        else:
            self._curr_exec_data['executable_tx_data_conts_by_rt_cont'] = None


        # self._curr_exec_data['cum_rx_dv'] = 0
        # self._curr_exec_data['cum_tx_dv'] = 0
        # self._curr_exec_data['rt_cont_queue'] = [rt_cont for rt_cont in self._curr_exec_act.rt_conts]
        # self._curr_exec_data['curr_tx_rt_cont'] = self._curr_exec_data['rt_cont_queue'].pop(0)

    @staticmethod
    def get_tx_executable_routing_objs(act,sat_indx,rt_conts,data_conts):
        """Given a set of planned routes (rt_conts) for act, determine which data packets (in data_conts) to actually transmit"""

        # Dictionary of possible data packets for each route container, before we actually pick which packets to send
        poss_data_conts_by_rt_cont = {}
        #  dictionary of executable data packets for each route container, after de conflicting data packets across route containers
        executable_data_conts_by_rt_cont = {}

        #  populate possible data packets
        for rt_cont in rt_conts:
            todo implement function
            poss_data_conts_by_rt_cont[rt_cont] = rt_cont.find_matching_data_conts(data_conts)

        #  remaining data volumes by containers
        remaining_dv_by_data_cont = [dc.data_vol for dcs in poss_data_conts_by_rt_cont.values() for dc in dcs]
        todo implement function
        remaining_dv_by_rt_cont = [rc.data_vol_for_wind(act) for rc in rt_conts]

        #  go through each route container ( in arbitrary order, shouldn't matter) and pick which data packets to send
        for curr_rc in rt_conts:
            executable_data_conts_by_rt_cont.set_default(curr_rc,[])
            dcs = poss_data_conts_by_rt_cont[curr_rc]

            while len(dcs) > 0 and remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
                curr_dc = dcs.pop(0)

                delta_dv = min(remaining_dv_by_data_cont[curr_dc],remaining_dv_by_rt_cont[curr_rc])

                if delta_dv > dv_epsilon:
                    executable_data_conts_by_rt_cont[curr_rc].append(curr_dc)
                    todo need to add the data volume used by curr_dc

                remaining_dv_by_rt_cont[curr_rc] -= delta_dv
                remaining_dv_by_data_cont[curr_dc] -= delta_dv 

            if remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
                # todo: remove this
                raise RuntimeWarning('insufficient data packet data volume to send over route containers')

        return executable_data_conts_by_rt_cont







    def _cleanup_act_execution_context(self):
        # self._curr_act_execution_start = None

        self.data_store.add(self._curr_exec_data['rx_data_conts'])
        implement cleanup func
        self.data_store.cleanup(self._curr_exec_data['tx_data_conts'])

        curr_act_wind = self._curr_execution_act
        end_time = self._curr_time_dt
        curr_act_wind.set_executed_properties(self._curr_exec_data['start'],end_time,self._curr_exec_data['dv_used'])
        self.state_recorder.add_act_hist(curr_act_wind)
        
        self._curr_execution_act = None
        self._curr_exec_data = None

    def _execute_curr_act(self,new_time_dt):
        # todo: add support for temporally overlapping activities?
        #  this execution code assumes constant average data rate for every activity, which is not necessarily true. In general this should be all right because the planner schedules from the center of every activity, but in future this code should probably be adapted to use the actual data rate at a given time

        # todo: this probably needs to be adjusted based on actual activity length
        delta_t_s = (new_time_dt - self._curr_time_dt).total_seconds()

        curr_act_wind = None
        if curr_act_wind:
            curr_act_wind = self._curr_exec_act.act
        #  if there is no current activity going on then there's no reason to execute the rest of this code
        else:
            return

        #  if it's an observation window, we are just receiving data
        if type(curr_act_wind) == ObsWindow:
            # if we just started this observation, then there are no data containers (packets). Make a new one and append
            if len(self._curr_exec_data['rx_data_conts']) == 0:
                curr_data_cont = SimDataContainer(self.sim_sat.sat_id,self.curr_dc_indx,route=curr_act_wind,dv=0)
                self.curr_dc_indx += 1
                self._curr_exec_data['rx_data_conts'].append(curr_data_cont)
            else:
                curr_data_cont = self._curr_exec_data['rx_data_conts'][-1]

            delta_dv = curr_act_wind.ave_data_rate * delta_t_s
            # todo add delta dv to DS state

            curr_data_cont.add_dv(delta_dv)
            self._curr_exec_data['dv_used'] += delta_dv

        if type(curr_act_wind) == XlnkWindow:

            xsat_indx = curr_act_wind.get_xlnk_partner(self.sim_sat.sat_indx)
            xsat = self.TODO[xsat_indx]
            # xsat_is_executing = .check_act_execution(curr_act_wind)
            # is_rx = curr_act_wind.is_rx(self.sim_sat.sat_indx)
            is_tx = not curr_act_wind.is_rx(self.sim_sat.sat_indx)

            # if is_rx:
                # self._curr_exec_data{'cum_rx_dv'} += curr_act_wind.ave_data_rate * delta_t_s
            else:
                # tx_delta_dv = curr_act_wind.ave_data_rate * delta_t_s
                # dv_txed = xsat.xlnk_receive_poll(self,self._curr_time_dt,curr_act_wind,tx_delta_dv)
                # self._curr_exec_data['cum_tx_dv'] += dv_txed
                # self._curr_exec_data['tx_dv_by_rt_cont'].set_default(rt_cont,0)
                # self._curr_exec_data['tx_dv_by_rt_cont'][rt_cont] += dv_txed

                tx_delta_dv = curr_act_wind.ave_data_rate * delta_t_s

                todo make dv_epsilon var
                # while still delta dv
                while tx_delta_dv > dv_epsilon:
                
                    # figure out data cont, amount of data to transmit
                    tx_dc = None
                    for rc,data_conts in self._curr_exec_data['executable_tx_data_conts_by_rt_cont']:
                        for dc in data_conts:
                            if self._curr_exec_data['remaining_tx_dv_by_data_cont'][dc] > dv_epsilon:
                                tx_dc = dc

                    dv_to_send = min(tx_delta_dv,self._curr_exec_data['remaining_tx_dv_by_data_cont'][tx_dc],tx_dc.data_vol)

                    if self._curr_exec_data['remaining_tx_dv_by_data_cont'][tx_dc] > tx_dc.data_vol:
                        raise RuntimeWarning('Thought there was more data to send than is actually present in data container')


                    # send data cont (and cleanup)
                    dv_txed,tx_success = xsat.xlnk_receive_poll(self._curr_time_dt,curr_act_wind,tx_dc,dv_to_send)

                    if tx_success:
                        tx_dc.remove_dv(dv_txed)
                        if not tx_dc in self._curr_exec_data['tx_data_conts']:
                            self._curr_exec_data['tx_data_conts'].append(tx_dc)

                    tx_delta_dv -= dv_txed



    def xlnk_receive_poll(self,curr_time,proposed_act,xsat_indx,xsat_data_cont,proposed_dv):

        # note: do not modify tx_data_cont in here!

        #  check if the satellites have the same current time. if not it's problematic. note that will assume they are both using the same time step and so the transmitting satellite that is polling this satellite integrates its data rate over the same amount of time
        # todo: verify this is okay
        if not curr_time == self._curr_time_dt:
            raise RuntimeWarning('time mismatch between satellites')

        if not type(proposed_act) == XlnkWindow:
            raise RuntimeWarning('saw a non-xlnk window')

        # todo: need to add planning info exchange too
        # todo: check that DS buffer not too full

        received_dv = 0
        #  if this satellite is not actually executing the activity, then it can't receive data
        if not self._curr_exec_act.act == proposed_act:
            received_dv = 0
        else:
            if not proposed_act.is_rx(self.sim_sat.sat_indx):
                raise RuntimeWarning('cross-link attempt with non-receiver satellite')

            received_dv = proposed_dv

        # todo add delta dv to DS state
        # todo: add checking of how much dv we can receive based on DS state. The rest of the code should only happen if received dv still > 0


        if xsat_data_cont == self._curr_exec_data['curr_xlnk_xsat_data_cont']:
            rx_dc = self._curr_exec_data['rx_data_conts'][-1]
        else:
            self._curr_exec_data['curr_xlnk_xsat_data_cont'] = xsat_data_cont
            new_rx_dc = xsat_data_cont.fork(self.sim_sat.sat_id,new_dc_indx=self.curr_dc_indx)
            self.curr_dc_indx += 1
            new_rx_dc.add_to_route(proposed_act,xsat_indx)
            self._curr_exec_data['rx_data_conts'].append(new_rx_dc)
            rx_dc = new_rx_dc

        rx_dc.add_dv(received_dv)

        self._curr_exec_data['dv_used'] += received_dv

        # todo: need to add recording of tlm, cmd update

        rx_success = False
        if received_dv > dv_epsilon:
            rx_success = True

        return received_dv,rx_success



    # def is_executing(act_wind):
    #     return self._curr_exec_act.act == act_wind

    #     todo add markup that current ts performed already


    def get_act_at_time(self,time_dt):

        if abs(time_dt - self._curr_time_dt) < self.sim_sat.time_epsilon_td:
            if self._curr_exec_act:
                return self._curr_exec_act.act
            else:
                return None
        else:
            raise NotImplementedError


class SatStateRecorder(StateRecorder):
    """Convenient interface for storing state history for satellites"""

    def __init__(self,sim_start_dt,state_recorder_params):
        self.base_time = sim_start_dt
        self.ES_state_hist = []
        self.DS_state_hist = []
        self.act_hist = []

        super().__init__()    

    def add_ES_hist(self,t_dt,val):
        # todo: add decimation?
        t_s = (t_dt - self.base_time).total_seconds()
        self.ES_state_hist.append((t_s,val))

    def add_act_hist(self,act):
        # note that the execution times, dv for act are stored in attributes: executable_start,executable_end,executable_data_vol
        self.act_hist.append(act)

    def get_act_hist(self):
        obs_exe = []
        xlnks_exe = []
        dlnks_exe = []
        for act in self.act_hist:
            if type(act) == ObsWindow: obs_exe.append(act)
            if type(act) == XlnkWindow: xlnks_exe.append(act)
            if type(act) == DlnkWindow: dlnks_exe.append(act)

        return obs_exe,dlnks_exe,xlnks_exe

    def get_ES_hist(self,out_units='minutes'):

        if not out_units == 'minutes':
            raise NotImplementedError

        t = []
        e = []
        for pt in self.ES_state_hist:
            t.append(pt[0]/60.0) # converted to minutes
            e.append(pt[1])

        return t,e



