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
from circinus_tools.other_tools import index_from_key
from .sim_agent_components import PlannerScheduler,StateRecorder,PlanningInfoDB,DataStore
from .sim_routing_objects import SimDataContainer,ExecutableDataContainer
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools

class SatStateSimulator:
    """Simulates satellite system state, holding internally any state variables needed for the process. This state includes things like energy storage, ADCS modes/pointing state (future work)"""

    def __init__(self,sim_sat,sim_start_dt,state_simulator_params,sat_power_params,sat_data_storage_params,sat_initial_state,dv_epsilon=0.01):
        #  should probably add more robust physical units checking

        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        ################
        #  Data storage stuff

        #  note that data generated is not really simulated here; the state simulator just provides a coherent API for storage ( the executive handles data generation)

        #  data storage is assumed to be in Mb
        #  assume we start out with zero data stored on board
        self.DS_state = 0
        storage_opt = sat_data_storage_params['storage_option']
        self.DS_max = sat_data_storage_params['data_storage_Gbit']['d_max'][storage_opt] * 1000  # convert to megabits
        # note:  assuming no minimum data storage limit - it's just 0!
        self.DS_min = 0

        ################
        #  energy storage stuff

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
            act_edot = self.sat_edot_by_mode[act.get_code(sat_indx)]

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

    def get_available_data_storage(self,time_dt):
        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to get available data volume from state sim off-timestep')

        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon)

        return self.DS_max - self.DS_state

    def add_to_data_storage(self,delta_dv,data_conts,time_dt):
        """ add an amount of data volume to data storage state"""

        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to add data volume to state sim off-timestep')

        # add any data containers that the data store doesn't know about yet
        self.data_store.add(data_conts)

        new_DS_state = self.DS_state + delta_dv

        if new_DS_state > self.DS_max:
            raise RuntimeWarning('Attempting to add more to data storage than than is available. Current DS state: %f, delta dv %f, DS  max: %f'%(self.DS_state,delta_dv,self.DS_max))
        if new_DS_state < self.DS_min:
            raise RuntimeWarning('Attempting to go below minimum data storage limit. Current DS state: %f, delta dv %f, DS min: %f'%(self.DS_state,delta_dv,self.DS_min))

        self.DS_state = new_DS_state

        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon)


    def get_curr_data_conts(self):
        return self.data_store.get_curr_data_conts()

    def cleanup_data_conts(self,data_conts):
        self.data_store.cleanup(data_conts)


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

    def __init__(self,sim_sat,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        #  scheduled activities list- a sorted list of the activities for satellite to perform ( obtained from schedule arbiter)
        #  these are of type circinus_tools.scheduling.routing_objects.ExecutableActivity
        self.scheduled_exec_acts = []

        # these keep track of which activity we are currently executing. The windox index (windex)  is the location within the scheduled activities list
        self._curr_exec_acts = []
        self._last_exec_act_windex = None

        #  maintains a record of of whether or not the current activity is canceled ( say, due to an off nominal condition)
        self._cancelled_acts = set()

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt
        self._last_time_dt = sim_start_dt
        #  keeps track of up to what time activities have been executed ( used as a check for timing)
        self._last_act_execution_time_dt = sim_start_dt

        # holds ref to SatStateSimulator
        self.state_sim = None
        # holds ref to SatScheduleArbiter
        self.arbiter = None
        # holds ref to SatStateRecorder
        self.state_recorder = None

        # keeps track of which data container (data packet) we're on
        self.curr_dc_indx = 0

        #  holds data structures that keep track of execution context for a given activity.  these are all of the data structures that keep track of what has been accomplished in the course of executing a given activity
        self._execution_context_by_exec_act = {}

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

        # the "effectively zero" data volume number
        self.dv_epsilon = dv_epsilon # Mb

    def _pull_schedule(self):

        scheduled_exec_acts_copy = self.scheduled_exec_acts

        # if schedule updates are available, blow away what we had previously (assumption is that arbiter handles changes to schedule gracefully)
        if self.arbiter.schedule_updated:
            self.scheduled_exec_acts = self.arbiter.get_scheduled_executable_acts()
        else:
            return

        # if we updated the schedule, need to deal with the consequences

        # if we're currently executing an act, figure out where that act is in the new sched. That's the "anchor point" in the new sched. (note in general the current act SHOULD be the first one in the new sched. But something may have changed...)
        if len(self._curr_exec_acts) > 0:
            # store most recent (by executable_start) exec act seen. 
            latest_exec_act = scheduled_exec_acts_copy[self._last_exec_act_windex]
            assert(latest_exec_act in self._curr_exec_acts)

            try:
                # search for current act in the scheduled acts
                # curr_act_indx = index_from_key(self.scheduled_exec_acts,key= ea: ea.act,value=latest_exec_act.act)
                # note that this searches by the activity window itself (the hash of the exec act)
                curr_act_indx = self.scheduled_exec_acts.index(latest_exec_act)
            except ValueError:
                # todo: add cleanup?
                # - what does it mean if current act is not in new sched? cancel current act?
                raise RuntimeWarning("Couldn't find activity in new schedule: %s"%(self._curr_exec_act.act))

            #  test if the routing plans for the current activity being executed have changed, we need to do something about that
            updated_exec_act = self.scheduled_exec_acts[curr_act_indx]
            if not latest_exec_act.plans_match(updated_exec_act):
                raise NotImplementedError('Saw updated plans for current ExecutableActivity, current: %s, new: %s'%(latest_exec_act,updated_exec_act))

            self._last_exec_act_windex = curr_act_indx

        # if there are activities in the schedule and we're not currently executing anything, then assume for the moment we're starting from beginning of it (actual index will be resolved in update step)
        elif len(self.scheduled_exec_acts) > 0:
            self._last_exec_act_windex = 0

        # no acts in schedule, for whatever reason (near end of sim scenario, a fault...)
        else:
            # todo: no act, so what is index? Is the below correct?
            self._last_exec_act_windex = None

    @staticmethod
    def executable_time_accessor(exec_act,time_prop):
        if time_prop == 'start':
            return exec_act.act.executable_start
        elif time_prop == 'end':
            return exec_act.act.executable_end
        else:
            raise NotImplementedError

    def update(self,new_time_dt):
        """ Update state of the executive """
        #  todo: should add handling in here for executing multiple activities at one time
        #  todo: this code should probably be moved around a bit at the same time, to make it more straightforward

        # If first step, check the time (first step time should be the sim start time)
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')

        #  if we haven't yet executed the current activities, then we should not be updating activity choices
        if not new_time_dt == self._last_act_execution_time_dt:
            raise RuntimeWarning('Trying to update the executive before executing current activities')

        ##############################
        # determine next activity

        # update schedule if need be
        self._pull_schedule()

        # figure out current activity, index of that act in schedule (note this could return None)
        next_exec_acts,exec_act_windices = find_windows_in_wind_list(new_time_dt,self._last_exec_act_windex,self.scheduled_exec_acts,self.executable_time_accessor)
        self._last_exec_act_windex = exec_act_windices[1]

        # sanity check that state sim has advanced to next timestep (new_time_dt) already
        assert(self.state_sim._curr_time_dt == new_time_dt)
        # if satellite state is not nominal at next timestep (new_time_dt), then that means the activities we are currently executing ( at current time step, self._curr_time_dt) have pushed our state over the edge. for that reason, we should not perform any activities at the next time step
        #  note that once we have canceled an activity, it stays canceled
        if not self.state_sim.nominal_state_check():
            for exec_act in next_exec_acts:
                self._cancelled_acts.add(exec_act)
            #  in this case, remove all of the next acts
            next_exec_acts = []


        # handle execution context setup/takedown
        
        added_exec_acts = set(next_exec_acts) - set(self._curr_exec_acts)
        removed_exec_acts = set(self._curr_exec_acts) - set(next_exec_acts)

        # Deal with any activities that have stopped
        for exec_act in removed_exec_acts:
            self._cleanup_act_execution_context(exec_act,new_time_dt)

        #  set up execution context for any activities that are just beginning
        #  note that we want to clean up before we initialize, because during initialization data structures are set up that may be dependent on an activity that ends in the time step for this new one. If at some point in the future we want to allow two ongoing activities to handle the same data (i.e. second activity grabs data from a first activity while the first is still ongoing), this initialization and cleanup approach will have to be changed. for the time being though, we assume that activities that execute at the same time handle mutually exclusive data
        for exec_act in added_exec_acts:
            self._initialize_act_execution_context(exec_act,new_time_dt)

        self._curr_exec_acts = next_exec_acts        

        self._last_time_dt = self._curr_time_dt
        self._curr_time_dt = new_time_dt

        # mark no longer first step
        if self._first_step:
            self._first_step = False

    def execute_acts(self,new_time_dt):
        """ execute current activities that the update step has chosen """

        #  sanity check that if we're executing activities, we do so with a finite time delta for consistency
        if not self._first_step and not new_time_dt > self._last_act_execution_time_dt:
            raise RuntimeWarning('Saw a zero time difference for activity execution')

        #  advance the activity execution time high-water mark
        self._last_act_execution_time_dt = new_time_dt

        # if it's not the first step, then we can execute current activities
        if not self._first_step:
            for exec_act in self._curr_exec_acts:
                self._execute_act(exec_act,new_time_dt)

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        act = exec_act.act

        if exec_act in self._execution_context_by_exec_act.keys():
            raise RuntimeWarning('trying to re-initialize context for an executive act')

        self._execution_context_by_exec_act[exec_act] = {}

        curr_exec_context = self._execution_context_by_exec_act[exec_act]
        curr_exec_context['act'] = exec_act.act

        #  using the dictionary here both to keep the number of attributes for self down, and to make extension to multiple current execution acts not terrible todo (hehe, sneaky todo there)

        # note that if we want to allow activities to begin at a time different from their actual start, need to update code here
        #  the start time seen for this activity - new_time_dt is the time when the executive notices that we start executing the activity, but we want that account for the fact that the act can start in between timesteps for the executive
        curr_exec_context['start_dt'] = min(exec_act.act.executable_start,new_time_dt)

        # assume for now act ends at the expected end
        curr_exec_context['end_dt'] = exec_act.act.executable_end

        #  received data containers (packets) for this activity
        curr_exec_context['rx_data_conts'] = []
        #  transmitted data containers (packets) for this activity
        curr_exec_context['tx_data_conts'] = []
        #  total amount of data volume used for this activity
        curr_exec_context['dv_used'] = 0

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

        if type(act) is XlnkWindow:
            curr_exec_context['curr_xlnk_txsat_data_cont'] = None

    def _cleanup_act_execution_context(self,exec_act,new_time_dt):

        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        # flag any empty data conts that we transmitted for removal
        self.state_sim.cleanup_data_conts(curr_exec_context['tx_data_conts'])

        #  set the executed properties for the activity
        #  note that we will actually set these twice for cross-links ( TX and RX sat), but that's okay

        curr_act_wind = curr_exec_context['act']

        #  this function is called when we choose to stop the activity. if we stop before the actual end of the activity, then we need to record that new end time.  for now we assume that the activity ends at the new time (as opposed to self._curr_time_dt), because the state simulator update step runs before the executive update step, and has already propagated state to new_time_dt assuming the activity executes until then. See SatStateSimulator.update().  if the state simulator is not run before the executive, this will need to be changed
        curr_exec_context['end_dt'] = new_time_dt
        curr_act_wind.set_executed_properties(curr_exec_context['start_dt'],curr_exec_context['end_dt'],curr_exec_context['dv_used'])
        self.state_recorder.add_act_hist(curr_act_wind)
        
        # delete the current exec context, for safety
        self._execution_context_by_exec_act[exec_act] = None

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

            if remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
                # todo: remove this
                raise RuntimeWarning('insufficient data packet data volume to send over route containers')

        return executable_data_conts_by_rt_cont

    @staticmethod
    def find_act_start_end_times(act_start_dt,act_end_dt,last_time_dt,curr_time_dt,next_time_dt):
        """ find the start and end time to use during this time step, accounting for activity start and end bound boundary conditions"""

        #  in order to allow relatively large time steps (to save computation time), we need to consider if the start/end of an activity falls between time steps for the executive

        # deal with the fact that the start might not fall exactly on a timepoint. If the start occurred between last time and current time, pick that instead of the current time
        assert(act_start_dt <= curr_time_dt)
        if act_start_dt > last_time_dt:
            ts_start_dt = act_start_dt
        else:
            ts_start_dt = curr_time_dt

        assert(act_end_dt >= curr_time_dt)
        #  deal with the fact that the end might not fall exactly on time point. if the end comes before the new time, use the end time
        if act_end_dt <= next_time_dt:
            #  we check the executable end here
            ts_end_dt = act_end_dt
        else:
            ts_end_dt = next_time_dt

        return ts_start_dt, ts_end_dt



    def _execute_act(self,exec_act,new_time_dt):
        # todo: add support for temporally overlapping activities?
        #  this execution code assumes constant average data rate for every activity, which is not necessarily true. In general this should be all right because the planner schedules from the center of every activity, but in future this code should probably be adapted to use the actual data rate at a given time

        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        curr_act_wind = exec_act.act

        #############
        # Figure out time window for execution

        act_start_dt = curr_exec_context['start_dt']
        act_end_dt = curr_exec_context['end_dt'] 
        ts_start_dt, ts_end_dt = self.find_act_start_end_times(act_start_dt,act_end_dt, self._last_time_dt, self._curr_time_dt,new_time_dt)
            
        #  time delta
        delta_t_s = (ts_end_dt - ts_start_dt).total_seconds()

        #  determine how much data volume can be produced or consumed based upon the activity average data rate ( the statement is valid for all types ObsWindow, XlnkWindow, DlnkWindow)
        delta_dv = curr_act_wind.ave_data_rate * delta_t_s

        #  reduce this number by any restrictions due to data volume storage availability
        #  note that storage availability is affected by activity execution order at the current time step ( if more than one activity is to be executed)
        delta_dv = min(delta_dv,self.state_sim.get_available_data_storage(self._curr_time_dt))

        #  the data volume that actually gets collected or lost
        executed_delta_dv = 0

        #############
        #  execute activity

        # if it's an observation window, we are just receiving data
        # note: this is the only place we need to deal with executing observation windows
        if type(curr_act_wind) == ObsWindow:

            # if we just started this observation, then there are no data containers (packets). Make a new one and append
            if len(curr_exec_context['rx_data_conts']) == 0:
                curr_data_cont = SimDataContainer(self.sim_sat.sat_id,self.sim_sat.sat_indx,self.curr_dc_indx,route=curr_act_wind,dv=0)
                self.curr_dc_indx += 1
                curr_exec_context['rx_data_conts'].append(curr_data_cont)
            else:
                curr_data_cont = curr_exec_context['rx_data_conts'][-1]

            # delta_dv = curr_act_wind.ave_data_rate * delta_t_s
            # todo add delta dv to DS state

            curr_data_cont.add_dv(delta_dv)
            curr_exec_context['dv_used'] += delta_dv
            executed_delta_dv += delta_dv

        #  deal with cross-link if we are the transmitting satellite
        #  note: this code only deals with execution by the transmitting satellite. execution by the receiving satellite is dealt with within xlnk_receive_poll()
        elif type(curr_act_wind) == XlnkWindow:

            xsat_indx = curr_act_wind.get_xlnk_partner(self.sim_sat.sat_indx)
            xsat_exec = self.sim_sat.get_sat_from_indx(xsat_indx).get_exec()
            is_tx = not curr_act_wind.is_rx(self.sim_sat.sat_indx)

            #  if we're the transmitting satellite, then we have the responsibility to start the data transfer transaction
            if is_tx:
                # tx_delta_dv = curr_act_wind.ave_data_rate * delta_t_s
                tx_delta_dv = delta_dv

                # while we still have delta dv to transmit
                while tx_delta_dv > self.dv_epsilon:
                
                    # figure out data cont, amount of data to transmit
                    tx_dc, dv_to_send = self.select_tx_data_cont(curr_exec_context['executable_tx_data_conts_by_rt_cont'],tx_delta_dv,self.dv_epsilon)

                    # send data to receiving satellite
                    #  note that once this receive poll returns a failure, we should not attempt to continue transmitting to the satellite ( subsequent attempts will not succeed in sending data)
                    dv_txed,tx_success = xsat_exec.xlnk_receive_poll(ts_start_dt,ts_end_dt,new_time_dt,curr_act_wind,self.sim_sat.sat_indx,tx_dc,dv_to_send)

                    #  if there was a successful reception of the data,
                    if tx_success:
                        tx_dc.remove_dv(dv_txed)
                        # todo subtract delta dv from DS state
                        if not tx_dc in curr_exec_context['tx_data_conts']:
                            curr_exec_context['tx_data_conts'].append(tx_dc)

                    #  if we did not successfully transmit, the receiving satellite is unable to accept more data at this time
                    else:
                        # todo: added this temporarily for debugging purposes -  remove once functionality verified!
                        raise RuntimeWarning('saw failure to receive')
                        break

                    tx_delta_dv -= dv_txed

                    curr_exec_context['dv_used'] += dv_txed

                    #  because  we are transmitting, we are losing data volume
                    executed_delta_dv -= dv_txed

        elif type(curr_act_wind) == DlnkWindow:
            pass

        #  otherwise, we don't know what to do with this activity type
        else:
            raise NotImplementedError

        all_data_conts = curr_exec_context['tx_data_conts'] + curr_exec_context['rx_data_conts']
        self.state_sim.add_to_data_storage(executed_delta_dv,all_data_conts,self._curr_time_dt)


    @staticmethod
    def select_tx_data_cont(exec_data_conts_by_rt_cont,tx_dv_possible,dv_epsilon):
        """Determines which data container (packet) to send based on plans (route containers) for given execution context"""

        #  figure out which data container to send based upon planned data transmission
        exec_dc_choice = None
        for rc,exec_data_conts in exec_data_conts_by_rt_cont.items():
            # each exec_dc is of type ExecutableDataContainer -  a record of how much data volume to transmit from a given data container for a given route container
            for exec_dc in exec_data_conts:
                #  if there is still remaining data volume for this data container, then choose it
                if exec_dc.remaining_dv > dv_epsilon:
                    exec_dc_choice = exec_dc
                    break

            if exec_dc_choice:
                break

        #  the amount of data volume to send is the minimum of how much we can send during current activity, how much we plan to send for a given data container, and how much data is available from that data container
        dv_to_send = min(tx_dv_possible,exec_dc_choice.remaining_dv,exec_dc_choice.data_vol)

        # sanity check -  if we had planned to send a certain amount of data volume from a data container, we should have that data volume present in the data container
        if exec_dc_choice.remaining_dv > exec_dc_choice.data_vol:
            raise RuntimeWarning('Thought there was more data to send than is actually present in data container')

        #  this subtracts data volume from the executable data container ( not the data container itself!)
        exec_dc_choice.remaining_dv -= dv_to_send

        return exec_dc_choice.data_cont,dv_to_send


    def xlnk_receive_poll(self,tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,xsat_indx,txsat_data_cont,proposed_dv):
        """Called by a transmitting satellite to see if we can receive data and if yes, handles the received data
        
        [description]
        :param ts_start_dt: the start time for this transmission, as calculated by the transmitting satellite
        :type ts_start_dt: datetime
        :param ts_end_dt: the end time for this transmission, as calculated by the transmitting satellite
        :type ts_end_dt: datetime
        :param proposed_act:  the activity which the transmitting satellite is executing
        :type proposed_act: XlnkWindow
        :param xsat_indx:  the index of the transmitting satellite
        :type xsat_indx: int
        :param txsat_data_cont:  the data container that the transmitting satellite is sending ( with route only up to the transmitting satellite)
        :type txsat_data_cont: SimDataContainer
        :param proposed_dv:  the data volume that the transmitting satellite is trying to send
        :type proposed_dv: float
        :returns:  data volume received, success flag
        :rtype: {int,bool}
        :raises: RuntimeWarning, RuntimeWarning, RuntimeWarning
        """

        # note: do not modify txsat_data_cont in here!
        #  note that a key assumption about this function is that once it indicates a failure to receive data, subsequent calls at the current time step will not be successful ( and therefore the transmitter does not have to continue trying)

        #  verify of the right type
        if not type(proposed_act) == XlnkWindow:
            raise RuntimeWarning('saw a non-xlnk window')

        #  if this satellite is not actually executing the activity, then it can't receive data
        # note that both satellites have to be referring to the same activity 
        if not proposed_act in [exec_act.act for exec_act in self._curr_exec_acts]:
            received_dv = 0
            rx_success = False
            return received_dv,rx_success

        #  sanity check that we're actually a receiver during this activity
        if not proposed_act.is_rx(self.sim_sat.sat_indx):
            raise RuntimeWarning('cross-link attempt with non-receiver satellite')
            
        #############
        # Figure out time window for reception execution, determine how much data can be received

        # get this sat's version of the executable activity
        self_exec_act = self._curr_exec_acts[index_from_key(self._curr_exec_acts,key=lambda ea:ea.act,value=proposed_act)]
        curr_exec_context = self._execution_context_by_exec_act[self_exec_act]

        act_start_dt = curr_exec_context['start_dt']
        act_end_dt = curr_exec_context['end_dt'] 
        rx_ts_start_dt, rx_ts_end_dt = self.find_act_start_end_times(act_start_dt, act_end_dt, self._last_time_dt, self._curr_time_dt,new_time_dt)

        ts_start_dt = max(tx_ts_start_dt,rx_ts_start_dt)
        ts_end_dt = min(tx_ts_end_dt,rx_ts_end_dt)

        #  if this is not true, that means the two time periods for the satellites are not overlapping, and the executives likely have different current times. that should not be allowed in the simulation. Note that will assume they are both using the same time step and so the transmitting satellite that is polling this satellite integrates its data rate over the same amount of time
        if not ts_end_dt >= ts_start_dt:
            raise RuntimeWarning('time mismatch between satellites')

        rx_delta_dv = proposed_act.ave_data_rate * (ts_end_dt-ts_start_dt).total_seconds()

        #  reduce this number by any restrictions due to data volume storage availability
        #  note that storage availability is affected by activity execution order at the current time step ( if more than one activity is to be executed)
        rx_delta_dv = min(rx_delta_dv,self.state_sim.get_available_data_storage(self._curr_time_dt))

        received_dv = min(proposed_dv,rx_delta_dv)


        # todo: need to add planning info exchange too
        # todo: need to add recording of tlm, cmd update
        # todo: check that DS buffer not too full
        # todo add delta dv to DS state
        # todo: add checking of how much dv we can receive based on DS state. The rest of the code should only happen if received dv still > 0

        #  if we're not able to receive data, then return immediately
        if received_dv < self.dv_epsilon:
            rx_success = False
            return received_dv,rx_success

        #############
        # Store the incoming data in the appropriate place

        #  check if the transmitted data container has changed or not
        tx_data_cont_changed = False
        if not txsat_data_cont == curr_exec_context['curr_xlnk_txsat_data_cont']:
            curr_exec_context['curr_xlnk_txsat_data_cont'] = txsat_data_cont
            tx_data_cont_changed = True

        #  ingest the data received into the appropriate destination
        self.curr_dc_indx = self.ingest_rx_data(
            curr_exec_context['rx_data_conts'],
            proposed_act,
            txsat_data_cont,
            received_dv,
            self.sim_sat.sat_id,
            xsat_indx,
            self.curr_dc_indx,
            tx_data_cont_changed
        )


        #  Mark the data volume received
        curr_exec_context['dv_used'] += received_dv

        #  factor this executed Delta into the total data volume Delta for this time step
        self.state_sim.add_to_data_storage(received_dv,curr_exec_context['rx_data_conts'],self._curr_time_dt)

        return received_dv,True

    @staticmethod
    def ingest_rx_data(rx_data_conts_list,act,txsat_data_cont,received_dv,rx_id,tx_sat_indx,dc_indx,tx_data_cont_changed):
        """Ingest received data, and do the process of putting that data into its proper data container destination"""

        #  if we've already been receiving data from this transmitted data container, then we should still receive into the same reception data container
        if not tx_data_cont_changed:
            rx_dc = rx_data_conts_list[-1]
        #  if the transmitted data container has changed, then we can make a new reception data container
        else:
            new_rx_dc = txsat_data_cont.fork(rx_id,new_dc_indx=dc_indx)
            dc_indx += 1
            new_rx_dc.add_to_route(act,tx_sat_indx)
            rx_data_conts_list.append(new_rx_dc)
            rx_dc = new_rx_dc

        rx_dc.add_dv(received_dv)

        return dc_indx

    def get_acts_at_time(self,time_dt):

        if abs(time_dt - self._curr_time_dt) < self.sim_sat.time_epsilon_td:
            return [exec_act.act for exec_act in self._curr_exec_acts]
        else:
            raise NotImplementedError


class SatStateRecorder(StateRecorder):
    """Convenient interface for storing state history for satellites"""

    def __init__(self,sim_start_dt,state_recorder_params):
        self.base_time = sim_start_dt
        self.ES_state_hist = [] # assumed to be in Wh
        self.DS_state_hist = [] # assumed to be in Mb
        self.act_hist = []

        super().__init__()    

    def add_ES_hist(self,t_dt,val):
        # todo: add decimation?
        t_s = (t_dt - self.base_time).total_seconds()
        self.ES_state_hist.append((t_s,val))

    def add_DS_hist(self,t_dt,val):
        # todo: add decimation?
        t_s = (t_dt - self.base_time).total_seconds()
        self.DS_state_hist.append((t_s,val))

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


    def get_DS_hist(self,out_units='minutes'):

        if not out_units == 'minutes':
            raise NotImplementedError

        t = []
        d = []
        for pt in self.DS_state_hist:
            t.append(pt[0]/60.0) # converted to minutes
            d.append(pt[1]) # converted to Gb

        return t,d



