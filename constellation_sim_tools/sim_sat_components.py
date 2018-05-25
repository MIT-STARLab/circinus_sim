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
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools

class SatStateSimulator(StateSimulator):
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

        super().__init__(sim_sat)

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

    def get_available_data_storage(self,time_dt,dv_desired=None):
        # note: for the satellite we don't care about dv_desired

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


class SatScheduleArbiter(ExecutiveAgentPlannerScheduler):
    """Handles ingestion of new schedule artifacts from ground planner and their deconfliction with onboard updates. Calls the LP"""

    def __init__(self,sim_sat,sim_start_dt,sim_end_dt):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        super().__init__(sim_sat,sim_start_dt,sim_end_dt)

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
        #  the list elements are of type ExecutableActivity
        #  filter rationale:  we may be in the middle of executing an activity, and we want to preserve the fact that that activity is in the schedule. so if a window is partially for current time, but ends after current time, we still want to consider it an executable act
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,filter_opt='partially_within',sat_indx=self.sim_sat.sat_indx)

        # sort executable activities by start time
        executable_acts.sort(key = lambda ex_act: ex_act.act.executable_start)

        self._schedule_updated = True
        self._planning_info_updated = False
        self._schedule_cache = executable_acts

        self._curr_time_dt = new_time_dt

class SatExecutive(Executive):
    """Handles execution of scheduled activities, with the final on whether or not to adhere exactly to schedule or make changes necessitated by most recent state estimates """

    def __init__(self,sim_sat,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        super().__init__(sim_sat,sim_start_dt,dv_epsilon)

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ sets up context dictionary for activity execution on the satellite"""

        curr_exec_context = super()._initialize_act_execution_context(exec_act,new_time_dt)

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

        #  returning this not because it's expected to be used, but to be consistent with superclass
        return curr_exec_context

    def _cleanup_act_execution_context(self,exec_act,new_time_dt):

        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        # flag any empty data conts that we transmitted for removal
        self.state_sim.cleanup_data_conts(curr_exec_context['tx_data_conts'])

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

            if remaining_dv_by_rt_cont[curr_rc] > dv_epsilon:
                # todo: remove this
                raise RuntimeWarning('insufficient data packet data volume to send over route containers')

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
            gs_indx = curr_act_wind.gs_indx
            gs_exec = self.sim_sat.get_gs_from_indx(gs_indx).get_exec()

            # note that sat is always transmitting, if it's a downlink. So sat has responsibility to start the data transfer transaction
            tx_delta_dv = delta_dv

            # while we still have delta dv to transmit
            while tx_delta_dv > self.dv_epsilon:
            
                # figure out data cont, amount of data to transmit
                tx_dc, dv_to_send = self.select_tx_data_cont(curr_exec_context['executable_tx_data_conts_by_rt_cont'],tx_delta_dv,self.dv_epsilon)

                # send data to receiving satellite
                #  note that once this receive poll returns a failure, we should not attempt to continue transmitting to the satellite ( subsequent attempts will not succeed in sending data)
                dv_txed,tx_success = gs_exec.dlnk_receive_poll(ts_start_dt,ts_end_dt,new_time_dt,curr_act_wind,self.sim_sat.sat_indx,tx_dc,dv_to_send)

                #  if there was a successful reception of the data,
                if tx_success:
                    tx_dc.remove_dv(dv_txed)
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


    def xlnk_receive_poll(self,tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,tx_sat_indx,txsat_data_cont,proposed_dv):
        """Called by a transmitting satellite to see if we can receive data over a crosslink and if yes, handles the received data"""
        
        # See documentation in receive_poll in sim_agent_components.py

        # note: do not modify txsat_data_cont in here!

        #  verify of the right type
        if not type(proposed_act) == XlnkWindow:
            raise RuntimeWarning('saw a non-xlnk window')

        #  sanity check that we're actually a receiver during this activity
        if not proposed_act.is_rx(self.sim_sat.sat_indx):
            raise RuntimeWarning('cross-link attempt with non-receiver satellite')
            
        return self.receive_poll(tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,tx_sat_indx,txsat_data_cont,proposed_dv)

    def get_acts_at_time(self,time_dt):

        if abs(time_dt - self._curr_time_dt) < self.sim_sat.time_epsilon_td:
            return [exec_act.act for exec_act in self._curr_exec_acts]
        else:
            raise NotImplementedError

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


