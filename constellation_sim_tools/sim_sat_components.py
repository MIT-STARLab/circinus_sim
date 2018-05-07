from random import normalvariate

from circinus_tools  import io_tools
from circinus_tools.scheduling.base_window  import find_window_in_wind_list
from .sim_agent_components import ScheduleArbiter, StateRecorder, PlanningInfoDB

class SatScheduleArbiter(ScheduleArbiter):
    """Handles ingestion of new schedule artifacts from ground planner and their deconfliction with onboard updates. Calls the LP"""

    def __init__(self,sim_sat,start_time_dt):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        self.plan_db = PlanningInfoDB()

        self._schedule_updated = False
        self._schedule_cache = []

        self.curr_time_dt = start_time_dt

        super().__init__()

    @property
    def schedule_updated(self):
        return self._schedule_updated

    def ingest_routes(self,rt_conts):
        self.plan_db.add_routes(rt_conts)

    def update_schedule(self):

        rt_conts = self.plan_db.get_sim_routes(self.curr_time_dt)

        all_executable_winds = []
        for rt_cont in rt_conts:
            all_executable_winds += simrc.get_winds_executable()

        executable_winds = []
        for wind in all_executable_winds:
            if wind.has_sat_indx(self.sim_sat.sat_indx):
                executable_winds.append(wind)

        executable_winds.sort(key = lambda wind: wind.executable_start)

        self._schedule_updated = True

    def get_scheduled_acts(self):
        self._schedule_updated = False

        return self._schedule_cache


class SatExecutive:
    """Handles execution of scheduled activities, with the final on whether or not to adhere exactly to schedule or make changes necessitated by most recent state estimates """

    def __init__(self,sim_sat,start_time_dt):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        self.scheduled_acts = []
        self._current_act = None
        self._current_act_windex = None

        self.curr_time_dt = start_time_dt

        # holds ref to SatStateSimulator, eventually
        self.sat_state_sim = None
        # holds ref to SatScheduleArbiter, eventually
        self.sat_arbiter = None


    def pull_schedule(self):

        # if schedule updates are available, blow away what we had previously (assumption is that arbiter handles changes to schedule gracefully)
        if self.sat_arbiter.schedule_updated:
            self.scheduled_acts = self.sat_arbiter.get_scheduled_acts()
        else:
            return

        # if we updated the schedule, need to deal with the consequences

        # if we're currently executing an act, figure out where that act is in the new sched. That's the "anchor point" in the new sched. (note in general the current act SHOULD be the first one in the new sched. But something may have changed...)
        if not self._current_act is None:
            try:
                curr_act_indx = self.scheduled_acts.index(self._current_act)
            except ValueError:
                # todo: add cleanup?
                # - what does it mean if current act is not in new sched? cancel current act?
                raise NotImplementedError("Haven't yet implemented schedule changes mid-activity")

            self._current_act_windex = curr_act_indx

        # if there are activities in the schedule, then assume we're starting from beginning of it
        elif len(self.scheduled_acts) > 0:
            self._current_act_windex = 0

        # no acts in schedule, for whatever reason (near end of sim scenario, a fault...)
        else:
            # todo: no act, so what is index? Is the below correct?
            self._current_act_windex = None

    @staticmethod
    def executable_time_accessor(wind,time_prop):
        if time_prop == 'start':
            return wind.executable_start
        elif time_prop == 'end':
            return wind.executable_end

    def update(self,new_time_dt):
        """ Update state of the executive """

        # update schedule if need be
        self.pull_schedule()

        # figure out current activity, index of that act in schedule (note this could return None)
        self._current_act,self._current_act_windex = find_window_in_wind_list(new_time_dt,self._current_act_windex,self.scheduled_acts,self.executable_time_accessor)

        # todo: add some hysteresis here? That is, a cool down time after returning to nominal state before deciding to execute another act?
        if not self.sat_state_sim.nominal_state_check():
            self._current_act = None

        self.curr_time_dt = new_time_dt

    def get_act_at_time(self,time_dt):

        if abs(time_dt - self.curr_time_dt) < self.sim_sat.time_epsilon_td:
            return self._current_act
        else:
            raise NotImplementedError


class SatStateSimulator:
    """Simulates satellite system state, holding internally any state variables needed for the process. This state includes things like energy storage, ADCS modes/pointing state (future work)"""

    def __init__(self,sim_sat,start_time_dt,state_simulator_params,sat_power_params,sat_data_storage_params,sat_initial_state,sat_event_data):
        # holds ref to the containing sim sat
        self.sim_sat = sim_sat

        self.ES_state = sat_initial_state['batt_e_Wh']

        self.sat_edot_by_mode,self.sat_batt_storage,power_units = io_tools.parse_power_consumption_params(sat_power_params)

        if not power_units['power_consumption'] == 'W':
            raise NotImplementedError
        if not power_units['battery_storage'] == 'Wh':
            raise NotImplementedError

        # todo: should current time only be stored on sat agent simulator?
        self.curr_time_dt = start_time_dt

        self.es_update_add_noise = state_simulator_params['es_state_update']['add_noise']
        self.es_noise_params = state_simulator_params['es_state_update']['noise_params']

        # we track eclipse windows here because they're not actually scheduled, they're just events that happen
        self.ecl_winds = sat_event_data['ecl_winds']
        self.ecl_winds.sort(key = lambda w: w.start)
        self._curr_ecl_windex = 0

        # holds ref to SatExecutive, eventually
        self.sat_exec = None

    def in_eclipse(self,time_dt):
        # move current eclipse window possibility forward if we're past it, and we're not yet at end of ecl winds
        # -1 so we only advance if we're not yet at the end
        # while self._curr_ecl_windex < len(self.ecl_winds)-1 and  time_dt > self.ecl_winds[self._curr_ecl_windex].end:
        #     self._curr_ecl_windex += 1

        # curr_ecl_wind = self.ecl_winds[self._curr_ecl_windex]
        # if self._curr_ecl_windex >= curr_ecl_wind.start and self._curr_ecl_windex <= curr_ecl_wind.end:
        #     return True

        curr_ecl_wind,self._curr_ecl_windex = find_window_in_wind_list(time_dt,self._curr_ecl_windex,self.ecl_winds)

        if curr_ecl_wind:
            return True

    def update(self,new_time_dt):
        """ Update state to new time by propagating state forward from last time to new time"""

        if new_time_dt < self.curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # this is consistent with power units above
        delta_t_h = (new_time_dt - self.curr_time_dt).total_seconds()/3600

        current_act = self.sat_exec.get_act_at_time(self.curr_time_dt)


        ########################
        # Energy storage update

        act_edot = 0
        if current_act:
            sat_indx = self.sim_sat.sat_indx
            act_edot = current_act.get_code(sat_indx) if type(current_act) == XlnkWindow else current_act.get_code()

        #  base-level satellite energy usage (not including additional activities)
        base_edot = self.sat_edot_by_mode['base']

        charging = True
        if self.in_eclipse(self.curr_time_dt):
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

        # deal with cases where noise puts us above max batt storage, which we'll assume is physically impossible
        self.ES_state = min(self.ES_state,self.sat_batt_storage['e_max'])

        # note: don't treat min battery storage the same way - but do raise warning if less than 0...
        if self.ES_state < 0:
            raise RuntimeWarning('ES_state went below 0 for sat %s'%(self))


        # todo: add in DS stuff?


        self.curr_time_dt = new_time_dt

        # # Add to state history
        # # Decmiated by STATE_HIST_DEC_FACTOR
        # if self.state_history_count == 0:
        #     self.time_history.append(self.time)

        #     # stored in megabytes and W-Hr (why did I do it that way? No idea)
        #     self.ES_state_history.append(self.ES_state/E_CONV_FACTOR)

    def nominal_state_check(self):
        # if we're below lower energy bound, state is off nominal
        if self.ES_state < self.sat_batt_storage['e_min']:
            return False

        return True

class SatStateRecorder(StateRecorder):

    def __init__(self):
        super().__init__()

        pass