from random import normalvariate

from .sim_agent_components import ScheduleArbiter, StateRecorder, PlanningInfoDB
from circinus_tools  import io_tools

class SatScheduleArbiter(ScheduleArbiter):
    """Handles ingestion of new schedule artifacts from ground planner and their deconfliction with onboard updates. Calls the LP"""

    def __init__(self):
        super().__init__()

        self.plan_db = PlanningInfoDB()

    def ingest_routes(self,rt_conts):
        self.plan_db.add_routes(rt_conts)

    def get_scheduled_acts(self):
        pass


class SatExecutive:
    """Handles execution of scheduled activities, with the final on whether or not to adhere exactly to schedule or make changes necessitated by most recent state estimates """

    def __init__(self):
        pass

class SatStateSimulator:
    """Simulates satellite state, holding internally any state variables needed for the process"""

    def __init__(self,start_time_dt,state_simulator_params,sat_power_params,sat_data_storage_params,sat_initial_state,sat_event_data):
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

        self.ecl_winds = sat_event_data['ecl_winds']
        self.ecl_winds.sort(key = lambda w: w.start)
        self.curr_ecl_windex = 0

    def in_eclipse(self,curr_time_dt):
        # move current eclipse window possibility forward if we're past it, and we're not yet at end of ecl winds
        # -1 so we only advance if we're not yet at the end
        while self.curr_ecl_windex < len(self.ecl_winds)-1 and  curr_time_dt > self.ecl_winds[self.curr_ecl_windex].end:
            self.curr_ecl_windex += 1

        curr_ecl_wind = self.ecl_winds[self.curr_ecl_windex]
        if self.curr_ecl_windex >= curr_ecl_wind.start and self.curr_ecl_windex <= curr_ecl_wind.end:
            return True

    def update_state(new_time_dt):

        # this is consistent with power units above
        delta_t_h = (new_time_dt - self.curr_time_dt).total_seconds()/3600

        current_act = blah


        ########################
        # Energy storage update

        #  base-level satellite energy usage (not including additional activities)
        base_edot = model.par_sats_edot_by_mode[sat_indx]['base']

        charging = True
        if self.in_eclipse(self.curr_time_dt):
            charging = False

        # add in charging energy contribution (if present)
        # assume charging is constant in sunlight
        charging_edot = model.par_sats_edot_by_mode[sat_indx]['orbit_insunlight_average_charging'] if charging else 0

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


        self.curr_time_dt = new_time_dt

        # # Add to state history
        # # Decmiated by STATE_HIST_DEC_FACTOR
        # if self.state_history_count == 0:
        #     self.time_history.append(self.time)

        #     # stored in megabytes and W-Hr (why did I do it that way? No idea)
        #     self.ES_state_history.append(self.ES_state/E_CONV_FACTOR)


class SatStateRecorder(StateRecorder):

    def __init__(self):
        super().__init__()

        pass