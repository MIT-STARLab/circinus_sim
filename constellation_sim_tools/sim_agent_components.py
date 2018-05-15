from collections import namedtuple

class PlannerScheduler:

    def __init__(self,sim_start_dt,sim_end_dt,sats_event_data):
        
        self.plan_db = PlanningInfoDB(sim_start_dt,sim_end_dt,sats_event_data)

    def get_plan_db(self):
        return self.plan_db


class StateRecorder:

    def __init__(self):
        pass

#  record of satellite state. update_dt is the date time at which this state was valid.
SatStateEntry = namedtuple('SatStateEntry','update_dt state_info')

class PlanningInfoDB:

    filter_opts = ['totally_within','partially_within']

    def __init__(self,sim_start_dt,sim_end_dt,sats_event_data):
        # todo: make this a dictionary by sat index? Could be a bit faster
        # todo: should add distinction between active and old routes
        self.sim_rt_conts_by_id = {}  # The id here is the Sim route container 
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt
        self.sat_state_by_id = {}  # the id here is the satellite

        #  contains information about events that happen on satellites ( outside of scheduled activities), as well as other general satellite specific information needed for keeping track of planning info
        self.sat_events = {}
        self.sat_events['ecl_winds_by_sat_id'] = sats_event_data['ecl_winds_by_sat_id']

        # todo: should this be initialized?
        for sat_id in sats_event_data['sat_id_order']:
            self.sat_state_by_id[sat_id] = []

    def update_routes(self,rt_conts):
        for rt_cont in rt_conts:
            if rt_cont.ID in self.sim_rt_conts_by_id.keys()
                # todo: is this the way to always do updates?
                if rt_cont.update_dt > self.sim_rt_conts_by_id[rt_cont.ID].update_dt:
                    self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont
            else:
                self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont

    def get_filtered_sim_routes(self,start_time_dt,end_time_dt=None,filter_opt='partially_within'):

        if not end_time_dt:
            end_time_dt = self.sim_end_dt

        if not filter_opt in filter_opts:
            raise NotImplementedError

        passes_filter(rt_cont):
            if filter_opt == 'totally_within':
                if (rt_cont.start < start_time_dt or rt_cont.end > end_time_dt):
                    return False
            if filter_opt == 'partially_within':
                if (rt_cont.end < start_filt_dt or rt_cont.start > end_time_dt):
                    return False
            return True

        all_rt_conts = []
        for rt_cont in self.sim_rt_conts_by_id.values():
            if passes_filter(rt_cont):
                all_rt_conts.append(rt_cont)

        return all_rt_conts

    def push_planning_info(self,other):
        """ Update other with planning information from self"""

        other.update_routes(self.sim_rt_conts_by_id.values())

    def get_sat_states(curr_time_dt):

        curr_sat_state_by_id = {}

        for sat_id in sats_event_data['sat_id_order']:
            curr_sat_state = {}
            # known_sat_state is a SatStateEntry type
            known_sat_state = self.sat_state_by_id[sat_id][-1]

            as todo: add this function, somewhere...
            need scheduled activities too for this...
            should this be done in GP?
            curr_sat_state['batt_e_Wh'] = propagate_sat_ES(known_sat_state.update_dt,curr_time_dt,known_sat_state.state_info['batt_e_Wh'])

            curr_sat_state_by_id[sat_id] = curr_sat_state

        return curr_sat_state_by_id


