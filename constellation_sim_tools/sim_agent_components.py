#  Contains objects for different software components running on agents. these are intended to be components that are used across both satellites and ground stations
#
# @author Kit Kennedy

from collections import namedtuple

from circinus_tools import io_tools
from circinus_tools.sat_state_tools import propagate_sat_ES
from circinus_tools.scheduling.schedule_tools  import synthesize_executable_acts

class PlannerScheduler:
    """ superclass for planning/scheduling elements running on ground and satellites"""

    def __init__(self,sim_start_dt,sim_end_dt):
        
        self.plan_db = PlanningInfoDB(sim_start_dt,sim_end_dt)

    def get_plan_db(self):
        return self.plan_db


class StateRecorder:
    """ records state for an agent"""

    def __init__(self):
        pass

class DataStore:
    """ Database for data containers (packets) stored on an agent"""
    #  add tracking of data store history?

    def __init__(self):

        #  database for data containers currently within the data store
        self.data_conts_by_id = {}

    def add(self,data_conts):
        """Add data containers (packets) to the data store"""

        for dc in data_conts:
            if dc in self.data_conts_by_id.keys():
                raise RuntimeWarning('Data container already exists within data store (%s)'%(dc))

            self.data_conts_by_id[dc.ID] = dc

    def cleanup(self,data_conts,dv_epsilon = 0.001):
        """ remove any data containers that have effectively zero data volume"""

        # dv_epsilon  assumed to be in Mb

        for dc in data_conts:
            if not dc.ID in self.data_conts_by_id.keys():
                continue

            if dc.data_vol < dv_epsilon:
                del self.data_conts_by_id[dc.ID]


    def get_curr_data_conts(self):
        """ get the data containers currently in the database"""
        return list(self.data_conts_by_id.values())


#  record of satellite state. update_dt is the date time at which this state was valid.
SatStateEntry = namedtuple('SatStateEntry','update_dt state_info')

class PlanningInfoDB:
    """database for information relevant for planning and scheduling on any agent"""

    filter_opts = ['totally_within','partially_within']
    expected_state_info = {'batt_e_Wh'}  #  this is set notation

    def __init__(self,sim_start_dt,sim_end_dt):
        # todo: make this a dictionary by sat index? Could be a bit faster
        # todo: should add distinction between active and old routes
        self.sim_rt_conts_by_id = {}  # The id here is the Sim route container 
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt

        #  holds list of satellite IDs in order
        self.sat_id_order = None

        #  contains state information history for every satellite
        self.sat_state_hist_by_id = {}  # the id here is the satellite
        #  contains information about events that happen on satellites ( outside of scheduled activities), as well as other general satellite specific information needed for keeping track of planning info
        self.sat_events = {}
        #  holds power parameters for each satellite ID,  after parsing
        self.parsed_power_params_by_sat_id = {}

    def initialize(self,plan_db_inputs):
        self.sat_id_order = plan_db_inputs['sat_id_order']
        self.resource_delta_t_s = plan_db_inputs['resource_delta_t_s']
        self.sat_events['ecl_winds_by_sat_id'] = {}

        for sat_id in self.sat_id_order:
            #  store eclipse windows for each satellite ID
            self.sat_events['ecl_winds_by_sat_id'][sat_id] = plan_db_inputs['ecl_winds_by_sat_id'][sat_id]

            state_keys_input = plan_db_inputs['initial_state_by_sat_id'][sat_id].keys()
            #  ensure that the set of expected state info keys is a subset of the provided keys ( everything expected is there)
            assert(self.expected_state_info < set(state_keys_input))

            #  store first state history element for each satellite ID. this needs to be time tagged because it will be updated throughout the sim
            self.sat_state_hist_by_id[sat_id] = []
            self.sat_state_hist_by_id[sat_id].append(SatStateEntry(update_dt=self.sim_start_dt, state_info=plan_db_inputs['initial_state_by_sat_id'][sat_id]))

            self.parsed_power_params_by_sat_id[sat_id] = {}
            sat_edot_by_mode,sat_batt_storage,power_units = io_tools.parse_power_consumption_params(plan_db_inputs['power_params_by_sat_id'][sat_id])
            self.parsed_power_params_by_sat_id[sat_id] = {
                "sat_edot_by_mode": sat_edot_by_mode,
                "sat_batt_storage": sat_batt_storage,
                "power_units": power_units,
            } 

    def update_routes(self,rt_conts):
        for rt_cont in rt_conts:
            if rt_cont.ID in self.sim_rt_conts_by_id.keys():
                # todo: is this the way to always do updates?
                if rt_cont.update_dt > self.sim_rt_conts_by_id[rt_cont.ID].update_dt:
                    self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont
            else:
                self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont

    def get_filtered_sim_routes(self,filter_start_dt,filter_end_dt=None,filter_opt='partially_within',sat_id=None):
        if not filter_end_dt:
            filter_end_dt = self.sim_end_dt

        if not filter_opt in self.filter_opts:
            raise NotImplementedError

        def do_filter(rt_cont):
            if sat_id is not None:
                #  check if the route container includes the satellite ID
                if not rt_cont.has_sat_indx(self.sat_id_order.index(sat_id)): 
                    return False

            if filter_opt == 'totally_within':
                if (rt_cont.start < filter_start_dt or rt_cont.end > filter_end_dt):
                    return False
            if filter_opt == 'partially_within':
                if (rt_cont.end < filter_start_dt or rt_cont.start > filter_end_dt):
                    return False
            return True

        all_rt_conts = []
        for rt_cont in self.sim_rt_conts_by_id.values():
            if do_filter(rt_cont):
                all_rt_conts.append(rt_cont)

        return all_rt_conts

    def push_planning_info(self,other):
        """ Update other with planning information from self"""

        other.update_routes(self.sim_rt_conts_by_id.values())

    def get_sat_states(self, curr_time_dt):
        """ get satellite states at the input time. Propagates each satellite's state forward from its last known state to the current time. Includes any known scheduled activities in this propagation"""

        curr_sat_state_by_id = {}


        for sat_indx,sat_id in enumerate(self.sat_id_order):
            curr_sat_state = {}

            #  get most recent update of satellite state. known_sat_state is a SatStateEntry type
            known_sat_state = self.sat_state_hist_by_id[sat_id][-1]
            last_update_dt = known_sat_state.update_dt

            # get scheduled/executable activities between state update time and current time but have the satellite ID somewhere along the route
            rt_conts = self.get_filtered_sim_routes(filter_start_dt=last_update_dt,filter_end_dt=curr_time_dt,filter_opt='partially_within',sat_id=sat_id)
            #  get all the windows that are executable from all of the route containers, filtered for this satellite ( also filtering on the relevant time window)
            # using a set to ensure unique windows ( there can be duplicate windows across route containers)
            # executable_acts = set() 
            # for rt_cont in rt_conts:
            #     executable_acts = executable_acts.union(rt_cont.get_winds_executable(filter_start_dt=last_update_dt,filter_end_dt=curr_time_dt,sat_indx=sat_indx))
            # # sort executable windows by start time
            # executable_acts = list(executable_acts)
    
            #  synthesizes the list of unique activities to execute, with the correct execution times and data volumes on them
            executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=last_update_dt,filter_end_dt=curr_time_dt,sat_indx=sat_indx)
            #  strip out the executable activity objects and leave just the windows
            executable_acts_just_winds = [exec_act.act for exec_act in executable_acts]
            executable_acts_just_winds.sort(key = lambda wind: wind.executable_start)

            # get eclipse Windows
            ecl_winds = self.sat_events['ecl_winds_by_sat_id'][sat_id]

            curr_ES_state = known_sat_state.state_info['batt_e_Wh']
            curr_sat_state['batt_e_Wh'],ES_state_went_below_min = propagate_sat_ES(last_update_dt,curr_time_dt,sat_indx,curr_ES_state,executable_acts_just_winds,ecl_winds,self.parsed_power_params_by_sat_id[sat_id],self.resource_delta_t_s)

            curr_sat_state_by_id[sat_id] = curr_sat_state

        return curr_sat_state_by_id

    def get_ecl_winds(self, sat_id):
        return self.sat_events['ecl_winds_by_sat_id'][sat_id]


