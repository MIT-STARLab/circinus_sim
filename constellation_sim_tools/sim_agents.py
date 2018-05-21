#  Contains objects  that are used in simulation loop to represent high-level agents within simulation, including satellites, ground stations, and the ground station network
#
# @author Kit Kennedy

from datetime import timedelta

from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gs_components import GroundNetworkPS,GroundNetworkStateRecorder

SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    def __init__(self):
        pass

    def get_plan_db(self):
        """ get planning info database used by this agent """

        #  intended to be a virtual method
        raise NotImplementedError

    def send_planning_info(self,other):
        """ send planning info from one agent to another """

        # todo: is it bad that this cuts through layers of abstraction to grab the planning db?

        #  get the planning info databases
        my_plan_db = self.get_plan_db()
        other_plan_db = other.get_plan_db()

        #  push data from self to the other
        my_plan_db.push_planning_info(other_plan_db)

        other.post_planning_info_rx()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information"""
        pass


class SimSatellite(SimAgent):
    """class for simulation satellites"""
    
    def __init__(self,sat_id,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        # current time for the agent. note that individual components within the agent store their own times
        self._curr_time_dt = sim_start_dt

        #  the satellite ID ( not necessarily the same as the satellite index, and must be a string)
        self.sat_id = sat_id
        #  the satellite index. this is used for indexing in internal data structures
        self.sat_indx = sat_indx

        #  internal satellite simulation objects
        self.state_sim = SatStateSimulator(self,
            sim_start_dt,
            sim_satellite_params['state_simulator'],
            sat_scenario_params['power_params'],
            sat_scenario_params['data_storage_params'],
            sat_scenario_params['initial_state'],
        )
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt)
        self.exec = SatExecutive(self,sim_start_dt)
        self.state_recorder = SatStateRecorder(sim_start_dt,sim_satellite_params['state_recorder'])

        # adds references between sat sim objects
        self.state_sim.sat_exec = self.exec
        self.state_sim.state_recorder = self.state_recorder
        self.exec.sat_state_sim = self.state_sim
        self.exec.sat_arbiter = self.arbiter
        self.exec.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_satellite_params['time_epsilon_s'])

        super().__init__()

    def state_update_step(self,new_time_dt):
        """ update the state of the satellite using the new time"""

        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.
        self.state_sim.update(new_time_dt)
        self.arbiter.update(new_time_dt)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.arbiter.get_plan_db()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        self.arbiter.flag_planning_info_update()

    def get_ecl_winds(self):
        return self.get_plan_db().get_ecl_winds(self.sat_id)

    def get_act_hist(self):
        return self.state_recorder.act_hist
    def get_ES_hist(self):
        return self.state_recorder.ES_state_hist
    def get_DS_hist(self):
        return self.state_recorder.DS_state_hist

class SimGroundStation(SimAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,name,gs_network):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.ID = ID
        self.name = name
        self.gs_network = gs_network

        super().__init__()


class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,num_sats,gp_wrapper,sim_gs_network_params):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.ID = ID
        self.name = name
        self.gs_list = []

        # current time for the agent. note that individual components within the agent store their own times
        self._curr_time_dt = sim_start_dt

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,gp_wrapper,sim_gs_network_params['gsn_ps_params'])
        self.state_recorder = GroundNetworkStateRecorder(sim_start_dt,num_sats)

        self.scheduler.state_recorder = self.state_recorder

        super().__init__()

    def state_update_step(self,new_time_dt):
        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        self.scheduler.update(new_time_dt)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.scheduler.get_plan_db()

    def get_all_sats_act_hists(self):
        return self.state_recorder.get_all_sats_act_hists()
