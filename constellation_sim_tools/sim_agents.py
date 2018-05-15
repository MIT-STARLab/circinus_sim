from datetime import timedelta

from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gs_components import GroundNetworkPS

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
    
    def __init__(self,sat_id,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params,sats_event_data):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.curr_time_dt = sim_start_dt

        self.sat_id = sat_id
        self.sat_indx = sat_indx
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt,sats_event_data)
        self.exec = SatExecutive(self,sim_start_dt)
        self.state_sim = SatStateSimulator(self,
            sim_start_dt,
            sim_satellite_params['state_simulator'],
            sat_scenario_params['power_params'],
            sat_scenario_params['data_storage_params'],
            sat_scenario_params['initial_state'],
            sats_event_data
        )
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
        self.state_sim.update(new_time_dt)
        self.exec.update(new_time_dt)

    def get_plan_db(self):
        return self.arbiter.get_plan_db()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        self.arbiter.flag_planning_info_update()


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
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,gp_wrapper,sim_gs_network_params):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.ID = ID
        self.name = name
        self.gs_list = []

        self.curr_time_dt = sim_start_dt

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,gp_wrapper,sim_gs_network_params['gsn_ps_params'],sats_event_data)

        super().__init__()

    def state_update_step(self,new_time_dt):
        self.scheduler.update(new_time_dt)

    def get_plan_db(self):
        return self.scheduler.get_plan_db()
