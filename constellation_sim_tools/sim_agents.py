from datetime import timedelta

from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gs_components import GroundNetworkPS

SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    def __init__(self):
        pass


class SimSatellite(SimAgent):
    """class for simulation satellites"""
    
    def __init__(self,sat_id,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params,sat_event_data):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.curr_time_dt = sim_start_dt

        self.sat_id = sat_id
        self.sat_indx = sat_indx
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt)
        self.exec = SatExecutive(self,sim_start_dt)
        self.state_sim = SatStateSimulator(self,sim_start_dt,sim_satellite_params['state_simulator'],sat_scenario_params['power_params'],sat_scenario_params['data_storage_params'],sat_scenario_params['initial_state'],sat_event_data)
        self.state_recorder = SatStateRecorder()

        # adds references between sat sim objects
        self.state_sim.sat_exec = self.exec
        self.exec.sat_state_sim = self.state_sim
        self.exec.sat_arbiter = self.arbiter

        self.time_epsilon_td = timedelta(seconds = sim_satellite_params['time_epsilon_s'])

        super().__init__()

    def state_update_step(self,new_time_dt):
        self.state_sim.update(new_time_dt)
        self.exec.update(new_time_dt)

class SimGroundStation(SimAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,name,gs_network):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__()

        self.ID = ID
        self.name = name
        self.gs_network = gs_network

class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,gp_wrapper,gs_list=[]):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.ID = ID
        self.name = name
        self.gs_list = gs_list

        self.curr_time_dt = sim_start_dt

        self.scheduler = GroundNetworkPS(self,sim_start_dt,gp_wrapper)

        super().__init__()

    def replan_step(self):
        # todo: add checking for right time to do replanning

        self.scheduler.do_plan_and_sched()
