#  Contains objects  that are used in simulation loop to represent high-level agents within simulation, including satellites, ground stations, and the ground station network
#
# @author Kit Kennedy

from datetime import timedelta

from .sim_agent_components import DataStore, ExecutiveAgentStateRecorder
from .sim_sat_components import SatScheduleArbiter,SatExecutive,SatStateSimulator,SatStateRecorder
from .sim_gsnetwork_components import GroundNetworkPS,GroundNetworkStateRecorder
from .sim_gs_components import GSStateSimulator,GSSchedulePassThru,GSExecutive

SAT_STATE_JSON_VER = '0.1'

class SimAgent:
    """Super class for simulation agents within constellation"""

    def __init__(self,ID,sim_start_dt,sim_end_dt):
        self.ID = ID

        # current time for the agent. note that individual components within the agent store their own times
        self._curr_time_dt = sim_start_dt

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
        my_plan_db.push_planning_info(other_plan_db,self._curr_time_dt)

        other.post_planning_info_rx()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information"""
        # intended to be implemented in subclass
        raise NotImplementedError

class SimExecutiveAgent(SimAgent):
    """ superclass for agents that are capable of executing plans in the simulation (e.g. satellites, ground stations)"""

    def __init__(self,ID,sim_start_dt,sim_end_dt):
        super().__init__(ID,sim_start_dt,sim_end_dt)

    def execution_step(self,new_time_dt):
        self.exec.execute_acts(new_time_dt)

    def get_exec(self):
        return self.exec

    def get_act_hist(self):
        return self.state_recorder.get_act_hist()

    def get_DS_hist(self):
        return self.state_recorder.get_DS_hist()


class SimSatellite(SimExecutiveAgent):
    """class for simulation satellites"""
    
    def __init__(self,ID,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        #  the satellite index. this is used for indexing in internal data structures
        self.sat_indx = sat_indx

        #  list of all simulation satellites (needed for inter-satellite transactions).  to be set after initialization.
        self.all_sim_sats = None
        
        #  list of all simulation ground stations (needed for  down links).  to be set after initialization.
        self.all_sim_gs = None

        #  internal satellite simulation objects
        self.state_sim = SatStateSimulator(self,
            sim_start_dt,
            sim_satellite_params['state_simulator'],
            sat_scenario_params['power_params'],
            sat_scenario_params['data_storage_params'],
            sat_scenario_params['initial_state'],
        )
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt,sim_satellite_params['sat_schedule_arbiter_params'])
        self.exec = SatExecutive(self,sim_start_dt)
        self.state_recorder = SatStateRecorder(sim_start_dt)

        # adds references between sat sim objects
        self.arbiter.state_sim = self.state_sim
        self.state_sim.sat_exec = self.exec
        self.state_sim.state_recorder = self.state_recorder
        self.exec.state_sim = self.state_sim
        self.exec.scheduler = self.arbiter
        self.exec.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_satellite_params['time_epsilon_s'])

        super().__init__(ID,sim_start_dt,sim_end_dt)

    @property
    def sat_id(self):
        return self.ID

    @property
    def lp_agent_id(self):
        """Get the name to use when creating new routes in the LP"""
        return 'sat%s'%(self.ID)

    @property
    def dc_agent_id(self):
        """Get the name to use when creating new data containers in the executive"""
        return 'sat%s_dc'%(self.ID)

    def state_update_step(self,new_time_dt,lp_wrapper):
        """ update the state of the satellite using the new time"""

        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.
        self.state_sim.update(new_time_dt)
        self.arbiter.update(new_time_dt,planner_wrapper=lp_wrapper)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt


    def get_sat_from_indx(self,sat_indx):
        return self.all_sim_sats[sat_indx]

    def get_gs_from_indx(self,gs_indx):
        return self.all_sim_gs[gs_indx]

    def get_plan_db(self):
        return self.arbiter.get_plan_db()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        self.arbiter.flag_planning_info_rx_external()

    def get_ecl_winds(self):
        return self.get_plan_db().get_ecl_winds(self.sat_id)

    def get_ES_hist(self):
        return self.state_recorder.get_ES_hist()

    def inject_obs(self,obs_list):
        self.exec.inject_obs(obs_list)

class SimGroundStation(SimExecutiveAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,gs_indx,name,gs_network,sim_start_dt,sim_end_dt):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.gs_indx = gs_indx
        self.name = name
        self.gs_network = gs_network

        #  internal ground station simulation objects
        self.state_sim = GSStateSimulator(self,sim_start_dt)
        self.scheduler_pass_thru = GSSchedulePassThru(self,sim_start_dt,sim_end_dt)
        self.exec = GSExecutive(self,sim_start_dt)
        self.state_recorder = ExecutiveAgentStateRecorder(sim_start_dt)

        self.state_sim.state_recorder = self.state_recorder
        self.exec.state_sim = self.state_sim
        self.exec.scheduler = self.scheduler_pass_thru
        self.exec.state_recorder = self.state_recorder

        super().__init__(ID,sim_start_dt,sim_end_dt)

    @property
    def gs_id(self):
        return self.ID

    @property
    def dc_agent_id(self):
        """Get the name to use when creating new data containers in the executive"""
        return 'gs%s_dc'%(self.ID)

    def state_update_step(self,new_time_dt):
        """ update the state of the satellite using the new time"""

        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        # note that the order of these update steps is not arbitrary. See their definition file for more information.
        self.state_sim.update(new_time_dt)
        self.scheduler_pass_thru.update(new_time_dt,planner_wrapper=None)
        self.exec.update(new_time_dt)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.scheduler_pass_thru.get_plan_db()

    def post_planning_info_rx(self):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        self.scheduler_pass_thru.flag_planning_info_rx_external()

class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,num_sats,num_gs,sim_gs_network_params):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        self.name = name
        self.gs_list = []

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,sim_gs_network_params['gsn_ps_params'])
        self.state_recorder = GroundNetworkStateRecorder(sim_start_dt,num_sats,num_gs)

        self.scheduler.state_recorder = self.state_recorder

        super().__init__(ID,sim_start_dt,sim_end_dt)

    def state_update_step(self,new_time_dt,gp_wrapper):
        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        self.scheduler.update(new_time_dt,planner_wrapper=gp_wrapper)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.scheduler.get_plan_db()

    def get_all_sats_planned_act_hists(self):
        return self.state_recorder.get_all_sats_planned_act_hists()

    def get_all_gs_planned_act_hists(self):
        return self.state_recorder.get_all_gs_planned_act_hists()
