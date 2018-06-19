#  Contains objects  that are used in simulation loop to represent high-level agents within simulation, including satellites, ground stations, and the ground station network
#
# @author Kit Kennedy

from datetime import timedelta

import circinus_tools.metrics.metrics_utils as met_util
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
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt

    def get_plan_db(self):
        """ get planning info database used by this agent """

        #  intended to be a virtual method
        raise NotImplementedError

    def send_planning_info(self,other,info_option='all'):
        """ send planning info from one agent to another """

        if not info_option in ['all','ttc_only','routes_only']:
            raise RuntimeWarning('unknown info sharing option: %s'%(info_option))

        #  get the planning info databases
        my_plan_db = self.get_plan_db()
        other_plan_db = other.get_plan_db()

        #  push data from self to the other
        my_plan_db.push_planning_info(other_plan_db,self._curr_time_dt,info_option)


        other.post_planning_info_rx(info_option)

    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information"""
        # intended to be implemented in subclass
        raise NotImplementedError

class SimExecutiveAgent(SimAgent):
    """ superclass for agents that are capable of executing plans in the simulation (e.g. satellites, ground stations)"""

    def __init__(self,ID,sim_start_dt,sim_end_dt):
        super().__init__(ID,sim_start_dt,sim_end_dt)

        # to be created in subclass
        self.state_sim = None

    def execution_step(self,new_time_dt):
        self.exec.execute_acts(new_time_dt)

    def get_exec(self):
        return self.exec

    def get_act_hist(self):
        return self.state_recorder.get_act_hist()

    def get_DS_hist(self):
        return self.state_recorder.get_DS_hist()

    def get_curr_data_conts(self):

        return self.state_sim.get_curr_data_conts()


class SimSatellite(SimExecutiveAgent):
    """class for simulation satellites"""
    
    def __init__(self,ID,sat_indx,sim_start_dt,sim_end_dt,sat_scenario_params,sim_satellite_params,act_timing_helper):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """

        super().__init__(ID,sim_start_dt,sim_end_dt)

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
        self.arbiter = SatScheduleArbiter(self,sim_start_dt,sim_end_dt,sim_satellite_params['sat_schedule_arbiter_params'],act_timing_helper)
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

    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information (satellite-specific)"""

        # todo: this terminology has gotten a bit overloaded, an update to it could be helpful
        # only flag a planning info update (and thus cause replanning) if we received updated routes
        if info_option in ['all','routes_only']:
            self.arbiter.flag_planning_info_rx_external()

    def get_ecl_winds(self):
        return self.get_plan_db().get_ecl_winds(self.sat_id)

    def get_ES_hist(self):
        return self.state_recorder.get_ES_hist()

    def inject_obs(self,obs_list):
        self.exec.inject_obs(obs_list)

    def get_merged_cmd_update_hist(self,gs_agents,gs_id_ignore_list):
        """ gets the merged command update history for this satellite
        
        Gets the update history for every ground station as seen by this satellite, and then merges these into a single update history. The update history for ground station gs_indx for this satellite  is a recording of when this satellite last heard from that ground station. By merging across all ground stations, we get a recording of when the satellite last heard from any ground station, which we assume is a good proxy for when ground commanding was last updated. ( note the underlying assumption that all ground stations have equal relevance for commanding the satellite)
        :param all_gs_IDs: [description]
        :type all_gs_IDs: [type]
        :param gs_id_ignore_list: [description]
        :type gs_id_ignore_list: [type]
        :returns: [description]
        :rtype: {[type]}
        """

        agent_ids = [gs_agent.ID for gs_agent in gs_agents if not gs_agent.ID in gs_id_ignore_list]

        update_hists = self.get_plan_db().get_ttc_update_hist_for_agent_ids(agent_ids)

        return met_util.merge_update_histories( update_hists,self.sim_end_dt)

class SimGroundStation(SimExecutiveAgent):
    """class for simulation ground stations"""
    
    def __init__(self,ID,gs_indx,name,gs_network,sim_start_dt,sim_end_dt,sim_gs_params,act_timing_helper):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,sim_start_dt,sim_end_dt)

        self.gs_indx = gs_indx
        self.name = name
        self.gs_network = gs_network

        #  internal ground station simulation objects
        self.state_sim = GSStateSimulator(self,sim_start_dt)
        self.scheduler_pass_thru = GSSchedulePassThru(self,sim_start_dt,sim_end_dt,act_timing_helper)
        self.exec = GSExecutive(self,sim_start_dt)
        self.state_recorder = ExecutiveAgentStateRecorder(sim_start_dt)

        self.state_sim.state_recorder = self.state_recorder
        self.exec.state_sim = self.state_sim
        self.exec.scheduler = self.scheduler_pass_thru
        self.exec.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_gs_params['time_epsilon_s'])

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

    def post_planning_info_rx(self,info_option):
        """ perform any actions required after receiving new planning information (satellite-specific)"""
        if info_option in ['all','routes_only']:
            self.scheduler_pass_thru.flag_planning_info_rx_external()

    def get_sat_tlm_update_hist(self,sat_agent):
        """gets the telemetry update history for a single satellite
        
        Gets the update history for a single satellite as seen by this ground station. The update history for satellite sat_indx for this ground station is a recording of when this ground station last heard from that satellite. By merging the update history returned here across satellites, you can get the merged telemetry update history for a single satellite across the full ground station network
        :param sat_indx: [description]
        :type sat_indx: [type]
        :returns: [description]
        :rtype: {[type]}
        """

        return self.get_plan_db().get_ttc_update_hist_for_agent_ids([sat_agent.ID])[0]

class SimGroundNetwork(SimAgent):
    """class for simulation ground network"""
    
    def __init__(self,ID,name,sim_start_dt,sim_end_dt,num_sats,num_gs,sim_gs_network_params,act_timing_helper):
        """initializes based on parameters
        
        initializes based on parameters
        :type params: dict
        """
        super().__init__(ID,sim_start_dt,sim_end_dt)
        

        self.name = name
        self.gs_list = []

        self.scheduler = GroundNetworkPS(self,sim_start_dt,sim_end_dt,sim_gs_network_params['gsn_ps_params'],act_timing_helper)
        self.state_recorder = GroundNetworkStateRecorder(sim_start_dt,num_sats,num_gs)

        self.scheduler.state_recorder = self.state_recorder

        self.time_epsilon_td = timedelta(seconds = sim_gs_network_params['time_epsilon_s'])


    def state_update_step(self,new_time_dt,gp_wrapper):
        if new_time_dt < self._curr_time_dt:
            raise RuntimeWarning('Saw earlier time')

        self.scheduler.update(new_time_dt,planner_wrapper=gp_wrapper)

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.scheduler.get_plan_db()

    def get_all_planned_rt_conts(self):
        return self.scheduler.get_plan_db().get_all_rt_conts()

    def get_all_sats_planned_act_hists(self):
        return self.state_recorder.get_all_sats_planned_act_hists()

    def get_all_gs_planned_act_hists(self):
        return self.state_recorder.get_all_gs_planned_act_hists()

    def post_planning_info_rx(self,info_option):
        pass