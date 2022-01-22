#  Contains objects for different software components running on  ground stations and the ground station network
#
# @author Kit Kennedy

from datetime import timedelta

from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import StateSimulator,Executive,ExecutiveAgentPlannerScheduler,DataStore
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools
import logging
class GSStateSimulator(StateSimulator):
    """Simulates gs system state. Mostly """

    def __init__(self,sim_gs,sim_start_dt,dv_epsilon=0.01):
        # todo:  should probably add more robust physical units checking
        super().__init__(sim_gs)

        self.sim_gs = sim_gs

        ################
        #  Data storage stuff

        #  note that data generated is not really simulated here; the state simulator just provides a coherent API for
        #  storage ( the executive handles data generation)

        #  data storage is assumed to be in Mb
        #  assume we start out with zero data stored on board
        self.DS_state = 0

        # current time for this component. we store the sim start time as the current time, but note that we still
        # need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        # holds ref to StateRecorder
        self.state_recorder = None
        # holds ref to SatDataStore
        # note that we include the data_store in the simulator because we may want, in future, to simulate data
        # corruptions or various other time dependent phenomena in the data store. So, we wrap it within this sim layer.
        # Also it's convenient to be able to store DS_state here alongside ES_state
        self.data_store = DataStore()

        # the "effectively zero" data volume number
        self.dv_epsilon = dv_epsilon # Mb

        #  whether or not we're on the first step of the simulation
        self._first_step = True 


    def update(self,new_time_dt):
        """
        Update state to new time by propagating state forward from last time to new time. Note that we use state at
        self._curr_time_dt to propagate forward to new_time_dt
        """

        # If first step, just record state then return
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self.state_recorder.add_DS_hist(self._curr_time_dt,self.DS_state)
            self._first_step = False
            return

        self._curr_time_dt = new_time_dt

        ##############################
        # update state recorder
    
        self.state_recorder.add_DS_hist(self._curr_time_dt,self.DS_state)

    def nominal_state_check(self):
        # assume state is always nominal for GS
        return True

    def get_available_data_storage(self,time_dt,dv_desired):
        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon)

        #  for the ground station we don't care about a data storage limit
        return dv_desired + self.dv_epsilon


    def update_data_storage(self,delta_dv,data_conts,time_dt):
        """ add an amount of data volume to data storage state"""

        if not time_dt == self._curr_time_dt:
            raise RuntimeWarning('Attempting to add data volume to state sim off-timestep')

        if delta_dv < 0 :
            raise RuntimeWarning('Attempted to take data volume away from a ground station')

        # add any data containers that the data store doesn't know about yet
        self.data_store.add(data_conts)

        self.DS_state += delta_dv

        # sanity check to make sure we are recording the same amount of dv as is actually in the data store
        assert(abs(self.DS_state - self.data_store.get_total_dv()) < self.dv_epsilon)

    def cleanup_data_conts(self,data_conts):
        self.data_store.remove_empty_dcs(data_conts)        

class GSSchedulePassThru(ExecutiveAgentPlannerScheduler):
    """
    Handles ingestion of new schedule artifacts from ground planner and distilling out the relevant details for the
    ground station. Does not make scheduling decisions
    """

    def __init__(self,sim_gs,sim_start_dt,sim_end_dt,act_timing_helper):
        super().__init__(sim_gs,sim_start_dt,sim_end_dt,act_timing_helper)

        # holds ref to the containing sim gs
        self.sim_gs = sim_gs

    def _check_internal_planning_update_req(self):
        #  don't need to do an internal planning update, because currently ground stations do not do any of their
        #  own planning ( they get all of their plans from the ground station network)
        # returns two things : (replan_required_bool, replan_type_str)
        return (False, 'nominal')

    def _internal_planning_update(self,replan_required,replan_type,planner_wrapper,new_time_dt):
        #  we don't do any internal planning updates for the ground station planner. ( including this for clarity of
        #  intent, not because things wouldn't run if it weren't present)

        #  explicitly raise an error if a planner wrapper was provided ( none should exist for the ground stations)
        if planner_wrapper:
            raise NotImplementedError

    def get_executable_acts(self):
        #  get relevant sim route containers for deriving a schedule
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,
                                                        filter_opt='partially_within',gs_id=self.sim_gs.gs_id)

        #  Synthesizes the list of unique activities to execute, with the correct execution times
        #  and data volumes on them.
        #  The list elements are of type ExecutableActivity.
        #  Filter rationale:  we may be in the middle of executing an activity, and we want to preserve the fact that
        #  that activity is in the schedule. so if a window is partially for current time, but ends after current time,
        #  we still want to consider it an executable act.
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,
                                                     filter_opt='partially_within',gs_indx=self.sim_gs.gs_indx,
                                                     act_timing_helper=self.act_timing_helper)

        return executable_acts
    
class GSExecutive(Executive):

    def __init__(self,sim_gs,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim ground station
        self.sim_gs = sim_gs

        super().__init__(sim_gs,sim_start_dt,dv_epsilon)

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ sets up context dictionary for activity execution on the satellite"""

        curr_exec_context = super()._initialize_act_execution_context(exec_act,new_time_dt)


        if curr_exec_context:
            #  for receiving, we should keep track of which data container the transmitting satellite last sent
            #  note: ground stations are currently assumed to be receivers only
            curr_exec_context['curr_txsat_data_cont'] = None

        #  returning this not because it's expected to be used, but to be consistent with superclass
        return curr_exec_context

    def _cleanup_act_execution_context(self,exec_act,new_time_dt):
        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        # if successfully received data, want to update the gs network with new planning info
        if curr_exec_context['rx_success']:
            print("Sending plan info to GSN as exec act {} has rx success".format(exec_act.wind.window_ID))
            # todo: note assumption that we only are looking at routes for now - should be changed to "all" in future
            self.sim_gs.send_planning_info(self.sim_gs.gs_network.ID,info_option='routes_only')

        
        super()._cleanup_act_execution_context(exec_act,new_time_dt)

    def _execute_act(self,exec_act,new_time_dt):
        """
        Execute the executable activity input, taking any actions that
        this ground stations is responsible for initiating
        """

        #  note that currently the ground station is not responsible for initiating any activities

        pass
