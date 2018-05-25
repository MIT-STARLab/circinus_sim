#  Contains objects for different software components running on  ground stations and the ground station network
#
# @author Kit Kennedy

from collections import namedtuple
from datetime import timedelta

from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import StateSimulator,Executive,ExecutiveAgentPlannerScheduler,DataStore
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools

class GSStateSimulator(StateSimulator):
    """Simulates gs system state. Mostly """

    def __init__(self,sim_gs,sim_start_dt,dv_epsilon=0.01):
        # todo:  should probably add more robust physical units checking

        self.sim_gs = sim_gs

        ################
        #  Data storage stuff

        #  note that data generated is not really simulated here; the state simulator just provides a coherent API for storage ( the executive handles data generation)

        #  data storage is assumed to be in Mb
        #  assume we start out with zero data stored on board
        self.DS_state = 0

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        # holds ref to StateRecorder
        self.state_recorder = None
        # holds ref to SatDataStore
        # note that we include the data_store in the simulator because we may want, in future, to simulate data corruptions or various other time dependent phenomena in the data store. So, we wrap it within this sim layer. Also it's convenient to be able to store DS_state here alongside ES_state
        self.data_store = DataStore()

        # the "effectively zero" data volume number
        self.dv_epsilon = dv_epsilon # Mb

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

        super().__init__(sim_gs)

    def update(self,new_time_dt):
        """ Update state to new time by propagating state forward from last time to new time. Note that we use state at self._curr_time_dt to propagate forward to new_time_dt"""

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


    def add_to_data_storage(self,delta_dv,data_conts,time_dt):
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

    def get_curr_data_conts(self):
        return self.data_store.get_curr_data_conts()

    def cleanup_data_conts(self,data_conts):
        self.data_store.cleanup(data_conts)        

class GSSchedulePassThru(ExecutiveAgentPlannerScheduler):
    """Handles ingestion of new schedule artifacts from ground planner and distilling out the relevant details for the ground station. Does not make scheduling decisions"""

    def __init__(self,sim_gs,sim_start_dt,sim_end_dt):
        # holds ref to the containing sim sat
        self.sim_gs = sim_gs

        super().__init__(sim_gs,sim_start_dt,sim_end_dt)

    def update(self,new_time_dt):
        # If first step, check the time
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self._first_step = False

        # rest of the code is fine to execute in first step, because we might have planning info from which to derive a schedule

        #  if planning info has not been updated in the schedule has already been updated, then there is no reason to update schedule
        if not self._planning_info_updated and self._schedule_updated:
            return

        #  get relevant sim route containers for deriving a schedule
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within',gs_id=self.sim_gs.gs_id)

        #  synthesizes the list of unique activities to execute, with the correct execution times and data volumes on them
        #  the list elements are of type circinus_tools.scheduling.routing_objects.ExecutableActivity
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt,gs_indx=self.sim_gs.gs_indx)

        # sort executable activities by start time
        executable_acts.sort(key = lambda ex_act: ex_act.act.executable_start)

        self._schedule_updated = True
        self._planning_info_updated = False
        self._schedule_cache = executable_acts

        self._curr_time_dt = new_time_dt
    
class GSExecutive(Executive):

    def __init__(self,sim_gs,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim ground station
        self.sim_gs = sim_gs

        super().__init__(sim_gs,sim_start_dt,dv_epsilon)

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ sets up context dictionary for activity execution on the satellite"""

        curr_exec_context = super()._initialize_act_execution_context(exec_act,new_time_dt)

        #  for receiving, we should keep track of which data container the transmitting satellite last sent
        #  note: ground stations are currently assumed to be receivers only
        curr_exec_context['curr_txsat_data_cont'] = None

        #  returning this not because it's expected to be used, but to be consistent with superclass
        return curr_exec_context

    def _execute_act(self,exec_act,new_time_dt):
        """ Execute the executable activity input, taking any actions that this ground stations is responsible for initiating """

        #  note that currently the ground station is not responsible for initiating any activities

        pass


    def dlnk_receive_poll(self,tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,tx_sat_indx,txsat_data_cont,proposed_dv):
        """Called by a transmitting satellite to see if this ground station can receive data and if yes, handles the received data
        
        [description]
        :param ts_start_dt: the start time for this transmission, as calculated by the transmitting satellite
        :type ts_start_dt: datetime
        :param ts_end_dt: the end time for this transmission, as calculated by the transmitting satellite
        :type ts_end_dt: datetime
        :param proposed_act:  the activity which the transmitting satellite is executing
        :type proposed_act: XlnkWindow
        :param xsat_indx:  the index of the transmitting satellite
        :type xsat_indx: int
        :param txsat_data_cont:  the data container that the transmitting satellite is sending ( with route only up to the transmitting satellite)
        :type txsat_data_cont: SimDataContainer
        :param proposed_dv:  the data volume that the transmitting satellite is trying to send
        :type proposed_dv: float
        :returns:  data volume received, success flag
        :rtype: {int,bool}
        :raises: RuntimeWarning, RuntimeWarning, RuntimeWarning
        """

        # note: do not modify txsat_data_cont in here!
        #  note that a key assumption about this function is that once it indicates a failure to receive data, subsequent calls at the current time step will not be successful ( and therefore the transmitter does not have to continue trying)

        #  verify of the right type
        if not type(proposed_act) == DlnkWindow:
            raise RuntimeWarning('saw a non-dlnk window')

        return self.receive_poll(tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,tx_sat_indx,txsat_data_cont,proposed_dv)
