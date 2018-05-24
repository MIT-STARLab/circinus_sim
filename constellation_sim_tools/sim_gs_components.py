#  Contains objects for different software components running on  ground stations and the ground station network
#
# @author Kit Kennedy

from collections import namedtuple
from datetime import timedelta

from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow
from .sim_agent_components import Executive,PlannerScheduler,StateRecorder
from .schedule_tools  import synthesize_executable_acts

from circinus_tools import debug_tools

class GroundNetworkPS(PlannerScheduler):
    """Handles calling of the GP, and ingestion of plan and state updates from satellites"""

    ReplanQEntry = namedtuple('ReplanQEntry', 'time_dt rt_conts')

    def __init__(self,sim_gsn,sim_start_dt,sim_end_dt,gp_wrapper,gsn_ps_params):
        # holds ref to the containing sim ground network
        self.sim_gsn = sim_gsn

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        self.gp_wrapper = gp_wrapper

        #  time to wait since last plans were released before rerunning planning
        self.replan_interval_s = gsn_ps_params['replan_interval_s']

        #  this is the FIFO waiting list for newly produced plans.  when current time reaches the time for the first entry, that entry will be popped and the planning info database will be updated with those plans (each entry is a ReplanQEntry named tuple). this too should always stay sorted in ascending order of entry add time
        self._replan_release_q = []
        #  the time that the scheduler waits after starting replanting before it makes the new plans available for sharing with other agents. this mechanism simulates the time required for planning in real life. set this to zero to release plans immediately ( meaning we assume planning is instantaneous)
        self.replan_release_wait_time_s = gsn_ps_params['replan_release_wait_time_s']
        #  if true first plans created at beginning of simulation will be released immediately
        self.release_first_plans_immediately = gsn_ps_params['release_first_plans_immediately']

        #  the last time a plan was performed
        self._last_replan_time_dt = None

        #  whether or not plans have been updated
        #  todo: not sure if this is too hacky of a way to do this, reassess once incorporated code for planning info sharing over links
        self.plans_updated = False

        # This keeps track of the latest data route index created. (the DR "uid" in the GP algorithm)
        self.latest_gp_route_indx = 0

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

        # holds ref to GroundNetworkStateRecorder
        self.state_recorder = None

        super().__init__(sim_start_dt,sim_end_dt)

    def update(self,new_time_dt):
        """  this code calls the global planner. it deals with the fact that in reality it takes time to run the global planner. for this reason we add the output of the global planner to a queue, and only release those new plans once enough simulation time is past for those plans to be 'available'.  we do this primarily because satellites can talk to the ground station at any time, so we don't want to do an instantaneous plan update right after the ground station talks to one satellite and then share those plans with another satellite immediately. We have to wait a replan time. """
        
        # If first step, check time validity
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')

        #  see if we need to replan at this time step
        # add in consideration for lots of received updated planning information from satellites causing a need to rerun global planner?
        replan_required = False
        if self._last_replan_time_dt is None:
            replan_required = True
        elif (self._curr_time_dt - self._last_replan_time_dt).total_seconds() >= self.replan_interval_s:
            replan_required = True
        #  if we already have plans waiting to be released
        if len(self._replan_release_q) > 0:
            replan_required = False

        def set_rt_cont_times(rt_conts,update_dt):
            """ set the update time on all of the route containers"""
            for rt_cont in rt_conts:
                rt_cont.set_times_safe(update_dt)

        #  perform re-plan if required, and release or add to queue as appropriate
        if replan_required:

            if len(self._replan_release_q) > 0:
                raise RuntimeWarning('Trying to rerun GP while there already plan results waiting for release.')

            new_rt_conts = self.run_planner()

            #  if we don't have to wait to release new plans
            if self.replan_release_wait_time_s == 0:
                #  mark all of the route containers with their release time
                set_rt_cont_times(new_rt_conts,self._curr_time_dt)
                # update plan database
                self.plan_db.update_routes(new_rt_conts)
                #  update replan time
                self._last_replan_time_dt = self._curr_time_dt
                self.plans_updated = True

            #  if this is the first plan cycle (beginning of simulation), there's an option to release immediately
            elif self._first_step and self.release_first_plans_immediately:
                #  mark all of the route containers with their release time
                set_rt_cont_times(new_rt_conts,self._curr_time_dt)
                self.plan_db.update_routes(new_rt_conts)
                self._last_replan_time_dt = self._curr_time_dt
                self.plans_updated = True

            #  if it's not the first time and there is a wait required
            else:
                # append a new set of plans, with their release/availaibility time as after replan_release_wait_time_s
                self._replan_release_q.append(self.ReplanQEntry(time_dt=self._curr_time_dt+timedelta(seconds=self.replan_release_wait_time_s),rt_conts=new_rt_conts))

        #  release any ripe plans from the queue
        while len(self._replan_release_q)>0 and self._replan_release_q[0].time_dt <= self._curr_time_dt:
            q_entry = self._replan_release_q.pop(0) # this pop prevents while loop from executing forever
            #  mark all of the route containers with their release time
            set_rt_cont_times(q_entry.rt_conts,self._curr_time_dt)
            self.plan_db.update_routes(q_entry.rt_conts)
            self._last_replan_time_dt = q_entry.time_dt
            self.plans_updated = True

        #  save off the executable activities seen by the global planner so they can be looked at at the end of the sim
        rt_conts = self.plan_db.get_filtered_sim_routes(filter_start_dt=self._curr_time_dt,filter_opt='partially_within')
        # debug_tools.debug_breakpt()
        executable_acts = synthesize_executable_acts(rt_conts,filter_start_dt=self._curr_time_dt)
        for exec_act in executable_acts:
            self.state_recorder.add_act_hist(exec_act.act)

        #  time update
        self._curr_time_dt = new_time_dt

        self._first_step = False

    def run_planner(self):

        #  get already existing sim route containers that need to be fed to the global planner
        existing_sim_rt_conts = self.plan_db.get_filtered_sim_routes(self._curr_time_dt,filter_opt='partially_within')

        #  get the satellite states at the beginning of the planning window
        sat_state_by_id = self.plan_db.get_sat_states(self._curr_time_dt)

        #  run the global planner
        # debug_tools.debug_breakpt()
        new_rt_conts, latest_gp_route_uid = self.gp_wrapper.run_gp(self._curr_time_dt,existing_sim_rt_conts,self.sim_gsn.ID,self.latest_gp_route_indx,sat_state_by_id)

        #  I figure this can be done immediately and it's okay -  immediately updating the latest route index shouldn't be bad. todo:  confirm this is okay
        self.latest_gp_route_indx = latest_gp_route_uid

        return new_rt_conts

class GSExecutive(Executive):

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ sets up context dictionary for activity execution on the satellite"""

        curr_exec_context = super._initialize_act_execution_context(exec_act,new_time_dt)

        #  for receiving, we should keep track of which data container the transmitting satellite last sent
        #  note: ground stations are currently assumed to be receivers only
        curr_exec_context['curr_txsat_data_cont'] = None

        #  returning this not because it's expected to be used, but to be consistent with superclass
        return curr_exec_context

    def execute_acts(self,new_time_dt):
        """ execute current activities that the update step has chosen """

        #  note: currently the ground station is not responsible for initiating any activities
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

class GroundNetworkStateRecorder(StateRecorder):
    """Convenient interface for storing state history for ground network"""
    # todo: is "state" the right word here?

    def __init__(self,sim_start_dt,num_sats):
        # store all these activities in a dictionary indexed by the ID, so that we can update activities later
        self.act_hist_by_wind_id = {}
        self.num_sats = num_sats

        super().__init__()    

    def add_act_hist(self,act):
        # Todo: include correct execution times

        #  note: implicitly update any entry that might've been in this dictionary before for this activity
        self.act_hist_by_wind_id[act.window_ID] = act

    def get_all_sats_act_hists(self):
        # all_sats_act_hists = []
        sats_obs = [[] for indx in range(self.num_sats)]
        sats_dlnks = [[] for indx in range(self.num_sats)]
        sats_xlnks = [[] for indx in range(self.num_sats)]

        for act in self.act_hist_by_wind_id.values():
            if type(act) == ObsWindow:
                sats_obs[act.sat_indx].append(act)
            elif type(act) == DlnkWindow:
                sats_dlnks[act.sat_indx].append(act)
            elif type(act) == XlnkWindow:
                sats_xlnks[act.sat_indx].append(act)
                sats_xlnks[act.xsat_indx].append(act)

        for sat_indx in range(self.num_sats):
            sats_obs[sat_indx].sort(key= lambda wind: wind.start)
            sats_dlnks[sat_indx].sort(key= lambda wind: wind.start)
            sats_xlnks[sat_indx].sort(key= lambda wind: wind.start)

        return sats_obs,sats_dlnks,sats_xlnks
