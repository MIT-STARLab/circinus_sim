#  Contains objects for different software components running on agents. these are intended to be components that are used across both satellites and ground stations
#
# @author Kit Kennedy

from collections import namedtuple
from datetime import timedelta
from copy import deepcopy

from circinus_tools import io_tools
from circinus_tools.sat_state_tools import propagate_sat_ES
from circinus_tools.scheduling.base_window  import find_windows_in_wind_list
from circinus_tools.other_tools import index_from_key
from circinus_tools.scheduling.custom_window import  DlnkWindow
from .schedule_tools  import synthesize_executable_acts,check_temporal_overlap

from circinus_tools import debug_tools

class StateSimulator:
    """Simulates agent state, holding internally any state variables needed for the process"""
    #  this is mostly a pass-through for the subclasses, because there's a fair bit of difference in what the ground station and satellites do

    def __init__(self,sim_executive_agent):
        # holds ref to the containing sim agent
        self.sim_executive_agent = sim_executive_agent

        # to be created in subclass
        self.data_store = None

    def update(self,new_time_dt):
        """ update state for state simulator"""

        #  intended to be implemented in the subclass
        raise NotImplementedError

    def get_curr_data_conts(self):
        return self.data_store.get_curr_data_conts()

ReplanQEntry = namedtuple('ReplanQEntry', 'time_dt rt_conts')

class PlannerScheduler:
    """ superclass for planning/scheduling elements running on ground and satellites"""

    def __init__(self,sim_start_dt,sim_end_dt):

        self.plan_db = PlanningInfoDB(sim_start_dt,sim_end_dt)

        #  this is the FIFO waiting list for newly produced plans.  when current time reaches the time for the first entry, that entry will be popped and the planning info database will be updated with those plans (each entry is a ReplanQEntry named tuple). this too should always stay sorted in ascending order of entry add time
        self._replan_release_q = []

        #  the time that the scheduler waits after starting replanting before it makes the new plans available for execution, and sharing with other agents. this mechanism simulates the time required for planning in real life. set this to zero to release plans immediately ( meaning we assume planning is instantaneous)
        self.replan_release_wait_time_s = None
        #  if true first plans created at beginning of simulation will be released immediately
        #  note that this is only relevant to set to true for the global planner only
        self.release_first_plans_immediately = False

        #  the last time a plan was performed
        self._last_replan_time_dt = None

        #  this records whether or not the information in the planning database has been updated. we use this planning information to derive a schedule.  we assume that we start with no planning information available, so this is false for now ( we can't yet derive a schedule from the planning info)
        #  note that this is not really used in the ground station network planner
        #  todo: not sure if this is too hacky of a way to do this, reassess once incorporated code for planning info sharing over links
        self._planning_info_updated = False
        self._planning_info_updated_internal_hist = []

        #  this keeps track of whether or not plans have been updated by taking in planning info from an external source, e.g. the ground network or other satellites
        self._planning_info_updated_external = False
        self._planning_info_updated_external_hist = []
        

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt

        #  whether or not we're on the first step of the simulation
        self._first_step = True 
        
    def check_plans_updated(self):
        return self._planning_info_updated

    def set_plans_updated(self,val):
        self._planning_info_updated = val

    def update(self,new_time_dt,planner_wrapper):
        """ update state for planner/scheduler"""

        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')
            self._first_step = False

        replan_required = self._check_internal_planning_update_req()

        self._internal_planning_update(replan_required,planner_wrapper)

        self._schedule_cache_update()

        self._curr_time_dt = new_time_dt

    def get_plan_db(self):
        return self.plan_db

    def _check_internal_planning_update_req(self):
        """ check if we need to do an internal planning update ( run the planner, if we have one, or release plans that have already been made )"""

        #  intended to be implemented in the subclass
        raise NotImplementedError

    def _schedule_cache_update(self):
        """ update the schedule cache ( for executive agents) """

        #  intended to be implemented in the subclass
        raise NotImplementedError

    def _internal_planning_update(self,replan_required,planner_wrapper):
        """ check and release any plans from the re-plan queue, and run planner to update plans if necessary"""

        # note:  this function can be overloaded in a subclass, if no planning actually needs take place (e.g.  ground station)

        # note: This code calls the planner for this agent. It deals with the fact that in reality it takes time to run the planner. For this reason we add the output of the planner to a queue, and only release those new plans once enough simulation time is past for those plans to be 'available'. We do this primarily because agents can talk to each other at any time, so we don't want to do an instantaneous plan update and share those plans immediately if in reality it would take time to compute them
        # note: that this uses the internal planner ( global planner or local planner) to update planning information. planning information can also be updated through reception of planning info from other agents. 

        #  perform re-plan if required, and release or add to queue as appropriate
        if replan_required:

            if len(self._replan_release_q) > 0:
                # note:  may need to add some better handling here... maybe sometimes we want to replan, but we should wait to we get the newest plans first?  why do we have a q if we can't put Multiple items on it?
                raise RuntimeWarning('Trying to rerun planner while there are already plan results waiting for release.')

            new_rt_conts = self._run_planner(planner_wrapper)

            #  if we don't have to wait to release new plans
            if self.replan_release_wait_time_s == 0:
                self._process_updated_routes(new_rt_conts,self._curr_time_dt)
                #  update replan time
                self._last_replan_time_dt = self._curr_time_dt
                self._planning_info_updated = True
                self._planning_info_updated_internal_hist.append(self._curr_time_dt)

            #  if this is the first plan cycle (beginning of simulation), there's an option to release immediately
            #  note: this is intended for the global planner
            elif self._first_step and self.release_first_plans_immediately:
                self._process_updated_routes(new_rt_conts,self._curr_time_dt)
                self._last_replan_time_dt = self._curr_time_dt
                self._planning_info_updated = True
                self._planning_info_updated_internal_hist.append(self._curr_time_dt)

            #  if it's not the first time and there is a wait required
            else:
                # append a new set of plans, with their release/availaibility time as after replan_release_wait_time_s
                self._replan_release_q.append(ReplanQEntry(time_dt=self._curr_time_dt+timedelta(seconds=self.replan_release_wait_time_s),rt_conts=new_rt_conts))

        #  release any ripe plans from the queue
        while len(self._replan_release_q)>0 and self._replan_release_q[0].time_dt <= self._curr_time_dt:
            q_entry = self._replan_release_q.pop(0) # this pop prevents while loop from executing forever
            self._process_updated_routes(q_entry.rt_conts,self._curr_time_dt)
            self._last_replan_time_dt = self._curr_time_dt
            self._planning_info_updated = True
            self._planning_info_updated_internal_hist.append(self._curr_time_dt)

    def _run_planner(self,planner_wrapper):
        """Run the planner contained within planner_wrapper"""

        #  intended to be implemented in the subclass
        raise NotImplementedError

    def _process_updated_routes(self,rt_conts):
        """ after receiving new route containers from a planner run, update them with any required information"""

        #  intended to be implemented in the subclass
        raise NotImplementedError


class ExecutiveAgentPlannerScheduler(PlannerScheduler):
    """Handles ingestion of new schedule artifacts from ground planner and distilling out the relevant details for the ground station. Does not make scheduling decisions"""

    def __init__(self,sim_executive_agent,sim_start_dt,sim_end_dt):
        super().__init__(sim_start_dt,sim_end_dt)

        # holds ref to the containing sim agent
        self.sim_executive_agent = sim_executive_agent

        #  whether or not schedule instance has been updated since it was last grabbed by executive
        self._schedule_cache_updated = False
        self._schedule_cache_updated_hist = []

        #  cached schedule, regenerated every time we receive new planning info. the elements in this list are of type ExecutableActivity
        self._schedule_cache = []


    @property
    def schedule_updated(self):
        return self._schedule_cache_updated

    def flag_planning_info_rx_external(self):
        # todo: Is this the right way to handle this?
        self._planning_info_updated = True
        self._planning_info_updated_external = True
        self._planning_info_updated_external_hist.append(self._curr_time_dt)

    # def ingest_routes(self,rt_conts):
    #     #  add to database
    #     self.plan_db.update_routes(rt_conts)

    def get_executable_acts(self):
        """  get filtered list of executable acts for this agent"""

        #  intended to be implemented in the subclass
        raise NotImplementedError

    def _schedule_cache_update(self):
        """ update the schedule cache, for use by the executive"""

        # rest of the code is fine to execute in first step, because we might have planning info from which to derive a schedule

        #  if planning info has not been updated then there is no reason to update schedule
        if not self._planning_info_updated:
            return

        executable_acts = self.get_executable_acts()

        # sort executable activities by start time
        executable_acts.sort(key = lambda ex_act: ex_act.act.executable_start)

        self._schedule_cache_updated = True
        self._schedule_cache_updated_hist.append(self._curr_time_dt)
        #  mark planning info as no longer being updated, because we have incorporated this updated information into the schedule
        self._planning_info_updated = False
        self._planning_info_updated_external = False
        self._schedule_cache = executable_acts

    def get_scheduled_executable_acts(self):
        self._schedule_cache_updated = False
        return self._schedule_cache
        
class Executive:
    """Superclass for executives running on ground and satellites"""

    def __init__(self,sim_executive_agent,sim_start_dt,dv_epsilon=0.01):
        # holds ref to the containing sim agent
        self.sim_executive_agent = sim_executive_agent

        #  scheduled activities list- a sorted list of the activities for satellite to perform ( obtained from schedule arbiter)
        #  these are of type ExecutableActivity
        self._scheduled_exec_acts = []
        self._scheduled_exec_acts_updated_hist = []
        #  this keeps track of executable activities that have been added ad hoc to the executive's scheduled activities. it's good to keep them separate from the actual schedule activities, because that makes it easier to update the schedule from the scheduler
        self._injected_exec_acts = []


        # these keep track of which activity we are currently executing. The windox index (windex)  is the location within the scheduled activities list
        self._curr_exec_acts = []
        self._last_scheduled_exec_act_windex = None
        self._last_injected_exec_act_windex = None

        #  maintains a record of of whether or not the current activity is canceled ( say, due to an off nominal condition)
        self._cancelled_acts = set()

        # current time for this component. we store the sim start time as the current time, but note that we still need to run the update step once at the sim start time
        self._curr_time_dt = sim_start_dt
        self._last_time_dt = sim_start_dt
        #  keeps track of up to what time activities have been executed ( used as a check for timing)
        self._last_act_execution_time_dt = sim_start_dt

        # holds ref to StateSimulator
        self.state_sim = None
        # holds ref to PlannerScheduler
        self.scheduler = None
        # holds ref to StateRecorder
        self.state_recorder = None

        # keeps track of which data container (data packet) we're on
        self._curr_dc_indx = 0

        #  holds data structures that keep track of execution context for a given activity.  these are all of the data structures that keep track of what has been accomplished in the course of executing a given activity
        self._execution_context_by_exec_act = {}

        #  whether or not we're on the first step of the simulation
        self._first_step = True 

        # the "effectively zero" data volume number
        self.dv_epsilon = dv_epsilon # Mb

    def _pull_schedule(self):

        scheduled_exec_acts_copy = self._scheduled_exec_acts

        # if schedule updates are available, blow away what we had previously (assumption is that scheduler handles changes to schedule gracefully)
        if self.scheduler.schedule_updated:
            self._scheduled_exec_acts = self.scheduler.get_scheduled_executable_acts()
        else:
            return

        # if we updated the schedule, need to deal with the consequences

        # if we're currently executing an act, figure out where that act is in the new sched. That's the "anchor point" in the new schedx. (note in general the current act SHOULD be the first one in the new sched. But something may have changed...)
        no_current_act = True
        if len(self._curr_exec_acts) > 0:
            no_current_act = False
            # store most recent (by executable_start) exec act seen. 
            latest_exec_act = scheduled_exec_acts_copy[self._last_scheduled_exec_act_windex]
            assert(latest_exec_act in self._curr_exec_acts)

            try:
                # search for current act in the scheduled acts
                # curr_act_indx = index_from_key(self._scheduled_exec_acts,key= ea: ea.act,value=latest_exec_act.act)
                # note that this searches by the activity window itself (the hash of the exec act)
                curr_act_indx = self._scheduled_exec_acts.index(latest_exec_act)

                #  test if the routing plans for the current activity being executed have changed, we need to do something about that
                updated_exec_act = self._scheduled_exec_acts[curr_act_indx]
                if not latest_exec_act.plans_match(updated_exec_act):
                    self.state_recorder.log_event(self._curr_time_dt,'sim_agent_components.py','plan change','Saw updated plans for current ExecutableActivity, current: %s, new: %s'%(latest_exec_act,updated_exec_act))
                    # raise NotImplementedError('Saw updated plans for current ExecutableActivity, current: %s, new: %s'%(latest_exec_act,updated_exec_act))

                self._last_scheduled_exec_act_windex = curr_act_indx
                
            except ValueError:
                no_current_act = True
                #  assume that we have canceled this activity, and reported in the log (it could also be the case that we're not currently executing an act, and latest_exec_act was simply the last act we executed) # todo: maybe filter for this?
                self._cancelled_acts.add(latest_exec_act)
                
                self.state_recorder.log_event(self._curr_time_dt,'sim_agent_components.py','plan change','latest exec act not found in new schedule: %s'%(latest_exec_act))
                
                # - what does it mean if current act is not in new sched? cancel current act?
                # raise RuntimeWarning("Couldn't find activity in new schedule: %s"%(self._curr_exec_act.act))

        if no_current_act:
            # if there are activities in the schedule and we're not currently executing anything, then assume for the moment we're starting from beginning of it (actual index will be resolved in update step)
            if len(self._scheduled_exec_acts) > 0:
                self._last_scheduled_exec_act_windex = 0

            # no acts in schedule, for whatever reason (near end of sim scenario, a fault...)
            else:
                # todo: no act, so what is index? Is the below correct?
                self._last_scheduled_exec_act_windex = None

        self._scheduled_exec_acts_updated_hist.append(self._curr_time_dt)

    def inject_acts(self,acts):
        #  meant to be implemented in subclass
        raise NotImplementedError


    @staticmethod
    def executable_time_accessor(exec_act,time_prop):
        if time_prop == 'start':
            return exec_act.act.executable_start
        elif time_prop == 'end':
            return exec_act.act.executable_end
        else:
            raise NotImplementedError

    def update(self,new_time_dt):
        """ Update state of the executive """

        # If first step, check the time (first step time should be the sim start time)
        if self._first_step:
            if new_time_dt != self._curr_time_dt:
                raise RuntimeWarning('Saw wrong initial time')

        #  if we haven't yet executed the current activities, then we should not be updating activity choices
        if not new_time_dt == self._last_act_execution_time_dt:
            raise RuntimeWarning('Trying to update the executive before executing current activities')

        ##############################
        # determine next activity

        # update schedule if need be
        self._pull_schedule()

        # figure out next activities, index of that act in schedule
        next_exec_acts = []
        regular_exec_acts,exec_act_windices = find_windows_in_wind_list(new_time_dt,self._last_scheduled_exec_act_windex,self._scheduled_exec_acts,self.executable_time_accessor)
        self._last_scheduled_exec_act_windex = exec_act_windices[1]
        next_exec_acts += regular_exec_acts
        #  also consider injected activities ("spontaneous", unplanned activities)
        injected_exec_acts,exec_act_windices = find_windows_in_wind_list(new_time_dt,self._last_injected_exec_act_windex,self._injected_exec_acts,self.executable_time_accessor)
        self._last_injected_exec_act_windex = exec_act_windices[1]
        next_exec_acts += injected_exec_acts

        # sanity check that state sim has advanced to next timestep (new_time_dt) already
        assert(self.state_sim._curr_time_dt == new_time_dt)
        # if satellite state is not nominal at next timestep (new_time_dt), then that means the activities we are currently executing ( at current time step, self._curr_time_dt) have pushed our state over the edge. for that reason, we should not perform any activities at the next time step
        #  note that once we have canceled an activity, it stays canceled
        if not self.state_sim.nominal_state_check():
            for exec_act in next_exec_acts:
                self._cancelled_acts.add(exec_act)
            #  in this case, remove all of the next acts
            next_exec_acts = []


        # remove cancelled acts from next exec acts
        next_exec_acts = [exec_act for exec_act in next_exec_acts if not exec_act in self._cancelled_acts]

        # handle execution context setup/takedown
        
        added_exec_acts = set(next_exec_acts) - set(self._curr_exec_acts)
        removed_exec_acts = set(self._curr_exec_acts) - set(next_exec_acts)

        # Deal with any activities that have stopped
        for exec_act in removed_exec_acts:
            self._cleanup_act_execution_context(exec_act,new_time_dt)

        #  set up execution context for any activities that are just beginning
        #  note that we want to clean up before we initialize, because during initialization data structures are set up that may be dependent on an activity that ends in the time step for this new one. If at some point in the future we want to allow two ongoing activities to handle the same data (i.e. second activity grabs data from a first activity while the first is still ongoing), this initialization and cleanup approach will have to be changed. for the time being though, we assume that activities that execute at the same time handle mutually exclusive data
        for exec_act in added_exec_acts:
            self._initialize_act_execution_context(exec_act,new_time_dt)

        self._curr_exec_acts = next_exec_acts        

        self._last_time_dt = self._curr_time_dt
        self._curr_time_dt = new_time_dt

        # mark no longer first step
        if self._first_step:
            self._first_step = False

    def _initialize_act_execution_context(self,exec_act,new_time_dt):
        """ Sets up the context dictionary used for the execution of a given activity. Should be run before starting to execute it"""

        if exec_act in self._execution_context_by_exec_act.keys():
            raise RuntimeWarning('trying to re-initialize context for an executive act')

        self._execution_context_by_exec_act[exec_act] = {}

        curr_exec_context = self._execution_context_by_exec_act[exec_act]
        curr_exec_context['act'] = exec_act.act
        curr_exec_context['rt_conts'] = exec_act.rt_conts

        # note that if we want to allow activities to begin at a time different from their actual start, need to update code here
        #  the start time seen for this activity - new_time_dt is the time when the executive notices that we start executing the activity, but we want that account for the fact that the act can start in between timesteps for the executive
        curr_exec_context['start_dt'] = min(exec_act.act.executable_start,new_time_dt)

        # assume for now act ends at the expected end
        curr_exec_context['end_dt'] = exec_act.act.executable_end

        #  received data containers (packets) for this activity ( if receiving)
        curr_exec_context['rx_data_conts'] = []
        #  total amount of data volume used for this activity
        curr_exec_context['dv_used'] = 0

        return curr_exec_context

    def _cleanup_act_execution_context(self,exec_act,new_time_dt):

        curr_exec_context = self._execution_context_by_exec_act[exec_act]

        #  set the executed properties for the activity
        #  note that we will actually set these twice for cross-links ( TX and RX sat), but that's okay

        curr_act_wind = curr_exec_context['act']

        #  this function is called when we choose to stop the activity. if we stop before the actual end of the activity, then we need to record that new end time.  for now we assume that the activity ends at the new time (as opposed to self._curr_time_dt), because the state simulator update step runs before the executive update step, and has already propagated state to new_time_dt assuming the activity executes until then. See SatStateSimulator.update().  if the state simulator is not run before the executive, this will need to be changed
        curr_exec_context['end_dt'] = new_time_dt
        curr_act_wind.set_executed_properties(curr_exec_context['start_dt'],curr_exec_context['end_dt'],curr_exec_context['dv_used'],dv_epsilon=0.1)
        self.state_recorder.add_act_hist(curr_act_wind)

        # delete the current exec context, for safety
        self._execution_context_by_exec_act[exec_act] = None

    def execute_acts(self,new_time_dt):
        """ execute current activities that the update step has chosen """

        #  sanity check that if we're executing activities, we do so with a finite time delta for consistency
        if not self._first_step and not new_time_dt > self._last_act_execution_time_dt:
            raise RuntimeWarning('Saw a zero time difference for activity execution')

        #  advance the activity execution time high-water mark
        self._last_act_execution_time_dt = new_time_dt

        # if it's not the first step, then we can execute current activities
        if not self._first_step:
            for exec_act in self._curr_exec_acts:
                self._execute_act(exec_act,new_time_dt)

    def _execute_act(self,exec_act,new_time_dt):
        """ Execute the executable activity input, taking any actions that this agent is responsible for initiating """

        #  intended to be implemented in subclass
        raise NotImplementedError

    def receive_poll(self,tx_ts_start_dt,tx_ts_end_dt,new_time_dt,proposed_act,tx_sat_indx,txsat_data_cont,proposed_dv):
        """Called by a transmitting satellite to see if we can receive data and if yes, handles the received data
        
        [description]
        :param ts_start_dt: the start time for this transmission, as calculated by the transmitting satellite
        :type ts_start_dt: datetime
        :param ts_end_dt: the end time for this transmission, as calculated by the transmitting satellite
        :type ts_end_dt: datetime
        :param proposed_act:  the activity which the transmitting satellite is executing
        :type proposed_act: XlnkWindow
        :param tx_sat_indx:  the index of the transmitting satellite
        :type tx_sat_indx: int
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

        #  if this ground station is not actually executing the activity, then it can't receive data
        # note that both agents have to be referring to the same activity 
        if not proposed_act in [exec_act.act for exec_act in self._curr_exec_acts]:
            received_dv = 0
            rx_success = False
            return received_dv,rx_success

        #############
        # Figure out time window for reception execution, determine how much data can be received

        # get this sat's version of the executable activity
        self_exec_act = self._curr_exec_acts[index_from_key(self._curr_exec_acts,key=lambda ea:ea.act,value=proposed_act)]
        curr_exec_context = self._execution_context_by_exec_act[self_exec_act]

        act_start_dt = curr_exec_context['start_dt']
        act_end_dt = curr_exec_context['end_dt'] 
        rx_ts_start_dt, rx_ts_end_dt = self.find_act_start_end_times(act_start_dt, act_end_dt, self._last_time_dt, self._curr_time_dt,new_time_dt)

        ts_start_dt = max(tx_ts_start_dt,rx_ts_start_dt)
        ts_end_dt = min(tx_ts_end_dt,rx_ts_end_dt)

        #  if this is not true, that means the two time periods for the agents are not overlapping, and the executives likely have different current times. that should not be allowed in the simulation. Note that will assume they are both using the same time step and so the transmitting satellite that is polling this satellite integrates its data rate over the same amount of time
        if not ts_end_dt >= ts_start_dt:
            raise RuntimeWarning('time mismatch between tx and rx agents')

        rx_delta_dv = proposed_act.ave_data_rate * (ts_end_dt-ts_start_dt).total_seconds()

        #  reduce this number by any restrictions due to data volume storage availability
        #  note that storage availability is affected by activity execution order at the current time step ( if more than one activity is to be executed)
        rx_delta_dv = min(rx_delta_dv,self.state_sim.get_available_data_storage(self._curr_time_dt,rx_delta_dv))

        received_dv = min(proposed_dv,rx_delta_dv)


        # todo: need to add recording of tlm, cmd update

        #  if we're not able to receive data, then return immediately
        if received_dv < self.dv_epsilon:
            rx_success = False
            return received_dv,rx_success

        #############
        # Store the incoming data in the appropriate place

        #  check if the transmitted data container has changed or not
        tx_data_cont_changed = False
        if not txsat_data_cont == curr_exec_context['curr_txsat_data_cont']:
            curr_exec_context['curr_txsat_data_cont'] = txsat_data_cont
            tx_data_cont_changed = True

        #  ingest the data received into the appropriate destination
        self._curr_dc_indx = self.ingest_rx_data(
            curr_exec_context['rx_data_conts'],
            proposed_act,
            txsat_data_cont,
            received_dv,
            self.sim_executive_agent.dc_agent_id,
            tx_sat_indx,
            self._curr_dc_indx,
            tx_data_cont_changed
        )


        #  Mark the data volume received
        curr_exec_context['dv_used'] += received_dv

        #  factor this executed Delta into the total data volume Delta for this time step
        self.state_sim.add_to_data_storage(received_dv,curr_exec_context['rx_data_conts'],self._curr_time_dt)

        return received_dv,True

    @staticmethod
    def ingest_rx_data(rx_data_conts_list,act,txsat_data_cont,received_dv,rx_agent_id,tx_sat_indx,dc_indx,tx_data_cont_changed):
        """Ingest received data, and do the process of putting that data into its proper data container destination"""

        #  if we've already been receiving data from this transmitted data container, then we should still receive into the same reception data container
        if not tx_data_cont_changed:
            rx_dc = rx_data_conts_list[-1]
        #  if the transmitted data container has changed, then we can make a new reception data container
        else:
            #  start out with zero data volume
            new_rx_dc = txsat_data_cont.fork(rx_agent_id,new_dc_indx=dc_indx,dv=0)
            dc_indx += 1
            new_rx_dc.add_to_route(act,tx_sat_indx)
            rx_data_conts_list.append(new_rx_dc)
            rx_dc = new_rx_dc

        rx_dc.add_dv(received_dv)

        return dc_indx

    @staticmethod
    def find_act_start_end_times(act_start_dt,act_end_dt,last_time_dt,curr_time_dt,next_time_dt):
        """ find the start and end time to use during this time step, accounting for activity start and end bound boundary conditions"""

        #  in order to allow relatively large time steps (to save computation time), we need to consider if the start/end of an activity falls between time steps for the executive

        # deal with the fact that the start might not fall exactly on a timepoint. If the start occurred between last time and current time, pick that instead of the current time
        assert(act_start_dt <= curr_time_dt)
        if act_start_dt > last_time_dt:
            ts_start_dt = act_start_dt
        else:
            ts_start_dt = curr_time_dt

        assert(act_end_dt >= curr_time_dt)
        #  deal with the fact that the end might not fall exactly on time point. if the end comes before the new time, use the end time
        if act_end_dt <= next_time_dt:
            #  we check the executable end here
            ts_end_dt = act_end_dt
        else:
            ts_end_dt = next_time_dt

        return ts_start_dt, ts_end_dt

class StateRecorder:
    """ records state for an agent"""

    def __init__(self,sim_start_dt):
        self.DS_state_hist = [] # assumed to be in Mb
        self.base_time = sim_start_dt


        self.event_hist_by_type ={}

    def add_DS_hist(self,t_dt,val):
        # todo: add decimation?
        t_s = (t_dt - self.base_time).total_seconds()
        self.DS_state_hist.append((t_s,val))

    def get_DS_hist(self,out_units='minutes'):

        if not out_units == 'minutes':
            raise NotImplementedError

        t = []
        d = []
        for pt in self.DS_state_hist:
            t.append(pt[0]/60.0) # converted to minutes
            d.append(pt[1]) # converted to Gb

        return t,d

    def log_event(self,time_dt,source,event_type,message):
        self.event_hist_by_type.setdefault(event_type,[])
        self.event_hist_by_type[event_type].append([time_dt.isoformat(),source,message])

    def get_events(self):
        return self.event_hist_by_type
        
class ExecutiveAgentStateRecorder(StateRecorder):
    """Convenient interface for storing state history for executive agents"""

    def __init__(self,sim_start_dt):
        self.act_hist = []

        super().__init__(sim_start_dt)

    def add_act_hist(self,act):
        # note that the execution times, dv for act are stored in attributes: executable_start,executable_end,executable_data_vol
        # do a deepcopy so it can't be changed out from under us
        self.act_hist.append(deepcopy(act))

    def get_act_hist(self):
        acts_exe = {'dlnk': []}
        for act in self.act_hist:
            if type(act) == DlnkWindow: acts_exe['dlnk'].append(act)

        return acts_exe

class DataStore:
    """ Database for data containers (packets) stored on an agent"""
    #  add tracking of data store history?

    def __init__(self):

        #  database for data containers currently within the data store
        self.data_conts_by_id = {}

    def add(self,data_conts):
        """Add data containers (packets) to the data store"""

        for dc in data_conts:
            # only add if it's not present yet
            self.data_conts_by_id.setdefault(dc.ID,dc)

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

    def get_total_dv(self):
        """Get the total amount of data volume stored currently"""
        return sum(dc.data_vol for dc in self.data_conts_by_id.values())


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
        # stores the times at which rt conts were updated
        self.sim_rt_cont_update_hist_by_id = {}
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt

        #  holds list of satellite IDs in order
        self.sat_id_order = None
        # holds list of gs IDs in order
        self.gs_id_order = None

        #  contains state information history for every satellite
        self.sat_state_hist_by_id = {}  # the id here is the satellite
        #  contains information about events that happen on satellites ( outside of scheduled activities), as well as other general satellite specific information needed for keeping track of planning info
        self.sat_events = {}
        #  holds power parameters for each satellite ID,  after parsing
        self.parsed_power_params_by_sat_id = {}

    def initialize(self,plan_db_inputs):
        self.sat_id_order = plan_db_inputs['sat_id_order']
        self.gs_id_order = plan_db_inputs['gs_id_order']
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
            sat_edot_by_mode,sat_batt_storage,power_units,_,_ = io_tools.parse_power_consumption_params(plan_db_inputs['power_params_by_sat_id'][sat_id])
            self.parsed_power_params_by_sat_id[sat_id] = {
                "sat_edot_by_mode": sat_edot_by_mode,
                "sat_batt_storage": sat_batt_storage,
                "power_units": power_units,
            } 

    def update_routes(self,rt_conts,curr_time_dt):
        for rt_cont in rt_conts:
            if rt_cont.ID in self.sim_rt_conts_by_id.keys():
                # todo: is this the way to always do updates?
                if rt_cont.update_dt > self.sim_rt_conts_by_id[rt_cont.ID].update_dt:
                    self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont
                    self.sim_rt_cont_update_hist_by_id[rt_cont.ID].append(curr_time_dt)
            else:
                self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont
                self.sim_rt_cont_update_hist_by_id.setdefault(rt_cont.ID,[curr_time_dt])

    def get_filtered_sim_routes(self,filter_start_dt=None,filter_end_dt=None,filter_opt='partially_within',sat_id=None,gs_id = None):

        if not filter_opt in self.filter_opts:
            raise NotImplementedError

        if (sat_id is not None) and (gs_id is not None):
            raise RuntimeError('should not filter with both satellite ID and ground station ID present')

        def do_filter(rt_cont):
            if sat_id is not None:
                #  check if the route container includes the satellite ID
                if not rt_cont.has_sat_indx(self.sat_id_order.index(sat_id)): 
                    return False

            if gs_id is not None:
                #  check if the route container includes the ground station ID
                if not rt_cont.has_gs_indx(self.gs_id_order.index(gs_id)): 
                    return False

            if not check_temporal_overlap(rt_cont.get_start(),rt_cont.get_end(),filter_start_dt,filter_end_dt,filter_opt):
                return False

            return True

        all_rt_conts = []
        for rt_cont in self.sim_rt_conts_by_id.values():
            if do_filter(rt_cont):
                all_rt_conts.append(rt_cont)

        return all_rt_conts

    def push_planning_info(self,other,curr_time_dt):
        """ Update other with planning information from self"""

        other.update_routes(self.sim_rt_conts_by_id.values(),curr_time_dt)

        #  also update satellite state history record
        for sat_id in self.sat_id_order:
            if len(self.sat_state_hist_by_id[sat_id]) > 0:
                other.update_sat_state_hist(sat_id,self.sat_state_hist_by_id[sat_id][-1])

    def update_sat_state_hist(self,sat_id,sat_state_entry):
        """ update the latest satellite state entry for given satellite ID, if necessary"""

        if len(self.sat_state_hist_by_id[sat_id]) > 0:
            latest = self.sat_state_hist_by_id[sat_id][-1]
            if latest.update_dt < sat_state_entry.update_dt:
                self.sat_state_hist_by_id[sat_id].append(sat_state_entry)
        else:
            self.sat_state_hist_by_id[sat_id].append(sat_state_entry)
            

    def get_sat_states(self, curr_time_dt):
        """ get satellite states at the input time. Propagates each satellite's state forward from its last known state to the current time. Includes any known scheduled activities in this propagation"""

        curr_sat_state_by_id = {}


        for sat_indx,sat_id in enumerate(self.sat_id_order):
            curr_sat_state = {}

            #  get most recent update of satellite state. known_sat_state is a SatStateEntry type
            known_sat_state = self.sat_state_hist_by_id[sat_id][-1]
            last_update_dt = known_sat_state.update_dt

            # get routes between last update time and current time that have the satellite ID somewhere along the route
            rt_conts = self.get_filtered_sim_routes(filter_start_dt=last_update_dt,filter_end_dt=curr_time_dt,filter_opt='partially_within',sat_id=sat_id)
    
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

    def get_rt_conts_by_obs(self):
        """Get dict of every route container seen for each observation """

        rt_conts_by_obs = {}

        # each of these should be a single route container
        for rt_cont in self.sim_rt_conts_by_id.values():
            rt_conts_by_obs.setdefault(rt_cont.get_obs(), []).append(rt_cont)

        return rt_conts_by_obs

    def get_all_rt_conts(self):
        """Get list of every route container seen for all observations """

        # each of these should be a single route container
        return [rt_cont for rt_cont in self.sim_rt_conts_by_id.values()]



