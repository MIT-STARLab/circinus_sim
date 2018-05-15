from collections import namedtuple

from .sim_agent_components import PlannerScheduler

class GroundNetworkPS(PlannerScheduler):
    """Handles calling of the GP, and ingestion of plan and state updates from satellites"""

    ReplanQEntry = namedtuple('ReplanQEntry', 'time_dt rt_conts')

    def __init__(self,sim_gsn,sim_start_dt,sim_end_dt,gp_wrapper,gsn_ps_params,sats_event_data):
        # holds ref to the containing sim ground network
        self.sim_gsn = sim_gsn

        self.curr_time_dt = start_time_dt

        self.gp_wrapper = gp_wrapper

        #  time to wait since last plans were released before rerunning planning
        self.replan_interval_s = gsn_ps_params['replan_interval_s']

        #  this is the FIFO waiting list for newly produced plans.  when current time reaches the time for the first entry, that entry will be popped and the planning info database will be updated with those plans (each entry is a ReplanQEntry named tuple). this too should always stay sorted in ascending order of entry add time
        self._replan_release_q = []
        #  the time that the scheduler waits after starting replanting before it makes the new plans available for sharing with other agents. this mechanism simulates the time required for planning in real life. set this to zero to release plans immediately ( meaning we assume planning is instantaneous)
        self.replan_release_wait_time_s = gsn_ps_params['replan_release_wait_time_s']
        #  if true first plans created at beginning of simulation will be released immediately
        self.release_first_plans_immediately = gsn_ps_params['release_first_plans_immediately']

        self._first_planning = True
        self._last_replan_time_dt = None

        self.plans_updated = False

        # This keeps track of the latest data route index created. (the DR "uid" in the GP algorithm)
        self.latest_gp_route_indx = 0

        super().__init__(sim_start_dt,sim_end_dt,sats_event_data)

    def update(self,new_time_dt):
        """  this code calls the global planner. it deals with the fact that in reality it takes time to run the global planner. for this reason we add the output of the global planner to a queue, and only release those new plans once enough simulation time is past for those plans to be 'available'.  we do this primarily because satellites can talk to the ground station at any time, so we don't want to do an instantaneous plan update right after the ground station talks to one satellite and then share those plans with another satellite immediately. We have to wait a replan time. """
        
        #  see if we need to replan at this time step
        # add in consideration for lots of received updated planning information from satellites causing a need to rerun global planner?
        replan_required = False
        if self._last_replan_time_dt is None:
            replan_required = True
        elif (self.curr_time_dt - self._last_replan_time_dt).total_seconds() > self.replan_interval_s:
            replan_required = True

        #  perform re-plan if required, and release or add to queue as appropriate
        if replan_required:

            if len(self._replan_release_q) > 0:
                raise RuntimeWarning('Trying to rerun GP while there already plan results waiting for release.')

            new_rt_conts = self.run_planner()

            #  if we don't have to wait to release new plans
            if self.replan_release_wait_time_s == 0:
                # update plan database
                self.plan_db.update_routes(new_rt_conts)
                #  update replan time
                self._last_replan_time_dt = self.curr_time_dt
                self.plans_updated = True

            #  if this is the first plan cycle (beginning of simulation), there's an option to release immediately
            elif self._first_planning and self.release_first_plans_immediately:
                self.plan_db.update_routes(new_rt_conts)
                self._last_replan_time_dt = self.curr_time_dt
                self.plans_updated = True

            #  if it's not the first time and there is a wait required
            else:
                self._replan_release_q.append(ReplanQEntry(time_dt=self.curr_time_dt,rt_conts=new_rt_conts))

        #  release any ripe plans from the queue
        while len(self._replan_release_q)>0 and self._replan_release_q[0] < self.curr_time_dt:
            q_entry = self._replan_release_q.pop(0)
            self.plan_db.update_routes(q_entry.rt_conts)
            self._last_replan_time_dt = q_entry.time_dt
            self.plans_updated = True


        #  time update
        #  if new time is none then don't update with it (this idiom allows us to run update step at very beginning of sim without advancing time)
        if new_time_dt:
            self.curr_time_dt = new_time_dt

    def run_planner(self):

        #  get already existing sim route containers that need to be fed to the global planner
        existing_sim_rt_conts = self.plan_db.get_filtered_sim_routes(self.curr_time_dt,filter_opt='partially_within')

        sat_state_by_id = self.plan_db.get_sat_states(self.curr_time_dt)

        #  run the global planner
        new_rt_conts, latest_gp_route_uid = gp_wrapper.run_gp(self.curr_time_dt,existing_sim_rt_conts,self.sim_gsn.ID,self.latest_gp_route_indx,sat_state_by_id)

        #  I figure this can be done immediately and it's okay -  immediately updating the latest route index shouldn't be bad. todo:  confirm this is okay
        self.latest_gp_route_indx = latest_gp_route_uid

        return new_rt_conts

