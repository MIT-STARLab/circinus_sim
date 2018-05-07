from .sim_agent_components import PlannerScheduler

class GroundNetworkPS(PlannerScheduler):
    """Handles calling of the GP, and ingestion of plan and state updates from satellites"""

    def __init__(self,sim_gsn,sim_start_dt,gp_wrapper):
        # holds ref to the containing sim ground network
        self.sim_gsn = sim_gsn

        self.plan_db = PlanningInfoDB(sim_start_dt,sim_end_dt)

        self.curr_time_dt = start_time_dt

        self.gp_wrapper = gp_wrapper

        super().__init__()

    def do_plan_and_sched(self):
        #  get already existing sim route containers that need to be fed to the global planner
        existing_sim_rt_conts = self.plan_db.get_sim_routes(self.curr_time_dt,filter_opt='partially_within')

        #  run the global planner
        new_rt_conts = gp_wrapper.run_gp(self.curr_time_dt,existing_sim_rt_conts,self.sim_gsn.ID)

        #  add to database
        self.plan_db.update(new_rt_conts)


