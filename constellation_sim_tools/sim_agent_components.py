
class PlannerScheduler:

    def __init__(self,sim_start_dt,sim_end_dt):
        
        self.plan_db = PlanningInfoDB(sim_start_dt,sim_end_dt)

    def get_plan_db(self):
        return self.plan_db


class StateRecorder:

    def __init__(self):
        pass

class PlanningInfoDB:

    filter_opts = ['totally_within','partially_within']

    def __init__(self,sim_start_dt,sim_end_dt):
        # todo: make this a dictionary by sat index? Could be a bit faster
        # todo: should add distinction between active and old routes
        self.sim_rt_conts_by_id = {}
        self.sim_start_dt = sim_start_dt
        self.sim_end_dt = sim_end_dt

    def update_routes(self,rt_conts):
        for rt_cont in rt_conts:
            if rt_cont.ID in self.sim_rt_conts_by_id.keys()
                # todo: is this the way to always do updates?
                if rt_cont.update_dt > self.sim_rt_conts_by_id[rt_cont.ID].update_dt:
                    self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont
            else:
                self.sim_rt_conts_by_id[rt_cont.ID] = rt_cont

    def get_filtered_sim_routes(self,start_time_dt,end_time_dt=None,filter_opt='partially_within'):

        if not end_time_dt:
            end_time_dt = self.sim_end_dt

        if not filter_opt in filter_opts:
            raise NotImplementedError

        passes_filter(rt_cont):
            if filter_opt == 'totally_within':
                if (rt_cont.start < start_time_dt or rt_cont.end > end_time_dt):
                    return False
            if filter_opt == 'partially_within':
                if (rt_cont.end < start_filt_dt or rt_cont.start > end_time_dt):
                    return False
            return True

        all_rt_conts = []
        for rt_cont in self.sim_rt_conts_by_id.values():
            if passes_filter(rt_cont):
                all_rt_conts.append(rt_cont)

        return all_rt_conts

    def push_planning_info(self,other):
        """ Update other with planning information from self"""

        other.update_routes(self.sim_rt_conts_by_id.values())

