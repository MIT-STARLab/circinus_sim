
class ScheduleArbiter:

    def __init__(self):
        pass

class StateRecorder:

    def __init__(self):
        pass

class PlanningInfoDB:

    def __init__(self):
        # todo: make this a dictionary by sat index? Could be a bit faster
        self.sim_rt_conts = set()

    def add_routes(self,rt_conts):
        # todo: deal with routes already existing in db
        self.sim_rt_conts.add(rt_conts)

    def get_sim_routes(self,time_dt):
        all_rt_conts = []
        for rt_cont in self.sim_rt_conts:
            if rt_cont.end > time_dt:
                all_rt_conts.append(rt_cont)

        return all_rt_conts

