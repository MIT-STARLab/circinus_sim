from .sim_routing_objects import ExecutableActivity
from copy import deepcopy

def synthesize_executable_acts(rt_conts,filter_start_dt=None,filter_end_dt=None,sat_indx=None):
    """ go through all of the route containers and synthesize a list of unique windows to execute with the correct time and the data volume utilization"""

    # First we need to find all of the executable versions of each activity window contained anywhere in the data routes in the route containers.
    # note!  there may be duplicate copies of wind residing within the executable Windows objects.  in general this is okay though because we use their window ID hash for lookup
    exec_acts_by_wind = {}
    for rt_cont in rt_conts:
        # this is an iterable of type ExecutableActivity
        rt_cont_exec_winds = rt_cont.get_winds_executable(filter_start_dt,filter_end_dt,sat_indx)
        for exec_act in rt_cont_exec_winds:
            exec_acts_by_wind.setdefault(exec_act.wind, [])
            exec_acts_by_wind[exec_act.wind].append(exec_act)

    #  then we need to synthesize these possibly disagreeing executable versions of each activity window into a single executable version; that is, we need to figure out the time utilization and DV utilization from every Sim route container and take the maximum over those. we take the sum because every route only accounts for how much of the activity window IT needs to use. if we take the sum over all routes, then we know how much of the window we actually need to use across all routes
    executable_acts_sythesized = []
    for act,exec_acts in exec_acts_by_wind.items():
        # exec_act is of type ExecutableActivity
        
        #  these are the amounts of data volume used for every route passing through this window ( well, every route within our filter)
        dvs_used = [exec_act.dv_used for exec_act in exec_acts]
        dv_used = sum(dvs_used)
        #  merge all of the route containers for this activity into a single list
        synth_rt_conts = []
        for exec_act in exec_acts:
            synth_rt_conts += exec_act.rt_conts

        # make a deepcopy so we don't risk information crossing the ether in the simulation...
        act = deepcopy(act)
        dv_epsilon = exec_acts[0].dv_epsilon
        act.set_executable_properties(dv_used,dv_epsilon)

        synth_exec_act = ExecutableActivity(
            wind=act,
            rt_conts=synth_rt_conts,
            dv_used=dv_used  # not really necessary keep track of this anymore, but throw it in for convenience
        )
        executable_acts_sythesized.append(synth_exec_act)

        #  do some quick sanity checks. every window should be the same, and the sum of the time and data volume utilizations should be less than or equal to full utilization (100%)
        #  note that this window equality check checks the hash of the window (window ID). they don't have to be exactly the same python runtime object, just the same activity in the simulation
        assert(all(act == exec_act.wind for exec_act in exec_acts))

    return executable_acts_sythesized


