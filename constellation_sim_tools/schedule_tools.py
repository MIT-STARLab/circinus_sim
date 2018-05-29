from copy import deepcopy

from circinus_tools import debug_tools

class ExecutableActivity:
    """ this is an object that keeps track of an activity window, and the route containers (with their underlying data routes) whose executions are the reason why the activity window is being performed. this helps us keep track of why a window is being performed and where data goes to and arrives from"""

    def __init__(self,wind,rt_conts,dv_used,injected=False):
        #  the activity window for this object
        self.wind =  wind
        # rt_conts are the route containers for the window
        self.rt_conts =  rt_conts
        # dv_used is the amount of data volume used for this window within the route in which the wind was found
        self.dv_used =  dv_used
        # Used to indicate if the activity is "injected" or not. "injected" means that it's a spontaneous/unplanned-for activity
        self.injected = injected

    def __hash__(self):
        # xor the components together
        return hash(self.wind)

    def __eq__(self, other):
        """this compares equality based on the activity window"""
        return hash(self) == hash(other)

    def __repr__(self):
        return "(ExecAct, wind: %s, dv_used: %f,# rt_conts: %d)"%(self.wind,self.dv_used,len(self.rt_conts))

    @property
    def act(self):
        return self.wind

    @property
    def dv_epsilon(self):
        #  just grab the dv epsilon from the first route container
        return self.rt_conts[0].get_dv_epsilon()

    def plans_match(self,other):
        """ check if two executable activities have the same plans ( their route containers are the same)"""

        try:
            #  search for all of our route containers in the other's route containers
            for rt_cont in self.rt_conts:
                other_rt_cont = other.rt_conts[other.rt_conts.index(rt_cont)]
                # if the update times are different, than they are not matching
                if not rt_cont.update_dt == other_rt_cont.update_dt:
                    return False

        #  if we cannot find the right container at all
        except ValueError:
            return False

        return True

def synthesize_executable_acts(rt_conts,filter_start_dt=None,filter_end_dt=None,filter_opt='partially_within',sat_indx=None,gs_indx=None):
    """ go through all of the route containers and synthesize a list of unique windows to execute with the correct time and the data volume utilization"""

    # if sat_indx == 1:
    #     debug_tools.debug_breakpt()


    # First we need to find all of the executable versions of each activity window contained anywhere in the data routes in the route containers.
    # note!  there may be duplicate copies of wind residing within the executable Windows objects.  in general this is okay though because we use their window ID hash for lookup
    exec_acts_by_wind = {}
    for rt_cont in rt_conts:
        # this is an iterable of type ExecutableActivity
        rt_cont_exec_winds = rt_cont.get_winds_executable(filter_start_dt,filter_end_dt,filter_opt,sat_indx,gs_indx)
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

def check_temporal_overlap(window_start_dt,window_end_dt,filter_start_dt=None,filter_end_dt=None,filter_opt='partially_within'):
    """ Check whether or not a window of time overlaps a filter window
    
    Filters a monolithic window of interest based upon a filter window. looks at whether or not the two windows overlap each other
    :param start_time:  the start of the window of interest
    :type start_time: datetime
    :param end_time:  the end of the window of interest
    :type end_time: datetime
    :param filter_start_dt:  the start of the filter window ( if desired)
    :type filter_start_dt: datetime or None
    :param filter_end_dt:  the end of the filter window ( if desired)
    :type filter_end_dt: datetime or None
    :param filter_opt:  the type of filter to apply, defaults to 'partially_within'
    :type filter_opt: str, optional
    :returns:  true if the windows overlap for the specified filter
    :rtype: {bool}
    :raises: NotImplementedError
    """

    # ignore if window at doesn't at least overlap with our filtered time period by a little bit
    if filter_opt == 'partially_within':
        if filter_start_dt and window_end_dt < filter_start_dt:
            return False
        if filter_end_dt and window_start_dt > filter_end_dt:
            return False
    # ignore if window doesn't completely overlap with our filtered time period
    elif filter_opt == 'totally_within':
        if filter_start_dt and window_start_dt < filter_start_dt:
            return False
        if filter_end_dt and window_end_dt > filter_end_dt:
            return False
    else:
        raise NotImplementedError

    return True


