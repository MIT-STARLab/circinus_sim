from copy import copy

from circinus_tools.scheduling.custom_window import ObsWindow
from circinus_tools.scheduling.routing_objects import DataRoute, DataMultiRoute, RoutingObjectID
from circinus_tools  import  time_tools as tt
from .schedule_tools  import check_temporal_overlap, ExecutableActivity

from circinus_tools import debug_tools

class SimDataContainer:
    '''This is essentially an observation data packet. Contains relevant information for tracking of data across sim run'''

    # note this route is simple:  there are no forks in the route; there is a simple linear path from an observation to a downlink through which data flows. all windows must be in temporal order.

    def __init__(self,agent_id,sat_indx,dc_indx,route,dv=0,injected=False):

        if agent_id is not None:
            self.ID = RoutingObjectID(agent_id,dc_indx,rt_obj_type='obs_data_pkt')
        else:
            self.ID = None

        if type(route) == ObsWindow:
            obs = route
            route = [obs]
            # this stores the data route taken by the observation data in this container. Note that it is a simple data route, NOT a data multi route
            self._executed_dr = DataRoute(agent_id,dc_indx, route, window_start_sats={obs:sat_indx},dv=dv)
        elif type(route) == DataRoute:
            self._executed_dr = route
        else:
            raise NotImplementedError

        #  indicates whether or not the observation that created this data container was injected
        self.injected = injected

        # keeps track of the IDs held by this route, in the course of forking
        self._ID_hist = []

        # Contains all of the route containers that specified the plans for routing this data. this history is added to every time data is moved between agents.
        # Note that when planning info is updated, this planned route history should be updated as well, if the global planner is intended to update existing data conts
        self._planned_rt_hist = []

    @property
    def data_vol(self):
        return self._executed_dr.data_vol

    @property
    def executed_data_route(self):
        return self._executed_dr

    @property
    def latest_planned_rt_cont(self):
        """ get the latest planned route for this data container. This can be trusted as the specification of where the data is planned to be sent, at any given time"""

        # Note that it is an error for the planned route history to be empty -  it should always be populated by at least one entry after a new Sim data route is created
        latest_planned_rt_cont = self._planned_rt_hist[-1]
            
        #  if there is a (most recent) planned route for this data container, then the executed data route should be contained within that planned route. this is basically saying that the plans for the data container must be up-to-date with its actual execution
        if latest_planned_rt_cont:
            if not latest_planned_rt_cont.contains_route(self._executed_dr):
                raise RuntimeWarning(' executed data route not contained within the latest planned route for data container. Executed route: %s, latest planned route: %s'%(self._executed_dr,latest_planned_rt_cont))

        return latest_planned_rt_cont

    def __repr__(self):
        return "(SimDataContainer %s: dv %f,<%s>)"%(self.ID,self._executed_dr.data_vol,self._executed_dr.get_route_string())

    def __copy__(self):
        #  the nones in the instantiation will be taken care of in the following lines
        newone = type(self)(agent_id=None, sat_indx=None, dc_indx=None, route=copy(self._executed_dr),dv=None)
        newone.ID = self.ID
        newone._ID_hist = copy(self._ID_hist)
        # copy the underlying route containers, because we want to be very sure that any of the route containers in this history cannot be changed. ( note that this does not break route container equality comparison because they are compared based on the routing object ID)
        newone._planned_rt_hist = [copy(rt_cont) for rt_cont in self._planned_rt_hist]
        return newone

    def add_dv(self,delta_dv):
        """Add data to the data container, assuming the same route as currently specified"""
        self._executed_dr.add_dv(delta_dv)

    def remove_dv(self,delta_dv):
        """Remove data from the data container, assuming the same route as currently specified"""
        self._executed_dr.remove_dv(delta_dv)

    def add_to_id_hist(self,ID):
        self._ID_hist.append(ID)

    def add_to_route(self,wind,window_start_sat_indx):
        self._executed_dr.append_wind_to_route(wind, window_start_sat_indx)

    def fork(self,new_agent_id,new_dc_indx,dv=0):
        new_ro_id = RoutingObjectID(new_agent_id,new_dc_indx,rt_obj_type='obs_data_pkt')
        newone = copy(self)
        newone.ID = new_ro_id
        newone._executed_dr.data_vol = dv
        newone.add_to_id_hist(self.ID)
        return newone

    def add_to_plan_hist(self,rt_cont):
        """ add a route container to the history of routes that were the reason for transmitting this data"""

        if len(self._planned_rt_hist) > 0:
            #  sanity check: None is a valid option for the first step of the route history, if observation data was collected without a route in mind, but none is not a valid option after that point,  because cross-links and down links should always have a route in mind when sending data
            assert(rt_cont is not None)

            #  because this function might be called many times for a single transmission activity, we want to check and make sure that we haven't already added this route container at the end of the history
            if not rt_cont == self._planned_rt_hist[-1]:
                self._planned_rt_hist.append(rt_cont)
        else:
            self._planned_rt_hist.append(rt_cont)

    def is_stale(self,time_dt):
        """ check if there is still a valid plan for routing data container"""
        rc = self.latest_planned_rt_cont

        # If there is no planned route for this data container, then it is definitely still
        if rc is None:
            return True

        next_planned_wind = rc.get_next_planned_wind(self._executed_dr)

        # if we're already past the next planned window for the planned route for this data cont, it's stale.
        # use original_end to make the check less stringent
        if next_planned_wind.original_end < time_dt:
            return True
        else:
            return False
    
class SimRouteContainer:
    """ This object wraps the data routes produced in both the global and local planners, for use in simulation. it can essentially be thought of as a data route, in the context of the constellation simulation.

    This contains lower level data route objects, for use in the constellation simulation. It effectively allows the simulation to easily vary the amount of data volume, time scheduled for a data route, in order to flexibly replan in realtime. 

    The use of this container is an artifact of the choice to model data routes as a series of activity windows of fixed start/end times. If we wish to change any of those start/end times, that would be reflected as a change in the underlying window object, but not in the data route object itself, which is not great. So we'd have to create a new data route object - which is not great for rescheduling because its harder to track the data route object as it gets passed around the constellation. By wrapping the data route in a container that can change its state, we have both timing/dv flexibility and persistent object indexing"""

    #  note that this container currently can only hold data multi-routes. 
    # note that i put the mechanisms in place here to have multiple DMRs in a single sim route container, but I don't use that capability currently. Mildly regretting doing so...

    def __init__(self,ro_ID,dmrs,dv_utilization_by_dmr_id,creation_dt,update_dt,creator_agent_ID):

        if not type(ro_ID) == RoutingObjectID:
            raise RuntimeWarning(' should not use anything but a RoutingObjectID as the ID for a DataMultiRoute')
            
        self.ID = ro_ID
        dmrs_by_id = {}

        #  this is used for copying functionality
        if dmrs is None:
            pass
        # handle case where we're only passed a single route (standard)
        elif type(dmrs) == DataMultiRoute:
            dmr = dmrs

            # assert(isinstance(t_utilization_by_dr_id,float))
            # t_utilization_by_dr_id = {dmr.ID:t_utilization_by_dr_id}
            assert(isinstance(dv_utilization_by_dmr_id,float))
            dv_utilization_by_dmr_id = {dmr.ID:dv_utilization_by_dmr_id}

            # make it a list
            dmrs_by_id = {dmr.ID:dmr}
        else:
            NotImplementedError

        # todo: relax this type constraint?
        for dmr in dmrs_by_id.values():
            if not type(dmr) == DataMultiRoute:
                raise RuntimeWarning('Expected a DataMultiRoute, found %s'%(dmr))

            # if not t_utilization_by_dr_id[dmr.ID] == dv_utilization_by_dmr_id[dmr.ID]:
            #     raise RuntimeWarning('For current version of global planner, expect time and data volume utilization for a data route to be the same. DMR: %s'%(dmr))

        if ro_ID:
            self.ID = ro_ID
        else:
            self.ID = RoutingObjectID(agent_ID,agent_ID_index)

        self.dmrs_by_id = dmrs_by_id

        # this is the "time utilization" for the data route (DMR), which is a number from 0 to 1.0 by which the duration for every window in the route should be multiplied to determine how long the window will actually be executed in the real, final schedule
        # self.t_utilization_by_dr_id = t_utilization_by_dr_id
        # this is the "data volume utilization" for the data route (DMR), which is a number from 0 to 1.0 by which the scheduled data volume for every window in the route should be multiplied to determine how much data volume will actually be throughput on this dr in the real, final schedule
        self.dv_utilization_by_dmr_id = dv_utilization_by_dmr_id
        self.update_dt = update_dt
        self.creation_dt = creation_dt

        # allowable difference in utilization before a DMR is considered "changed" in utilization
        self.dv_utilization_epsilon = 0.001
        self.dv_epsilon = 0.01  # Mb

        self.output_date_str_format = 'short'

        self.creator_agent_ID = creator_agent_ID

    @property
    def data_vol(self):
        return sum(dmr.data_vol*self.dv_utilization_by_dmr_id[dmr.ID] for dmr in self.dmrs_by_id.values())

    def __repr__(self):
        creation_dt_str = tt.date_string(self.creation_dt,self.output_date_str_format) if self.creation_dt else 'None'
        update_dt_str = tt.date_string(self.update_dt,self.output_date_str_format) if self.update_dt else 'None'
        return '(SRC %s, ct %s, ut %s: %s)'%(self.ID,creation_dt_str,update_dt_str,self.get_display_string())

    def __copy__(self):
        # want to make sure that the copy made of this route container copies the underlying data route objects, so that route containers don't end up sharing these.  for this reason, this copy function should be used sparingly
        #  note the assumption here that ideas will never be changed at a low level, so copying the reference is fine
        #  the nones in the instantiation will be taken care of in the following lines
        newone = type(self)(ro_ID=self.ID, dmrs=None, dv_utilization_by_dmr_id=None, creation_dt=self.creation_dt, update_dt=self.update_dt, creator_agent_ID=self.creator_agent_ID)
        newone.dmrs_by_id = {ID:copy(dmr) for ID,dmr in self.dmrs_by_id.items()}
        newone.dv_utilization_by_dmr_id = copy(self.dv_utilization_by_dmr_id)
        return newone
        
    def get_display_string(self):
        return 'utilization_by_dmr: %s'%({'DMR %s - '%(dr_id) +self.dmrs_by_id[dr_id].get_display_string():util for dr_id,util in self.dv_utilization_by_dmr_id.items()})

    def get_start(self,time_opt='regular'):
        # get earliest start of all dmrs
        return min(dmr.get_start(time_opt) for dmr in self.dmrs_by_id.values())

    def get_end(self,time_opt='regular'):
        # get latest end of all dmrs
        return max(dmr.get_end(time_opt) for dmr in self.dmrs_by_id.values())

    def get_obs(self):
        """ get the observation of the first DMR in this SRC """
        # note the assumption that all DRs here have the same obs
        # todo: is this too hacky? Reasses at some point
        return list(self.dmrs_by_id.values())[0].get_obs()

    def get_dlnk(self):
        """ get the downlink of the first DMR in this SRC """
        # note the assumption that all DRs here have the same obs
        # todo: is this too hacky? Reasses at some point
        return list(self.dmrs_by_id.values())[0].get_dlnk()

    def get_latency(self,units='minutes',obs_option = 'original_end', dlnk_option = 'center'):
        """ get the observation of the first DMR in this SRC """
        # note the assumption that all DRs here have the same latency
        # todo: is this too hacky? Reasses at some point
        return list(self.dmrs_by_id.values())[0].get_latency(units,obs_option,dlnk_option)
    

    def get_dmr_utilization(self,dmr):
        return self.dv_utilization_by_dmr_id[dmr]

    def has_sat_indx(self,sat_indx):
        """ return true if satellite with sat index is contained within the route in this route container"""
        for dmr in self.dmrs_by_id.values():
            if dmr.has_sat_indx(sat_indx): return True
        return False

    def has_gs_indx(self,gs_indx):
        """ return true if ground station with gs index is contained within the route in this route container"""
        for dmr in self.dmrs_by_id.values():
            if dmr.has_gs_indx(gs_indx): return True
        return False

    def get_routes(self):
        return list(self.dmrs_by_id.values())

    def get_next_planned_wind(self,rt):

        for dmr in self.get_routes():
            if dmr.contains_route(rt):
                return dmr.get_next_planned_wind(rt)

        # if we're checking for a next planned route, should be sure rt is contained in self first before calling this func. Raise warning here to warn about bad situations otherwise.
        raise RuntimeWarning('did not find a matching planned dmr in self for rt (self: %s, other rt: %s)'%(self,rt))

    def get_dv_epsilon(self):
        """ get DV epsilon that is representative for this route container"""
        #  for now, just grab the DV epsilon of the first route
        return list(self.dmrs_by_id.values())[0].dv_epsilon

    def set_times_safe(self,update_dt):
        """Set the update and creation times, if they have not already been set"""
        if self.update_dt is None:
            self.update_dt = update_dt
        if self.creation_dt is None:
            self.creation_dt = update_dt

    def updated_check(self,dmr,dmr_dv_util):
        """Returns true if self is not updated; that is, utilization for contained datamultiroute has not changed significantly"""

        if not type(dmr) == DataMultiRoute:
            raise NotImplementedError

        # sanity check that the same DMR is actually here
        assert(dmr.ID in self.dmrs_by_id.keys())

        return (abs(self.dv_utilization_by_dmr_id[dmr.ID] - dmr_dv_util) > self.dv_utilization_epsilon)


    #  note: should not be using this function to update route containers ( the objects should be replaced)
    # def update_route(self,update_dr,dr_t_util,dr_dv_util,update_dt):
    #     #  note the implicit check here that the update DR is already present within this object
    #     self.dmrs_by_id[update_dr.ID] = update_dr
    #     self.t_utilization_by_dr_id[update_dr.ID] = dr_t_util
    #     self.dv_utilization_by_dmr_id[update_dr.ID] = dr_dv_util
    #     self.update_dt = update_dt

    def get_winds_executable(self,filter_start_dt=None,filter_end_dt=None,filter_opt='partially_within',sat_indx=None,gs_indx=None):
        """find and set the windows within this route container that are relevant for execution under a set of filters"""

        # stores the filtered windows for execution, with their executable properties set
        #  note: using a list here instead of a set because in general the list should be small and okay for membership checking
        winds_executable = []

        for dmr in self.dmrs_by_id.values():
            winds = dmr.get_winds()

            for wind in winds:
                #  test if this window is relevant for this satellite index
                if (sat_indx is not None) and not wind.has_sat_indx(sat_indx):
                    continue
                #  test if this window is relevant for this ground station index
                if (gs_indx is not None) and not wind.has_gs_indx(gs_indx):
                    continue

                #  also apply any start and end filters if we want to
                if not check_temporal_overlap(wind.start,wind.end,filter_start_dt,filter_end_dt,filter_opt):
                    continue

                #  create a new executable window entry, which specifies both the window and the amount of data volume used from it for this route
                winds_executable.append(
                    ExecutableActivity(
                        wind=wind,
                        # t_utilization=self.t_utilization_by_dr_id[dmr.ID],
                        rt_conts=[self],
                        dv_used=self.dv_utilization_by_dmr_id[dmr.ID]*dmr.data_vol_for_wind(wind),
                    )
                )

        return winds_executable

    def data_vol_for_wind(self,wind):
        """ get the data volume used for a given activity window in this route container"""

        wind_sum = sum(dmr.data_vol_for_wind(wind)*self.dv_utilization_by_dmr_id[dmr.ID] for dmr in self.dmrs_by_id.values())

        # only throw error if this route is actual scheduled for dv
        if wind_sum == 0 and self.data_vol > self.dv_epsilon:
            raise KeyError('Found zero data volume for window, which assumedly means it is not in the route. self: %s, wind: %s'%(self,wind))

        return wind_sum

    def contains_wind(self,wind):
        """ check if this route container contains the given window"""

        for dmr in self.dmrs_by_id.values():
            if wind in dmr.get_winds():
                return True

    def find_matching_data_conts(self,data_conts,match_method='planned'):
        """ find those data containers that match this same route container

        # todo: update this description
        This method is essentially the linchpin in matching schedule intent ( represented by the sim route container) to execution ( represented by the data container). A data container is said to match a route container in two possible ways:
        1. "planned": if its latest planned data route matches at least one of the data routes underlying the Sim route container 
        2. "executed": if its executed route matches at least one of the data routes underlying the Sim route container 
        
        This route matching is on a window by window basis. that is, the data container route has the same observation and same cross-links as a data route within the Sim route container, up to and including the latest cross-link in the data container. This could also include the final downlink, but it's likely not useful to do this matching process after the downlink has occurred. Note that no data volume constraints are set on this match -  only the windows in the routes.

        :param data_conts:  data containers to check for matches
        :type data_conts: iterable
        :returns:  matching data containers
        :rtype: {list(SimDataContainer)}
        """

        # note that the success of this matching process depends on having up-to-date plans ( route containers) and marking those updates for each data container. If plans have not been updated, then you might end up in a situation where there is a route container that expresses routing plans, but no data container can be found to service those plans

        matches = []

        for dc in data_conts:
            # dc_dr = dc.data_route
            if match_method == 'planned':
                dc_route = dc.latest_planned_rt_cont
            elif match_method == 'executed':
                dc_route = dc.executed_data_route
                if not dc_route: raise RuntimeWarning('Could not find an executed route for data container %s'%(dc))
            else:
                raise NotImplementedError

            #  if we can't find a route, then it's not possible to match to this data container
            if not dc_route:
                continue

            #  check each data multi-route to see if it matches the route of the data container
            for dmr in self.dmrs_by_id.values():
                if match_method == 'planned':
                    match_found =  dmr == dc_route
                # in this case, we want to check if the dmr contains the same windows as dc_route
                elif match_method == 'executed':
                    match_found= dmr.contains_route(dc_route)

                if match_found: 
                    matches.append(dc)
                    break

        return matches

    def contains_route(self,route):
        contains = False
        for dmr in self.dmrs_by_id.values():
            if dmr.contains_route(route):
                contains = True

        return contains

    def get_first_dmr_utilization(self):
        return list(self.dv_utilization_by_dmr_id.values())[0]


class ExecutableDataContainer:
    """This object holds a data container alongside the remaining data volume allowed to be routed for it in a given context"""

    def __init__(self,data_cont,remaining_dv):
        # the data container (packet)
        self.data_cont = data_cont
        # the remaining data volume to be used from the data container
        self.remaining_dv = remaining_dv

    @property
    def data_vol(self):
        return self.data_cont.data_vol

    def __repr__(self):
        return "(ExecutableDataContainer, dc: %s, remaining_dv: %f)"%(self.data_cont.ID,self.remaining_dv)
