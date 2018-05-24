from copy import copy

from circinus_tools.scheduling.custom_window import ObsWindow
from circinus_tools.scheduling.routing_objects import DataRoute, RoutingObjectID

class SimDataContainer:
    '''This is essentially an observation data packet. Contains relevant information for tracking of data across sim run'''

    # note this route is simple:  there are no forks in the route; there is a simple linear path from an observation to a downlink through which data flows. all windows must be in temporal order.

    def __init__(self,sat_id,sat_indx,dc_indx,route,dv=0):

        self.ID = RoutingObjectID(sat_id,dc_indx,rt_obj_type='obs_data_pkt')

        if type(route) == ObsWindow:
            obs = route
            route = [obs]
            # this stores the data route taken by the observation data in this container. Note that it is a simple data route, NOT a data multi route
            self.dr = DataRoute(sat_id,dc_indx, route, window_start_sats={obs:sat_indx},dv=dv)
        elif type(route) == DataRoute:
            self.dr = route
        else:
            raise NotImplementedError

        # keeps track of the IDs held by this route, in the course of forking
        self.ID_hist = []

    @property
    def data_vol(self):
        return self.dr.data_vol

    @property
    def data_route(self):
        return self.dr

    def __repr__(self):
        return "(SimDataContainer %s: dv %f,<%s>)"%(self.ID,self.dr.data_vol,self.dr.get_route_string())

    def add_dv(self,delta_dv):
        """Add data to the data container, assuming the same route as currently specified"""
        self.dr.add_dv(delta_dv)

    def remove_dv(self,delta_dv):
        """Remove data from the data container, assuming the same route as currently specified"""
        self.dr.remove_dv(delta_dv)

    def add_to_id_hist(self,ID):
        self.ID_hist.append(ID)

    def add_to_route(self,wind,window_start_sat_indx):
        self.dr.append_wind_to_route(wind, window_start_sat_indx)

    def fork(self,new_sat_id,new_dc_indx,dv=0):
        newone = SimDataContainer(new_sat_id, None, new_dc_indx, copy(self.dr))
        newone.dr.data_vol = dv
        newone.add_to_id_hist(self.ID)
        return newone

class ExecutableDataContainer:
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

    