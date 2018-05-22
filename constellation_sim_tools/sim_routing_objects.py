
from circinus_tools.scheduling.custom_window import ObsWindow
from circinus_tools.scheduling.routing_objects import DataRoute, RoutingObjectID

class SimDataContainer():
    '''This is essentially an observation data packet. Contains relevant information for tracking of data across sim run'''

    # note this route is simple:  there are no forks in the route; there is a simple linear path from an observation to a downlink through which data flows. all windows must be in temporal order.

    def __init__(self,sat_id,sat_indx,dc_indx,route,dv=0):

        # the list storing all objects in the route; a list ObsWindow, XlnkWindow, XlnkWindow...DlnkWindow
        self.data_vol = dv

        self.ID = RoutingObjectID(sat_id,dc_indx,rt_obj_type='obs_data_pkt')

        if type(route) == ObsWindow:
            obs = route
            route = [obs]
            # this stores the data route taken by the observation data in this container. Note that it is a simple data route, NOT a data multi route
            self.dr = DataRoute(sat_id,dc_indx, route, window_start_sats={obs:sat_indx},dv=0)
        elif type(route) == DataRoute:
            self.dr = route
        else:
            raise NotImplementedError

        # keeps track of the IDs held by this route, in the course of forking
        self.ID_hist = []

    def add_dv(self,delta_dv):
        """Add data to the data container, assuming the same route as currently specified"""
        self.data_vol += delta_dv
        self.dr.add_dv(delta_dv)

    def add_to_id_hist(self,ID):
        self.ID_hist.append(ID)

    def add_to_route(self,wind,window_start_sat_indx):
        self.dr.append_wind_to_route(wind, window_start_sat_indx)

    def fork(self,new_sat_id,new_dc_indx):

        newone = SimDataContainer(new_sat_id, None, new_dc_indx, copy(self.dr))
        newone.add_to_id_hist(self.ID)