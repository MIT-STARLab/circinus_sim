
from circinus_tools.scheduling.custom_window import ObsWindow
from circinus_tools.scheduling.routing_objects import DataRoute, RoutingObjectID

class SimDataContainer():
    '''This is essentially an observation data packet. Contains relevant information for tracking of data across sim run'''

    # note this route is simple:  there are no forks in the route; there is a simple linear path from an observation to a downlink through which data flows. all windows must be in temporal order.

    def __init__(self,sat_id,sat_indx,obs_indx,route,dv=0):

        # the list storing all objects in the route; a list ObsWindow, XlnkWindow, XlnkWindow...DlnkWindow
        self.data_vol = dv

        self.ID = RoutingObjectID(sat_id,obs_indx,rt_obj_type='obs_data_pkt')

        if type(route) == ObsWindow:
            obs = route
            route = [obs]
            # this stores the data route taken by the observation data in this container. Note that it is a simple data route, NOT a data multi route
            self.dr = DataRoute(sat_id,obs_indx, route, window_start_sats={obs:sat_indx},dv=0)
        else:
            raise NotImplementedError

    def add_dv(self,delta_dv):
        """Add data to the data container, assuming the same route as currently specified"""
        self.data_vol += delta_dv
        self.dr.add_dv(delta_dv)