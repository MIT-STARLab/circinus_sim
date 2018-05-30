from circinus_tools.scheduling.routing_objects  import RoutingObjectID

class PartialFlow:
    """  encapsulates all the relevant information about a flow arriving on or leaving a satellite"""

    valid_directions = ['inflow','outflow']

    def __init__(self,flow_indx,sat_indx,route,data_vol,winds_in_planning_window,direction):
        self.ro_ID = RoutingObjectID(creator_agent_ID='lp',creator_agent_ID_indx=flow_indx,rt_obj_type='partial_flow')

        self.data_vol = data_vol
        self._route = route

        self.sat_indx = sat_indx

        #  stores a string indicating the direction of the flow
        self._direction = direction

        #  stores all of those activity windows which were found to be within the planning window on the satellite of interest. so these are the activity windows which are to be used for routing data volume for this flow. note that data volume for this flow should not be calculated from _winds_in_planning_window, but rather be obtained from self.data_vol. the point of storing these windows is for us to know which windows are required to be executed for routing the flow
        self._winds_in_planning_window = winds_in_planning_window

        if not direction in self.valid_directions:
            raise NotImplementedError

        #  there could be multiple windows within the route that arrives at or depart from the satellite of interest, because the route could be a data multi-route, which allows parallel paths through the constellation. cache these windows for now
        self._sat_winds = [wind for wind in route.get_winds() if wind.has_sat_indx(self.sat_indx)]

    @property
    def ID(self):
        return self.ro_ID

    @property
    def dv(self):
        return self.data_vol

    def __repr__(self):
        return "(PartialFlow %s, dv: %f, dir: %s)"%(self.ro_ID,self.data_vol,self.direction)

    def flow_precedes(self,other):
        """ Check if the data delivered from self (inflow) is available before the other (outflow) """

        if self.direction == 'outflow':
            raise RuntimeWarning('trying to check if an outflow precedes another flow, must be an inflow (self: %s, other: %s)'%(self,other))
        if other.direction == 'inflow':
            raise RuntimeWarning('trying to check if an inflow follows another flow, must be an outflow(self: %s, other: %s)'%(self,other))

        # figure out the latest time that the inflow is delivering data to the satellite.
        latest_arrival_on_sat = max(wind.end for wind in self._sat_winds)
        # figure out the earliest time that the outflow is carrying data off the satellite.
        earliest_departure_from_sat = min(wind.start for wind in other._sat_winds)

        # todo:  handle any necessary transition time constraints here
        return latest_arrival_on_sat <= earliest_departure_from_sat

    def get_required_acts(self):
        """ get all of the activity windows that are relevant for scheduling this flow"""
        return wind for wind in self._winds_in_planning_window

    def is_inflow(self):
        return self._direction == "inflow"

    def is_outflow(self):
        return self._direction == "outflow"
