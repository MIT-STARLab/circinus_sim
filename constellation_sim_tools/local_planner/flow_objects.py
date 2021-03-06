from datetime import timedelta
from circinus_tools.scheduling.routing_objects  import RoutingObjectID
from circinus_tools.scheduling.routing_objects import DataRoute,DataMultiRoute

class PartialFlow:
    """  encapsulates all the relevant information about a flow arriving on or leaving a satellite"""

    valid_directions = ['inflow','outflow']
    # valid_flow_types = ['route_container','data_container']

    def __init__(self,flow_indx,sat_indx,route,data_vol,winds_in_planning_window,direction,injected=False): #,flow_type='route_container'):
        self.ro_ID = RoutingObjectID(creator_agent_ID='lp',creator_agent_ID_indx=flow_indx,rt_obj_type='partial_flow')

        self.data_vol = data_vol
        self._route = route

        self.sat_indx = sat_indx

        #  stores a string indicating the direction of the flow
        self._direction = direction

        # self._flow_type = flow_type

        #  stores all of those activity windows which were found to be within the planning window on the satellite of interest. so these are the activity windows which are to be used for routing data volume for this flow. note that data volume for this flow should not be calculated from _winds_in_planning_window, but rather be obtained from self.data_vol. the point of storing these windows is for us to know which windows are required to be executed for routing the flow
        # note this may be zero if it's an inflow from a data cont
        self._winds_in_planning_window = winds_in_planning_window


        if not direction in self.valid_directions:
            raise NotImplementedError

        #  indicates whether or not the observation that created this inflow was injected
        self.injected = injected

        # if not flow_type in self.valid_flow_types:
        #     raise NotImplementedError

    @property
    def ID(elf):
        # http://www.chris.com/ascii/joan/www.geocities.com/SoHo/7373/xmas.html#elf
        #              ___,@
        #             /  <
        #        ,_  /    \  _,
        #    ?    \`/______\`/
        # ,_(_).  |; (e  e) ;|
        #  \___ \ \/\   7  /\/    _\8/_
        #      \/\   \'=='/      | /| /|
        #       \ \___)--(_______|//|//|
        #        \___  ()  _____/|/_|/_|
        #           /  ()  \    `----'
        #          /   ()   \
        #         '-.______.-'
        # jgs   _    |_||_|    _
        #      (@____) || (____@)
        #       \______||______/
        return elf.ro_ID

    @property
    def rt_ID(self):
        return self._route.ID

    @property
    def dv(self):
        return self.data_vol

    @property
    def route(self):
        return self._route

    def __repr__(self):
        return "(PartialFlow %s, rt_id: %s, dv: %f, dir: %s)"%(self.ro_ID,self._route.ID,self.data_vol,self._direction)

    def get_simple_drs_dvs(self):
        """ return a list of tuples for every data route within this flow, with each tuple containing a data route and the data volume for that route"""

        #  this may be poor OOP design, but we have to deal with the fact that a data route and a data multi-route apportion data volumes differently...

        #  if it's a single data route then all of the data volume goes to that route
        if type(self._route) == DataRoute:
            return [(self._route,self.data_vol)]

        #  if it's a data multi-route then it contains multiple simple data routes.  we need to figure out how much data volume is allotted to each one of them
        if type(self._route) == DataMultiRoute:
            drs = self._route.simple_data_routes

            #  because we don't want to go about assigning data volume to routes manually, we calculate a fraction
            dv_fraction = self.data_vol/self._route.data_vol

            return [(dr,dr.data_vol*dv_fraction) for dr in drs]



    def flow_precedes(self,other,act_timing_helper):
        """ Check if the data delivered from self (inflow) is available before the other (outflow)."""
        # Note: this avoids flow splitting, by ensuring that all of the windows in self that arrive at the satellite precede all of the windows in other that depart the satellite. if we did not ensure this, we could end up splitting two parallel flows present within a DataMultiRoute

        if self._direction == 'outflow':
            raise RuntimeWarning('trying to check if an outflow precedes another flow, must be an inflow (self: %s, other: %s)'%(self,other))
        if other._direction == 'inflow':
            raise RuntimeWarning('trying to check if an inflow follows another flow, must be an outflow(self: %s, other: %s)'%(self,other))

        #  if the outflow has no windows relevant for the satellite, that's an error ( no outflow should be constructed if the route didn't have any tx windows within the planning window)
        if len(other._winds_in_planning_window) == 0:
            raise RuntimeWarning('Did not find activity windows within the planning window for outflow %s'%(other))

        #  if the inflow has no activity windows within the planning window, that means that the inflow arrived on the satellite before the planning window, and it's valid to say that it proceeds any outflow within the planning window
        if len(self._winds_in_planning_window) == 0:
            return True

        # figure out the latest time that the inflow is delivering data to the satellite.
        # note: functions like an argmax
        latest_arrival_on_sat_wind = max((wind for wind in self._winds_in_planning_window),key= lambda w:w.end)
        # figure out the earliest time that the outflow is carrying data off the satellite.
        earliest_departure_from_sat_wind = min((wind for wind in other._winds_in_planning_window),key= lambda w:w.start)

        if latest_arrival_on_sat_wind.center > earliest_departure_from_sat_wind.center:
            return False

        # include transition time consideration
        preceeds = (latest_arrival_on_sat_wind.end + 
                        timedelta(seconds = act_timing_helper.get_transition_time_req(latest_arrival_on_sat_wind,
                                earliest_departure_from_sat_wind,self.sat_indx,self.sat_indx
                        ))
                        <= earliest_departure_from_sat_wind.start
                    )

        return preceeds

    def get_required_acts(self):
        """ get all of the activity windows that are relevant for scheduling this flow"""
        return (wind for wind in self._winds_in_planning_window)

    def is_inflow(self):
        return self._direction == "inflow"

    def is_outflow(self):
        return self._direction == "outflow"

    def flow_before_time(self,time_dt):
        if not self._direction == "outflow":
            raise NotImplementedError

        earliest_departure= min((wind for wind in self._winds_in_planning_window),key= lambda w:w.start)

        if earliest_departure.start < time_dt:
            return True

        return False


class UnifiedFlow:
    """holds a pairing of an inflow to an outflow"""
    def __init__(self,ID,inflow,outflow):
        self.ID = ID
        self.inflow = inflow
        self.outflow = outflow

    def __repr__(self):
        return "(UnifiedFlow %s, inflow: %s %s, outflow: %s %s)"%(self.ID,self.inflow.ID,self.inflow.rt_ID,self.outflow.ID,self.outflow.rt_ID)

    def get_latency( self,units='minutes',obs_option = 'center', dlnk_option = 'center'):

        #  the regular start and end for these Windows gets changed by the global planner, so on subsequent passes the calculation will be different. want to avoid this
        if obs_option in ['start','end']:
            raise RuntimeWarning('Warning: should not use obs start/end for latency calculation (use original start/end)')
        if dlnk_option in ['start','end']:
            raise RuntimeWarning('Warning: should not use dlnk start/end for latency calculation (use original start/end)')

        obs =  self.inflow.route.get_obs()
        dlnk =  self.outflow.route.get_dlnk()

        return DataRoute.calc_latency(obs,dlnk,units,obs_option,dlnk_option)
