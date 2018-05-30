from constellation_sim_tools.schedule_tools  import check_temporal_overlap
from .flow_objects import PartialFlow

from circinus_tools import debug_tools

class LPProcessing:
    def __init__(self, lp_params):
        """initializes based on parameters

        initializes based on parameters
        :param lp_params: params (Note: left intentionally vague so that it will be super confusing later) ( note 2: look at global planner)
        :type params: dict
        """

        sat_params = lp_params['orbit_prop_params']['sat_params']

        lp_inst_planning_params = lp_params['lp_instance_params']['planning_params']

        self.sat_indx = lp_params['lp_instance_params']['sat_indx']
        self.sat_id = lp_params['lp_instance_params']['sat_id']
        #  the earliest time for which we consider incoming flows
        self.planning_start_dt  = lp_inst_planning_params['planning_start_dt']
        #  the earliest time at which we allow a leaving flow to start
        self.planning_leaving_flow_start_dt  = lp_inst_planning_params['planning_leaving_flow_start_dt']
        #  the latest time for which we consider an incoming or leaving flow
        self.planning_end_dt  = lp_inst_planning_params['planning_end_dt']
        

    def determine_flows(self,existing_route_data):
        """ figure out which routes and data containers deliver/carry data volume to/from the satellite"""
        #  note:  outflows are routes that carry data volume away from the satellite within the planning. These are routes that we could actually use for routing any observation data. inflows are routes that will deliver data volume to the satellite during the planning period, or data containers that are already present on the satellite and thus constitute data that can be routed

        flow_indx = 0

        outflows = []
        inflows = []

        # utilization_by_flow_id = {}

        # def get_flow_obj(rt,direction,flow_indx):
        #     flobject = PartialFlow(flow_indx, self.sat_indx, rt, direction)
        #     flow_indx += 1
        #     return flobject


        #  note that these are of type data multi-route
        for rt in existing_route_data['planned_routes']:
            # Notes about filtering below: 
            # - we are using the regular start and end for the windows below, not the original start and end. this is fine for now because the local planner cannot extend the schedule time for window past the already-scheduled-by-the-global planner start/end times, so we don't need to consider the orginal_start/end times.
            # - we are filtering for activity windows that are totally within the planning window, meaning there start and end times are both within the window. this is important for the start of the window, self.planning_leaving_flow_start_dt,  because we don't want to allow the case where a time-bound-overlapping activity is long enough such that it stretches before that start time and maybe even up to the current time on the satellite that is calling the local planner. that would be problematic, because we would be making planning decisions about an activity that is already being executed. so we want to be sure that all activity windows are fully confined to the planning window

            #  determine if any of the activity windows within this route are within the planning window, and this satellite is transmitting data during the window. if yes, it means that this route can carry data volume away from the satellite
            tx_winds_in_planning_window = [ wind for wind in rt.get_winds() if  
                (wind.has_sat_indx(self.sat_indx) and wind.is_tx(self.sat_indx)) and
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_leaving_flow_start_dt,
                    self.planning_end_dt,
                    filter_opt='totally_within'
                )
            ]
            has_tx_in_planning_window = len(tx_winds_in_planning_window) > 0
            #  this is the amount of outgoing data volume from this route, within the planning window
            tx_dv_in_planning_window = sum(wind.data_vol for wind in tx_winds_in_planning_window)

            #  determine if any of the activity windows within this route are within the planning window, and this satellite is receiving data during the window. if yes, it means that this route can bring data volume onto the satellite
            rx_winds_in_planning_window = [ wind for wind in rt.get_winds() if  
                (wind.has_sat_indx(self.sat_indx) and wind.is_rx(self.sat_indx)) and
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_leaving_flow_start_dt,
                    self.planning_end_dt,
                    filter_opt='totally_within'
                )
            ]
            has_rx_in_planning_window = len(rx_winds_in_planning_window) > 0
            #  this is the amount of incoming data volume for this route, within the planning window
            rx_dv_in_planning_window = sum(wind.data_vol for wind in rx_winds_in_planning_window)

            debug_tools.debug_breakpt()

            #  if the route is an outflow, create a partial flow object to encapsulate it and mark the amount of data volume that can flow out
            #  reduce the data volume by the planned utilization of the route. note that we have now divorced ourselves entirely from the original data volume for the route, because we could potentially be splitting the route ( in the case that it's a data multi-route). we will need to do the final bookkeeping for all data volume after running LP scheduling.
            if has_tx_in_planning_window:
                dv = tx_dv_in_planning_window * existing_route_data['utilization_by_planned_route_id'][rt.ID] 
                flobject = PartialFlow(flow_indx, self.sat_indx, rt, dv, tx_winds_in_planning_window,direction='outflow')
                flow_indx += 1
                outflows.append(flobject)

            if has_rx_in_planning_window: 
                dv = rx_dv_in_planning_window * existing_route_data['utilization_by_planned_route_id'][rt.ID] 
                flobject = PartialFlow(flow_indx, self.sat_indx, rt, dv, rx_winds_in_planning_window,direction='inflow')
                flow_indx += 1
                inflows.append(flobject)

        #  every one of the executed routes ( from the data containers in the simulation) is considered an inflow, because it's data that is currently on the satellite
        for rt in existing_route_data['executed_routes']:
            #  utilization for executed route is by definition 100%
            flobject = PartialFlow(flow_indx, self.sat_indx, rt, rt.data_vol, winds_in_planning_window= [],direction='inflow')
            flow_indx += 1
            inflows.append(flobject)

        return inflows,outflows #,utilization_by_flow_id

