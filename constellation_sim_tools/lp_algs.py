from .schedule_tools  import check_temporal_overlap

from circinus_tools import debug_tools

class LPAlgs:
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

        outflows = []
        inflows = []
        #  note that these are of type data multi-route
        for rt in existing_route_data['planned_routes']:

            #  determine if any of the activity windows within this route are within the planning window, and this satellite is transmitting data during the window. if yes, it means that this route can carry data volume away from the satellite
            # Note: notice that were using the regular start and end for the windows below, not the original start and end. this is fine for now because the local planner cannot extend the schedule time for window past the already-scheduled-by-the-global planner start/end times, so we don't need to consider the orginal_start/end times.
            has_tx_in_planning_window = any(wind for wind in rt.get_winds() if  
                (wind.has_sat_indx(self.sat_indx) and wind.is_tx(self.sat_indx)) and
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_leaving_flow_start_dt,
                    self.planning_end_dt,
                    filter_opt='totally_within'
                )
            )

            #  determine if any of the activity windows within this route are within the planning window, and this satellite is  receiving data during the window. if yes, it means that this route can bring data volume onto the satellite
            has_rx_in_planning_window = any(wind for wind in rt.get_winds() if  
                (wind.has_sat_indx(self.sat_indx) and wind.is_rx(self.sat_indx)) and
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_leaving_flow_start_dt,
                    self.planning_end_dt,
                    filter_opt='totally_within'
                )
            )

            if has_tx_in_planning_window: 
                outflows.append(rt)

            if has_rx_in_planning_window: 
                inflows.append(rt)

        #  every one of the executed routes ( from the data containers in the simulation) is considered an inflow, because it's data that is currently on the satellite
        for rt in existing_route_data['executed_routes']:
            inflows.append(rt)

        debug_tools.debug_breakpt()

        return outflows,inflows