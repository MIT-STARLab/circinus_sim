from circinus_tools.scheduling.routing_objects import DataRoute,DataMultiRoute,RoutingObjectID
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

        lp_general_params = lp_params['const_sim_inst_params']['lp_general_params']
        self.dv_epsilon = lp_general_params['dv_epsilon_Mb']
        self.existing_utilization_epsilon = lp_general_params['existing_utilization_epsilon']
        self.inflow_dv_minimum = lp_general_params['inflow_dv_minimum_Mb']
        

    def determine_flows(self,existing_route_data):
        """ figure out which routes and data containers deliver/carry data volume to/from the satellite"""
        #  note:  outflows are routes that carry data volume away from the satellite within the planning. These are routes that we could actually use for routing any observation data. inflows are routes that will deliver data volume to the satellite during the planning period, or data containers that are already present on the satellite and thus constitute data that can be routed

        flow_indx = 0

        outflows = []
        inflows = []

        planned_rts_outflows_in_planning_window = set()

        # utilization_by_flow_id = {}

        # def get_flow_obj(rt,direction,flow_indx):
        #     flobject = PartialFlow(flow_indx, self.sat_indx, rt, direction)
        #     flow_indx += 1
        #     return flobject

        rt_inflows_seen = set()

        #  note that these are of type data multi-route
        for rt in existing_route_data['planned_routes']:

            # just a sanity check that we're working with a DMR
            assert(type(rt) == DataMultiRoute)

            # Notes about filtering below: 
            # - we are using the regular start and end for the windows below, not the original start and end. this is fine for now because the local planner cannot extend the schedule time for window past the already-scheduled-by-the-global planner start/end times, so we don't need to consider the orginal_start/end times.
            # - we are filtering for activity windows that are totally within the planning window, meaning there start and end times are both within the window. this is important for the start of the window, self.planning_leaving_flow_start_dt,  because we don't want to allow the case where a time-bound-overlapping activity is long enough such that it stretches before that start time and maybe even up to the current time on the satellite that is calling the local planner. that would be problematic, because we would be making planning decisions about an activity that is already being executed. so we want to be sure that all activity windows are fully confined to the planning window

            #  determine if any of the activity windows within this route are within the planning window, and this satellite is transmitting data during the window. if yes, it means that this route can carry data volume away from the satellite
            # note: don't use self.planning_leaving_flow_start_dt here - we'll check for that elsewhere
            tx_winds_sat = [ wind for wind in rt.get_winds() if  wind.has_sat_indx(self.sat_indx) and wind.is_tx(self.sat_indx)]
            tx_winds_in_planning_window = [ wind for wind in tx_winds_sat if  
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_start_dt,
                    self.planning_end_dt,
                    filter_opt='totally_within'
                )
            ]

            has_tx_in_planning_window = len(tx_winds_in_planning_window) > 0
            #  this is the amount of outgoing data volume from this route, within the planning window
            # note: utilization is factored in below
            tx_dv_in_planning_window = sum(rt.data_vol_for_wind(wind) for wind in tx_winds_in_planning_window)



            #  determine if any of the activity windows within this route are within the planning window, and this satellite is receiving data during the window. if yes, it means that this route can bring data volume onto the satellite
            rx_winds_sat = [ wind for wind in rt.get_winds() if (wind.has_sat_indx(self.sat_indx) and wind.is_rx(self.sat_indx))]
            rx_winds_in_planning_window = [ wind for wind in rx_winds_sat if  
                check_temporal_overlap(
                    wind.start,
                    wind.end,
                    self.planning_start_dt,
                    self.planning_end_dt,
                    filter_opt='partially_within'
                )
            ]
            has_rx_in_planning_window = len(rx_winds_in_planning_window) > 0
            # note: utilization is factored in below
            rx_dv_in_planning_window = sum(rt.data_vol_for_wind(wind) for wind in rx_winds_in_planning_window)


            #  check that, if the route has multiple tx or rx windows relevant for this satellite, and at least one of them falls within the planning window, then ALL of them are within the planning window (this is, again, to deal with DataMultiRoute objects)
            # in layman's terms: we don't want to allow a DataMultiRoute to be used as an in/outflow in the case where it gets split, because then we wouldn't be actually spreading scheduled DV evenly within the DMR (split: it has multiple DataRoute objects relevant for this sat, but not all of DataRoutes pass through the planning window)
            if has_tx_in_planning_window and not len(tx_winds_in_planning_window) == len(tx_winds_sat):
                raise NotImplementedError(" have not implemented code to deal with the case where a DataMultiRoute has activities for sat_indx both in and out of the LP planning window (sat: %d, rt: %s, winds: %s, planning window: %s)"%(self.sat_indx,rt,tx_winds_sat,[self.planning_leaving_flow_start_dt,self.planning_end_dt]))
            
            if has_rx_in_planning_window and not len(rx_winds_in_planning_window) == len(rx_winds_sat):
                raise NotImplementedError(" have not implemented code to deal with the case where a DataMultiRoute has activities for sat_indx both in and out of the LP planning window (sat: %d, rt: %s, winds: %s, planning window: %s)"%(self.sat_indx,rt,rx_winds_sat,[self.planning_leaving_flow_start_dt,self.planning_end_dt]))


            #  if the route is an outflow, create a partial flow object to encapsulate it and mark the amount of data volume that can flow out
            #  reduce the data volume by the planned utilization of the route. note that we have now divorced ourselves entirely from the original data volume for the route, because we could potentially be splitting the route ( in the case that it's a data multi-route). we will need to do the final bookkeeping for all data volume after running LP scheduling.
            if has_tx_in_planning_window:
                #  add in an epsilon to the utilization number, because it may be that the utilization was precisely chosen to meet the minimum data volume requirement -  don't want to not make the minimum data volume requirement this time because of round off error
                dv = tx_dv_in_planning_window * (existing_route_data['utilization_by_planned_route_id'][rt.ID]+self.existing_utilization_epsilon)

                if dv >= self.inflow_dv_minimum:
                    flobject = PartialFlow(flow_indx, self.sat_indx, rt, dv, tx_winds_in_planning_window,direction='outflow')
                    flow_indx += 1
                    outflows.append(flobject)

                    # keep track of if this route outflows within the planning wind
                    planned_rts_outflows_in_planning_window.add(rt)
                    
                    #This sanity check is to make sure that all of the windows we have found for this route constitute a single outflow direction.  it is possible that a route could pass through satellite, go to other satellites, and then circle back to the original satellite, only to pass on to additional satellites and finally a downlink. in that case it is possible that we would double count some of this dv as outflow ( note: I don't really expect to see this ever)
                    assert(tx_dv_in_planning_window == rt.data_vol)


            if has_rx_in_planning_window: 

                # filter out any data containers that are being transmitted RIGHT NOW. That is, their tx window for the sat begins before planning window. Note that the tx window for the sat in an exected route (from a DataContainer) should never be wholly before the start of the planning window...
                # if len(tx_winds_sat) > 1: raise NotImplementedError
                # for tx_wind in tx_winds_sat:
                #     if tx_wind.start < self.planning_start_dt:
                #         continue

                # # if a route comes inflows during the planning window but doesn't have an outflow in the window, I considered not adding that as an inflow... because that's just pointlessly adding more input data volume that looks to the LP like it needs to be routed. However, we'd need to handle executed routes that don't have a tx as well (below), and I don't want to do that right now. The LP should be robust enough to ignore this case, because it has a mechanism to reward existing routes (that do have both an inflow and outflow)
                # if not has_tx_in_planning_window:
                #     pass
                # else:

                dv = rx_dv_in_planning_window * (existing_route_data['utilization_by_planned_route_id'][rt.ID] +self.existing_utilization_epsilon)

                if dv >= self.inflow_dv_minimum:
                    inflow_injected = rt.ID in existing_route_data['injected_route_ids']
                    flobject = PartialFlow(flow_indx, self.sat_indx, rt, dv, rx_winds_in_planning_window,direction='inflow',injected=inflow_injected)
                    flow_indx += 1
                    inflows.append(flobject)
                    
                    assert(rx_dv_in_planning_window == rt.data_vol)

                    rt_inflows_seen.add(rt)


        #  every one of the executed routes ( from the data containers in the simulation) is considered an inflow, because it's data that is currently on the satellite
        for rt in existing_route_data['executed_routes']:

            tx_winds_sat = [ wind for wind in rt.get_winds() if  wind.has_sat_indx(self.sat_indx) and wind.is_tx(self.sat_indx)]
            # if this route somehow loops through sat multiple times, then well....I haven't covered that case here. raise notimplementederror

            if len(tx_winds_sat) > 1: raise NotImplementedError
            # filter out any data containers that are being transmitted RIGHT NOW - skip them. That is, their tx window for the sat begins before planning window. Note that the tx window for the sat in an exected route (from a DataContainer) should never be wholly before the start of the planning window...
            skip_route = False
            for tx_wind in tx_winds_sat:
                if tx_wind.start < self.planning_start_dt:
                    skip_route = True
            if skip_route:        
                continue


            # If the route was injected (i.e. observation data was collected where it wasn't planned in advance)
            inflow_injected = rt.ID in existing_route_data['injected_route_ids']

            if rt in rt_inflows_seen:
                continue

            dv = rt.data_vol * existing_route_data['utilization_by_executed_route_id'][rt.ID]

            if dv >= self.inflow_dv_minimum:
                flobject = PartialFlow(flow_indx, self.sat_indx, rt, dv, winds_in_planning_window= [],direction='inflow',injected=inflow_injected)
                flow_indx += 1
                inflows.append(flobject)
                rt_inflows_seen.add(rt)

        #     if rt.ID.indx == 0 and self.sat_indx == 5:
        #         debug_tools.debug_breakpt()

        # if self.sat_indx == 3:
        # if self.sat_indx == 3:
        #     debug_tools.debug_breakpt()
                
        return inflows,outflows,planned_rts_outflows_in_planning_window

