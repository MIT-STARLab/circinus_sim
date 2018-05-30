from collections import namedtuple

from pyomo import environ  as pe

from circinus_tools.scheduling.formulation.schedulers import PyomoMILPScheduling

from circinus_tools import debug_tools

def print_verbose(string,verbose=False):
    if verbose:
        print(string)

class LPScheduling(PyomoMILPScheduling):
    """ local planner scheduling algorithm/solver, using pyomo"""
    
    UnifiedFlow = namedtuple('UnifiedFlow', 'ID inflow_ro_id outflow_ro_id')

    def __init__(self,lp_params):
        """initializes based on parameters
        
        initializes based on parameters
        :param lp_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        super().__init__()

        gp_as_params = lp_params['gp_general_params']['activity_scheduling_params']

        # this is the minimum obs dv that must be downlinked by a unified route in order for it to count it towards objective terms (other than total dv)
        self.min_obs_dv_dlnk_req =gp_as_params['min_obs_dv_dlnk_req_Mb']


    def get_flow_structs(self,inflows,outflows):

        all_flows = inflows + outflows

        unified_flows = []
        unified_flow_indx = 0
        #  how much data volume each flow can deliver/carry. note that these capacity numbers already factor in the utilizations of the data routes
        capacity_by_partial_flow_id = {}
        possible_unified_flows_ids_by_inflow_id = {}
        possible_unified_flows_ids_by_outflow_id = {}

        all_acts_windids = set()

        for inflow in inflows:
            capacity_by_partial_flow_id[inflow.ID] = inflow.data_vol
            possible_unified_flows_ids_by_inflow_id[inflow.ID] = []

            for outflow in outflows:
                capacity_by_partial_flow_id[outflow.ID] = outflow.data_vol
                possible_unified_flows_ids_by_outflow_id[outflow.ID] = []

                #  if the data delivered from this inflow is available before the outflow, then they can be combined into a unified flow
                if inflow.flow_precedes(outflow):
                    unified_flows.append(self.UnifiedFlow(ID=unified_flow_indx,inflow_ro_id=inflow.ID, outflow_ro_id=outflow.ID))

                    possible_unified_flows_ids_by_inflow_id[inflow.ID].append(unified_flow_indx)
                    possible_unified_flows_ids_by_outflow_id[outflow.ID].append(unified_flow_indx)
                    unified_flow_indx += 1

        capacity_by_act_windid = {}
        inflows_ids_by_act_windid = {}
        outflows_ids_by_act_windid = {}
        for flow in all_flows:
            for act in flow.get_required_acts():
                act_windid = act.window_ID
                all_acts_windids.add(act_windid)

                #  note that we're using the original activity capacity/available data volume here, rather than a capcity number constrained by existing routes.  this is because the LP can decide, if it wants to, to extend the length of an activity to use all of its potential data volume. Currently though the actual data routes, which make use of that potential data volume, are constrained by existing utilization values ( so we can lengthen an activity to get more data volume, but we can't do anything with it because we can't enlarge the data routes that would use that data volume - in the current version of the LP)
                capacity_by_act_windid[act_windid] = act.data_vol

                if flow.is_inflow():
                    inflows_ids_by_act_windid.setdefault(act_windid, []).append(flow.ID)
                elif flow.is_outflow():
                    outflows_ids_by_act_windid.setdefault(act_windid, []).append(flow.ID)



        return unified_flows,capacity_by_partial_flow_id,possible_unified_flows_ids_by_inflow_id,possible_unified_flows_ids_by_outflow_id,all_acts_windids,capacity_by_act_windid,inflows_ids_by_act_windid,outflows_ids_by_act_windid



    def make_model ( self,inflows,outflows, verbose = False):
        # all the items in outflows,inflows are PartialFlow objects
        # note: all flows in inflows,outflows 

        # important assumption: all activity window IDs are unique!

        model = pe.ConcreteModel()
        self.model = model

        ##############################
        #  Make indices/ subscripts
        ##############################

        (unified_flows,
            capacity_by_partial_flow_id,
            possible_unified_flows_ids_by_inflow_id,
            possible_unified_flows_ids_by_outflow_id,
            all_acts_windids,
            capacity_by_act_windid,
            inflows_ids_by_act_windid,
            outflows_ids_by_act_windid,
            ) =  self.get_flow_structs(inflows,outflows)

        #     latency_sf_by_dmr_id =  self.get_dmr_latency_score_factors(
        #         routes_by_dmr_id,
        #         dmr_ids_by_obs_act_windid
        #     )

        #     # verify that all acts found are within the planning window, otherwise we may end up with strange results
        #     for sat_acts in sats_mutable_acts:
        #         for act in sat_acts:
        #             if act.original_start < self.planning_start_dt or act.original_end > self.planning_end_dt:
        #                 raise RuntimeWarning('Activity is out of planning window range (start %s, end %s): %s'%(self.planning_start_dt,self.planning_end_dt,act))

        #     # construct a set of dance cards for every satellite, 
        #     # each of which keeps track of all of the activities of satellite 
        #     # can possibly execute at any given time slice delta T. 
        #     # this is for constructing energy storage constraints
        #     # using resource_delta_t_s because this dancecard is solely for use in constructing resource constraints
        #     # note that these dancecards will baloon in size pretty quickly as the planning_end_dt increases. However most of the complexity they introduce is before planning_end_obs,xlnk_dt because that's the horizon where obs,xlnk actitivities are included. After that there should only be sparse downlinks
        #     es_act_dancecards = [Dancecard(self.planning_start_dt,self.planning_end_dt,self.resource_delta_t_s,item_init=None,mode='timestep') for sat_indx in range (self.num_sats)]
            
        #     #  add windows to dance card, silenty dropping any time steps that appear outside of the planning window bounds ( we don't need to enforce resource constraints on out-of-bounds activities)
        #     def wind_time_getter_orig(wind,time_opt):
        #         if time_opt == 'start': return wind.original_start
        #         if time_opt == 'end': return wind.original_end
        #     def wind_time_getter_reg(wind,time_opt):
        #         if time_opt == 'start': return wind.start
        #         if time_opt == 'end': return wind.end

        #     for sat_indx in range (self.num_sats): 
        #         es_act_dancecards[sat_indx].add_winds_to_dancecard(sats_mutable_acts[sat_indx],wind_time_getter_orig,drop_out_of_bounds=True)
        #         es_act_dancecards[sat_indx].add_winds_to_dancecard(ecl_winds[sat_indx],wind_time_getter_reg,drop_out_of_bounds=True)

        #     # this is for data storage
        #     # for each sat/timepoint, we store a list of all those data multi routes that are storing data on the sat at that timepoint
        #     ds_route_dancecards = [Dancecard(self.planning_start_dt,self.planning_end_dt,self.resource_delta_t_s,item_init=None,mode='timepoint') for sat_indx in range (self.num_sats)]
            
        #     # add data routes to the dancecard
        #     for dmr in routes_filt:
        #         # list of type routing_objects.SatStorageInterval
        #         dmr_ds_intervs = dmr.get_data_storage_intervals()

        #         for interv in dmr_ds_intervs:
        #             # store the dmr object at this timepoint, silenty dropping any time points that appear outside of the planning window bounds ( we don't need to enforce resource constraints on out-of-bounds intervals)
        #             ds_route_dancecards[interv.sat_indx].add_item_in_interval(dmr.ID,interv.start,interv.end,drop_out_of_bounds=True)

        # except IndexError:
        #     raise RuntimeWarning('sat_indx out of range. Are you sure all of your input files are consistent? (including pickles)')        
        # self.dmr_ids_by_act_windid = dmr_ids_by_act_windid
        # self.all_acts_windids = all_acts_windids
        # self.obs_windids = all_obs_windids
        # self.lnk_windids = all_lnk_windids
        # self.all_acts_by_windid = all_acts_by_windid
        # self.mutable_acts_windids = mutable_acts_windids

        # # these dmr subscripts probably should've been done using the unique IDs for the objects, rather than their arbitrary locations within a list. Live and learn, hélas...

        # model.inflow_ids = pe.Set(initialize= [flow.ro_ID for flow in inflows])
        # model.outflow_ids = pe.Set(initialize= [flow.ro_ID for flow in outflows])
        inflow_ids = [flow.ro_ID for flow in inflows]
        outflow_ids = [flow.ro_ID for flow in outflows]


        #  subscript for each activity a
        model.all_act_windids = pe.Set(initialize= all_acts_windids)

        # Make an index in the model for every inflow and outflow
        all_partial_flow_ids = [flow.ro_ID for flow in inflows] + [flow.ro_ID for flow in outflows]
        model.partial_flow_ids = pe.Set(initialize= all_partial_flow_ids)

        model.unified_flow_ids = pe.Set(initialize= [flow.ID for flow in unified_flows])

        # # timepoints is the indices, which starts at 0 
        # #  NOTE: we assume the same time system for every satellite
        # self.es_time_getter_dc = es_act_dancecards[0]
        # es_num_timepoints = es_act_dancecards[0].num_timepoints
        # model.es_timepoint_indcs = pe.Set(initialize=  self.es_time_getter_dc.get_tp_indcs())

        # self.ds_time_getter_dc = ds_route_dancecards[0]
        # model.ds_timepoint_indcs = pe.Set(initialize=  self.ds_time_getter_dc.get_tp_indcs())

        # #  unique indices for observation and link acts
        # model.obs_windids = pe.Set(initialize= all_obs_windids)
        # model.lnk_windids = pe.Set(initialize= all_lnk_windids)

        # if self.solver_name == 'gurobi' or self.solver_name == 'cplex':
        #     int_feas_tol = self.solver_params['integer_feasibility_tolerance']
        # elif self.solver_name == 'glpk':
        #     # raise an error, because it could be misleading if someone changes the int feas tol in the inputs...
        #     raise NotImplementedError('glpk runs, but I have not yet figured out setting integer_feasibility_tolerance')
        # else:
        #     raise NotImplementedError

        # for p,lat_sf in latency_sf_by_dmr_id.items():        
        #     if lat_sf > int_feas_tol*self.big_M_lat:
        #         raise RuntimeWarning('big_M_lat (%f) is not large enough for latency score factor %f and integer feasibility tolerance %f (dmr index %d)'%(self.big_M_lat,lat_sf,int_feas_tol,p))

        # # for act_obj in all_act_windids_by_obj.keys():
        # #     if 2*(act_obj.end-act_obj.start).total_seconds() > self.big_M_act_t_dur_s:
        # #         raise RuntimeWarning('big_M_act_t_dur_s (%f) is not large enough for act of duration %s and integer feasibility tolerance %f (act string %s)'%(self.big_M_act_t_dur_s,act_obj.end-act_obj.start,int_feas_tol,act_obj))
        # #     # if 2*(act_obj.end-act_obj.start).total_seconds() > (1-int_feas_tol) *  self.big_M_act_t_dur_s:
        # #         # raise RuntimeWarning('big_M_act_t_dur_s (%f) is not large enough for act of duration %s and integer feasibility tolerance %f (act string %s)'%(self.big_M_act_t_dur_s,act_obj.end-act_obj.start,int_feas_tol,act_obj))

        # #     if 2*act_obj.data_vol > (1-int_feas_tol) * self.big_M_act_dv:
        # #         raise RuntimeWarning('big_M_act_dv (%f) is not large enough for act of dv %f and integer feasibility tolerance %f (act string %s)'%(self.big_M_act_dv,act_obj.data_vol,int_feas_tol,act_obj))
                


        ##############################
        #  Make parameters
        ##############################

        model.par_min_obs_dv_dlnk_req = pe.Param (initialize=self.min_obs_dv_dlnk_req)

        # The data volume available for each partial flow ( possible arriving data volume for inflows, and possible leaving data volume for outflows)
        model.par_partial_flow_capacity = pe.Param(model.partial_flow_ids,initialize =capacity_by_partial_flow_id)

        # model.par_total_obs_dv = sum(dv_by_obs_act_windid.values())
        # model.par_link_capacity = pe.Param(model.lnk_windids,initialize =dv_by_link_act_windid)
        model.par_act_capacity = pe.Param(model.all_act_windids,initialize =capacity_by_act_windid)
        # #  data volume for each data multi-route
        # model.par_dmr_dv = pe.Param(model.dmr_ids,initialize ={ dmr.ID: dmr.data_vol for dmr in routes_filt})
        # #  data volume for each activity in each data multi-route

        # #  stored data volume for each activity used by each data route. only store this for the activity if the activity was found to be within the planning window filter (hence, the act in self.all_act_windids_by_obj.keys() check)
        # model.par_dmr_act_dv = pe.Param(
        #     model.dmr_ids,
        #     model.all_act_windids,
        #     initialize = { (dmr.ID,act.window_ID): 
        #         dmr.data_vol_for_wind(act) for dmr in routes_filt for act in dmr.get_winds() if act.window_ID in mutable_acts_windids 
        #     }
        # )

        # # each of these is essentially a dictionary indexed by link or obs act indx, with  the value being a list of dmr indices that are included within that act
        # # these are valid indices into model.dmr_ids
        # # model.par_dmr_subscrs_by_link_act = pe.Param(model.lnk_windids,initialize =dmr_ids_by_link_act_windid)
        # model.par_dmr_subscrs_by_obs_act = pe.Param(model.obs_windids,initialize =dmr_ids_by_obs_act_windid)
        # model.par_dmr_subscrs_by_act = pe.Param(model.all_act_windids,initialize =dmr_ids_by_act_windid)


        # if self.energy_unit == "Wh":
        #     model.par_resource_delta_t = pe.Param (initialize= self.resource_delta_t_s/3600)
        # else: 
        #     raise NotImplementedError
        # model.par_sats_estore_initial = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_init_estate_Wh)})
        # model.par_sats_estore_min = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_emin_Wh)})
        # model.par_sats_estore_max = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_emax_Wh)})
        # model.par_sats_edot_by_mode = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_edot_by_mode_W)})

        # model.par_sats_dstore_min = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_dmin_Mb)})
        # model.par_sats_dstore_max = pe.Param ( model.sat_indcs,initialize= { i: item for i,item in enumerate (self.sats_dmax_Mb)})

        ##############################
        #  Make variables
        ##############################


        # activity utilization variable indicating how much of an activity's capacity is used
        model.var_activity_utilization  = pe.Var (model.all_act_windids, bounds =(0,1))
        model.var_act_indic  = pe.Var (model.all_act_windids, within = pe.Binary)

        # dmr utilization variable indicating how much of a dmr's capacity is used [2]
        # model.var_dmr_utilization  = pe.Var (model.dmr_ids, bounds =(0,1))

        #  utilization for each in/out flow (x_i,x_o)
        model.var_partial_flow_utilization  = pe.Var (model.partial_flow_ids, bounds =(0,1))

        #  indicator variables for whether or not dmrs [3] and activities [4] have been chosen
        # model.var_dmr_indic  = pe.Var (model.dmr_ids, within = pe.Binary)

        # data volume used for a given incoming/outgoing flow pair combination (v_(i,o))
        model.var_unified_flow_dv  = pe.Var (model.unified_flow_ids, within = pe.Reals)
        #  indicates if a given incoming/outgoing flow pair combination is chosen (I_(i,o))
        model.var_unified_flow_indic  = pe.Var (model.unified_flow_ids, within = pe.Binary)


        # a utilization number for existing routes that will be bounded by the input existing route utilization (can't get more  "existing route" reward for a route than the route's previous utilization) [8]
        # model.var_existing_dmr_utilization_reward  = pe.Var (model.existing_dmr_ids, bounds =(0,1))
        
        # # satellite energy storage
        # model.var_sats_estore  = pe.Var (model.sat_indcs,  model.es_timepoint_indcs,  within = pe.NonNegativeReals)

        # # satellite data storage (data buffers)
        # model.var_sats_dstore  = pe.Var (model.sat_indcs,  model.ds_timepoint_indcs,  within = pe.NonNegativeReals)

        # model.var_latency_sf_obs = pe.Var (model.obs_windids,  bounds = (0,1.0))
        
        # allow_act_timing_constr_violations = False
        # if allow_act_timing_constr_violations:
        #     print('allow_act_timing_constr_violations is True')

        # #  variables for handling the allowance of inter-activity timing constraint violations. these are only generated if allow_act_timing_constr_violations is True
        # model.var_intra_sat_act_constr_violations = pe.VarList()
        # model.var_inter_sat_act_constr_violations = pe.VarList()
        # model.intra_sat_act_constr_bounds  = pe.ConstraintList()
        # model.inter_sat_act_constr_bounds  = pe.ConstraintList()

        # #  stores all of the lower bounds of the constraint violation variables, for use in normalization for objective function
        # min_var_intra_sat_act_constr_violation_list = [] 
        # min_var_inter_sat_act_constr_violation_list = [] 

        # constraint_violation_model_objs = {}
        # constraint_violation_model_objs['intra_sat_act_constr_violation_acts_list'] = []
        # constraint_violation_model_objs['inter_sat_act_constr_violation_acts_list'] = []
        # constraint_violation_model_objs['var_intra_sat_act_constr_violations'] = model.var_intra_sat_act_constr_violations
        # constraint_violation_model_objs['var_inter_sat_act_constr_violations'] = model.var_inter_sat_act_constr_violations
        # constraint_violation_model_objs['intra_sat_act_constr_bounds'] = model.intra_sat_act_constr_bounds
        # constraint_violation_model_objs['inter_sat_act_constr_bounds'] = model.inter_sat_act_constr_bounds
        # constraint_violation_model_objs['min_var_intra_sat_act_constr_violation_list'] = min_var_intra_sat_act_constr_violation_list 
        # constraint_violation_model_objs['min_var_inter_sat_act_constr_violation_list'] = min_var_inter_sat_act_constr_violation_list 

        ##############################
        #  Make constraints
        ##############################

        print_verbose('make constraints',verbose)

        # TODO: renumber  these with the final numbering

        def c1_rule( model,i):
            return (sum(model.var_unified_flow_dv[u] for u in possible_unified_flows_ids_by_inflow_id[i]) 
                    <= model.par_partial_flow_capacity[i] * model.var_partial_flow_utilization[i])
        model.c1 =pe.Constraint ( inflow_ids,  rule=c1_rule)

        def c2_rule( model,o):
            return (sum(model.var_unified_flow_dv[u] for u in possible_unified_flows_ids_by_outflow_id[o]) 
                    <= model.par_partial_flow_capacity[o] * model.var_partial_flow_utilization[o])
        model.c2 =pe.Constraint ( outflow_ids,  rule=c2_rule)

        def c3_rule( model,u):
            return model.var_unified_flow_dv[u] >= model.par_min_obs_dv_dlnk_req*model.var_unified_flow_indic[u]
        model.c3 =pe.Constraint ( model.unified_flow_ids,  rule=c3_rule)

        debug_tools.debug_breakpt()

        def c4_rule( model,a):
            return model.var_act_indic[a] >=  model.var_activity_utilization[a]
        model.c4 =pe.Constraint ( model.all_act_windids,  rule=c4_rule)  

        def c5a_rule( model,a):
            return model.par_act_capacity[a] * model.var_activity_utilization[a] - sum(model.par_partial_flow_capacity[i] * model.var_partial_flow_utilization[i] for i in inflows_ids_by_act_windid[a]) >= 0 
        model.c5a =pe.Constraint ( model.all_act_windids,  rule=c5a_rule)  

        def c5b_rule( model,a):
            return model.par_act_capacity[a] * model.var_activity_utilization[a] - sum(model.par_partial_flow_capacity[o] * model.var_partial_flow_utilization[o] for o in outflows_ids_by_act_windid[a]) >= 0 
        model.c5b =pe.Constraint ( model.all_act_windids,  rule=c5b_rule)  

        # print_verbose('make overlap constraints',verbose)

        # #  intra-satellite activity overlap constraints [4],[5],[5b]
        # #  well, 5B is activity minimum time duration
        # model.c4_5  = pe.ConstraintList() # this now contains all of the activity overlap constraints
        # model.c5b  = pe.ConstraintList()
        # # pass the model objects getter function so it can be called in place
        # (self.c5b_binding_exprs_by_act,
        #     self.c4_5_binding_exprs_by_act) =  self.gen_intra_sat_act_overlap_constraints(
        #         model.c4_5,
        #         model.c5b,
        #         sats_mutable_acts,
        #         self.get_act_model_objs,
        #         constraint_violation_model_objs
        #     )

        # # inter-satellite downlink overlap constraints [9],[10]
        # model.c9_10  = pe.ConstraintList()
        # self.c9_10_binding_exprs_by_act = self.gen_inter_sat_act_overlap_constraints(
        #     model.c9_10,
        #     sats_mutable_dlnks,
        #     self.get_act_model_objs,
        #     constraint_violation_model_objs
        # )

        # print_verbose('make energy, data constraints',verbose)

        # #  energy constraints [6]
        # # todo: maybe this ought to be moved to the super class, but o don't anticipate this code changing much any time soon, so i'll punt that.
        # model.c6  = pe.ConstraintList()
        # for sat_indx in range (self.num_sats): 

        #     # tp_indx serves as an index into the satellite activity dance cards
        #     for tp_indx in model.es_timepoint_indcs:
        #         #  constraining first time step to initial energy storage
        #         #  continue for loop afterwards because no geq/leq constraints needed for this index
        #         if tp_indx == 0:
        #             model.c6.add( model.var_sats_estore[sat_indx,tp_indx] ==  model.par_sats_estore_initial[sat_indx])
        #             continue 

        #         #  minimum and maximum storage constraints
        #         model.c6.add( model.var_sats_estore[sat_indx,tp_indx] >= model.par_sats_estore_min[sat_indx])
        #         model.c6.add( model.var_sats_estore[sat_indx,tp_indx] <= model.par_sats_estore_max[sat_indx])

        #         if self.enforce_energy_storage_constr:
        #             # determine activity energy consumption
        #             charging = True
        #             activity_delta_e = 0 
        #             #  get the activities that were active during the time step immediately preceding time point
        #             activities = es_act_dancecards[sat_indx].get_objects_at_ts_pre_tp_indx(tp_indx)
        #             # activities may be none if nothing is happening at timestep, to minimize RAM usage
        #             if activities:
        #                 for act in activities:
        #                     #  if this is a "standard activity" that we can choose to perform or not
        #                     if type(act) in self.standard_activities:
        #                         act_code = act.get_code(sat_indx)
        #                         activity_delta_e += (
        #                             model.par_sats_edot_by_mode[sat_indx][act_code] 
        #                             * model.var_activity_utilization[act.window_ID]
        #                             * model.par_resource_delta_t
        #                         )

        #                     #  if the satellite is not in sunlight then we can't charge
        #                     elif type(act) == EclipseWindow:
        #                         charging = False

        #             # add in charging energy contribution ( if possible)
        #             # assume charging is constant in sunlight
        #             charging_delta_e = model.par_sats_edot_by_mode[sat_indx]['orbit_insunlight_average_charging']*model.par_resource_delta_t if charging else 0

        #             #  base-level satellite energy usage (not including additional activities)
        #             base_delta_e = model.par_sats_edot_by_mode[sat_indx]['base']*model.par_resource_delta_t

        #             # maximum bound of energy at current time step based on last time step
        #             model.c6.add( model.var_sats_estore[sat_indx,tp_indx] <= 
        #                 model.var_sats_estore[sat_indx,tp_indx-1]
        #                 + activity_delta_e
        #                 + charging_delta_e
        #                 + base_delta_e
        #             )

        #             # minimum bound of energy at current time step based on last time step
        #             model.c6.add( model.var_sats_estore[sat_indx,tp_indx] >= 
        #                 model.var_sats_estore[sat_indx,tp_indx-1]
        #                 + activity_delta_e
        #                 + base_delta_e
        #             )


        # #  data storage constraints [7]
        # model.c7  = pe.ConstraintList()
        # for sat_indx in range (self.num_sats): 

        #     # tp_indx serves as an index into the satellite data storage dance cards
        #     for tp_indx in model.ds_timepoint_indcs:

        #         # todo: add in an intital data volume value?
        #         #  maximum storage constraints
        #         model.c7.add( model.var_sats_dstore[sat_indx,tp_indx] <= model.par_sats_dstore_max[sat_indx])

        #         if self.enforce_data_storage_constr:
        #             routes_storing = ds_route_dancecards[sat_indx][tp_indx]

        #             # todo: this may be too slow to use == below. Change it back to >= and use a better approach to extract real data storage values in extract_resource_usage below? Leaving == for now because it means I can use these vars to extract output data usage values
        #             # constrain the minimum data storage at this tp by the amount of data volume being buffered by each route passing through the sat
        #             if routes_storing:
        #                 model.c7.add( model.var_sats_dstore[sat_indx,tp_indx] == sum(model.par_dmr_dv[p]*model.var_dmr_utilization[p] for p in routes_storing))
        #             else:
        #                 model.c7.add( model.var_sats_dstore[sat_indx,tp_indx] == 0)



        # #  observation latency score factor constraints [8]
        # model.c8  = pe.ConstraintList()
        # #  note this is for all observation window IDs, not just mutable ones
        # for o in model.obs_windids:
        #     dmrs_obs = model.par_dmr_subscrs_by_obs_act[o]

        #     #  sort the latency score factors for all the dmrs for this observation in increasing order -  important for constraint construction
        #     dmrs_obs.sort(key= lambda p: latency_sf_by_dmr_id[p])

        #     num_dmrs_obs = len(dmrs_obs)
        #     #  initial constraint -  score factor for this observation will be equal to zero if no dmrs for this obs were chosen
        #     model.c8.add( model.var_latency_sf_obs[o] <= 0 + self.big_M_lat * sum(model.var_dmr_indic[p] for p in dmrs_obs) )
            
        #     for dmr_obs_indx in range(num_dmrs_obs):
        #         #  add constraint that score factor for observation is less than or equal to the score factor for this dmr_obs_indx, plus any big M terms for any dmrs with larger score factors.
        #         #  what this does is effectively disable the constraint for the score factor for this dmr_obs_indx if any higher score factor dmrs were chosen 
        #         model.c8.add( model.var_latency_sf_obs[o] <= 
        #             latency_sf_by_dmr_id[dmrs_obs[dmr_obs_indx]] + 
        #             self.big_M_lat * sum(model.var_dmr_indic[p] for p in dmrs_obs[dmr_obs_indx+1:num_dmrs_obs]) )

        #         #  note: use model.c8[indx].expr.to_string()  to print out the constraint in a human readable form
        #         #                ^ USES BASE 1 INDEXING!!! WTF??
                

        # # constrain utilization of existing routes that are within planning_fixed_end
        # model.c11  = pe.ConstraintList()
        # for p in model.fixed_dmr_ids:
        #     # less than constraint because equality should be achievable (if we're only using existing routes that have all previously been scheduled and deconflicted together - which is the case for current version of GP), but want to allow route to lessen its utilization if a more valuable route is available. 
        #     #  add in an epsilon at the end, because it may be that the utilization number was precisely chosen to meet the minimum data volume requirement -  don't want to not make the minimum data volume requirement this time because of round off error
        #     model.c11.add( model.var_dmr_utilization[p] <= utilization_by_existing_route_id[p] + self.epsilon_fixed_utilization) 
        
        # # constrain utilization reward of existing routes by the input utilization numbers
        # model.c12  = pe.ConstraintList()
        # for p in model.existing_dmr_ids:
        #     # constrain the reward utilization (used in obj function) by the input utilization for this existing route
        #     model.c12.add( model.var_existing_dmr_utilization_reward[p] <= utilization_by_existing_route_id[p] ) 
        #     # also constrain by the current utilization in the optimization
        #     model.c12.add( model.var_existing_dmr_utilization_reward[p] <= model.var_dmr_utilization[p] ) 

        # print_verbose('make obj',verbose)


        # # from circinus_tools import debug_tools
        # # debug_tools.debug_breakpt()

        # ##############################
        # #  Make objective
        # ##############################


        # #  determine which time points to use as "spot checks" on resource margin. These are the points that will be used in the objective function for maximizing resource margin
        # timepoint_spacing = ceil(es_num_timepoints/self.resource_margin_obj_num_timepoints)
        # # need to turn the generator into a list for slicing
        # #  note: have to get the generator again
        # decimated_tp_indcs = list(self.es_time_getter_dc.get_tp_indcs())[::timepoint_spacing]
        # rsrc_norm_f = len(decimated_tp_indcs) * len(model.sat_indcs)

        # def obj_rule(model):
        #     #  note the first two objectives are for all observations, not just mutable observations

        #     # obj [1]
        #     total_dv_term = self.obj_weights['obs_dv'] * 1/model.par_total_obs_dv * sum(model.par_dmr_dv[p]*model.var_dmr_utilization[p] for p in model.dmr_ids) 

        #     # obj [2]
        #     latency_term = self.obj_weights['route_latency'] * 1/len(model.obs_windids) * sum(model.var_latency_sf_obs[o] for o in model.obs_windids)
            
        #     # obj [5]
        #     energy_margin_term = self.obj_weights['energy_storage'] * 1/rsrc_norm_f * sum(model.var_sats_estore[sat_indx,tp_indx]/model.par_sats_estore_max[sat_indx] for tp_indx in decimated_tp_indcs for sat_indx in model.sat_indcs)

        #     # obj [6]
        #     existing_routes_term = self.obj_weights['existing_routes'] * 1/sum_existing_route_utilization * sum(model.var_existing_dmr_utilization_reward[p] for p in model.existing_dmr_ids) if len(model.existing_dmr_ids) > 0 else 0

        #     if len(min_var_inter_sat_act_constr_violation_list) > 0:
        #         inter_sat_act_constr_violations_term = self.obj_weights['inter_sat_act_constr_violations'] * 1/sum(min_var_inter_sat_act_constr_violation_list) * sum(model.var_inter_sat_act_constr_violations[indx] for indx in range(1,len(model.var_inter_sat_act_constr_violations)+1))
        #     else:
        #         inter_sat_act_constr_violations_term = 0

        #     if len(min_var_intra_sat_act_constr_violation_list) > 0:
        #         intra_sat_act_constr_violations_term = self.obj_weights['intra_sat_act_constr_violations'] * 1/sum(min_var_intra_sat_act_constr_violation_list) * sum(model.var_intra_sat_act_constr_violations[indx] for indx in range(1,len(model.var_inter_sat_act_constr_violations)+1))
        #     else:
        #         intra_sat_act_constr_violations_term = 0

        #     # from circinus_tools import debug_tools
        #     # debug_tools.debug_breakpt()

        #     return total_dv_term + latency_term + energy_margin_term + existing_routes_term - inter_sat_act_constr_violations_term - intra_sat_act_constr_violations_term
            
        # model.obj = pe.Objective( rule=obj_rule, sense=pe.maximize )

        self.model_constructed = True

        # print(self.planning_start_dt)
        # print(self.planning_end_dt)
        # print([rt.get_obs().end for rt in  selected_routes])
        # print('all minus mutable')
        # print([self.all_acts_by_windid[a] for a in (set(all_acts_windids)-set(mutable_acts_windids))])
        # from circinus_tools import debug_tools
        # debug_tools.debug_breakpt()

    