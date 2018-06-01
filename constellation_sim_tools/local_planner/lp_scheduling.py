from collections import namedtuple

from pyomo import environ  as pe

from circinus_tools.scheduling.custom_window import   ObsWindow,  DlnkWindow, XlnkWindow,  EclipseWindow
from circinus_tools.scheduling.formulation.agent_scheduler import AgentScheduling
from circinus_tools.scheduling.routing_objects import DataRoute,DataMultiRoute,RoutingObjectID

from circinus_tools import debug_tools

def print_verbose(string,verbose=False):
    if verbose:
        print(string)

class LPScheduling(AgentScheduling):
    """ local planner scheduling algorithm/solver, using pyomo"""
    
    #  holds a pairing of an inflow to an outflow. all possible pairings are considered in the scheduler.  note that the unified flow IDs are considered a different namespace from that of the inflow and outflow IDs ( which are actually the routing object IDs for the underlying data routes)
    #  note that in constructing unified flows, we ensure that all rx activities on the inflow precede all tx activities on the outflow ( for the satellite index that we're considering), so we do not end up splitting the flow of a DataMultiRoute
    UnifiedFlow = namedtuple('UnifiedFlow', 'ID inflow outflow')

    def __init__(self,lp_params):
        """initializes based on parameters
        
        initializes based on parameters
        :param lp_params: global namespace parameters created from input files (possibly with some small non-structural modifications to params). The name spaces here should trace up all the way to the input files.
        :type params: dict
        """

        super().__init__()

        sat_params = lp_params['orbit_prop_params']['sat_params']
        gp_as_params = lp_params['gp_general_params']['activity_scheduling_params']
        lp_general_params = lp_params['const_sim_inst_params']['lp_general_params']

        self.sat_indx = lp_params['lp_instance_params']['sat_indx']

        # this is the minimum obs dv that must be downlinked by a unified route in order for it to count it towards objective terms (other than total dv)
        self.min_obs_dv_dlnk_req =gp_as_params['min_obs_dv_dlnk_req_Mb']

        self.sat_activity_params = sat_params['activity_params']

        self.min_act_duration_s = {
            ObsWindow: self.sat_activity_params['min_duration_s']['obs'],
            DlnkWindow: self.sat_activity_params['min_duration_s']['dlnk'],
            XlnkWindow: self.sat_activity_params['min_duration_s']['xlnk']
        }

        self.obj_weights = lp_general_params['obj_weights']
        self.solver_name = lp_general_params['solver_name']
        self.solver_params = lp_general_params['solver_params']

        self.dv_epsilon = lp_general_params['dv_epsilon_Mb']

    @staticmethod
    def get_act_model_objs(act,model):
        """ get the pyomo model objects used for modeling activity utilization."""
        # note: can be overridden in subclass - this function provides an example implementation

        model_objs_act = {
            'act_object': act,
            'var_dv_utilization': model.var_activity_utilization[act.window_ID]*model.par_act_capacity[act.window_ID],
            'par_dv_capacity': model.par_act_capacity[act.window_ID],
            'var_act_indic': model.var_act_indic[act.window_ID],
        }

        return model_objs_act

    def get_flow_structs(self,inflows,outflows):

        all_flows = inflows + outflows

        unified_flows = []
        unified_flow_indx = 0
        #  how much data volume each flow can deliver/carry. note that these capacity numbers already factor in the utilizations of the data routes
        capacity_by_partial_flow_id = {}
        possible_unified_flows_ids_by_inflow_id = {}
        possible_unified_flows_ids_by_outflow_id = {}


        for inflow in inflows:
            capacity_by_partial_flow_id[inflow.ID] = inflow.data_vol
            possible_unified_flows_ids_by_inflow_id.setdefault(inflow.ID,[])

            for outflow in outflows:
                capacity_by_partial_flow_id[outflow.ID] = outflow.data_vol
                possible_unified_flows_ids_by_outflow_id.setdefault(outflow.ID,[])

                #  if the data delivered from this inflow is available before the outflow, then they can be combined into a unified flow
                if inflow.flow_precedes(outflow):
                    unified_flows.append(self.UnifiedFlow(ID=unified_flow_indx,inflow=inflow, outflow=outflow))

                    possible_unified_flows_ids_by_inflow_id[inflow.ID].append(unified_flow_indx)
                    possible_unified_flows_ids_by_outflow_id[outflow.ID].append(unified_flow_indx)
                    unified_flow_indx += 1

        all_acts_windids = set()
        all_acts = []
        capacity_by_act_windid = {}
        inflows_ids_by_act_windid = {}
        outflows_ids_by_act_windid = {}
        for flow in all_flows:
            for act in flow.get_required_acts():
                act_windid = act.window_ID
                if act_windid not in all_acts_windids: all_acts.append(act)
                all_acts_windids.add(act_windid)

                #  note that we're using the original activity capacity/available data volume here, rather than a capcity number constrained by existing routes.  this is because the LP can decide, if it wants to, to extend the length of an activity to use all of its potential data volume. Currently though the actual data routes, which make use of that potential data volume, are constrained by existing utilization values ( so we can lengthen an activity to get more data volume, but we can't do anything with it because we can't enlarge the data routes that would use that data volume - in the current version of the LP)
                capacity_by_act_windid[act_windid] = act.data_vol

                inflows_ids_by_act_windid.setdefault(act_windid, [])
                outflows_ids_by_act_windid.setdefault(act_windid, [])
                if flow.is_inflow():
                    inflows_ids_by_act_windid[act_windid].append(flow.ID)
                elif flow.is_outflow():
                    outflows_ids_by_act_windid[act_windid].append(flow.ID)


        return unified_flows,capacity_by_partial_flow_id,possible_unified_flows_ids_by_inflow_id,possible_unified_flows_ids_by_outflow_id,all_acts,all_acts_windids,capacity_by_act_windid,inflows_ids_by_act_windid,outflows_ids_by_act_windid



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
            all_acts,
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

        self.unified_flows = unified_flows
        self.inflows = inflows
        self.outflows = outflows

        # # these dmr subscripts probably should've been done using the unique IDs for the objects, rather than their arbitrary locations within a list. Live and learn, hélas...

        # model.inflow_ids = pe.Set(initialize= [flow.ID for flow in inflows])
        # model.outflow_ids = pe.Set(initialize= [flow.ID for flow in outflows])
        inflow_ids = [flow.ID for flow in inflows]
        outflow_ids = [flow.ID for flow in outflows]


        #  subscript for each activity a
        model.all_act_windids = pe.Set(initialize= all_acts_windids)

        # Make an index in the model for every inflow and outflow
        all_partial_flow_ids = [flow.ID for flow in inflows] + [flow.ID for flow in outflows]
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

        #  the maximum unified flow capacity cannot exceed the amount of data volume delivered by inflows or the amount of data volume carried out by outflows
        model.par_max_possible_unified_flow_capacity = min(
            sum(capacity_by_partial_flow_id[i] for i in inflow_ids),
            sum(capacity_by_partial_flow_id[o] for o in outflow_ids)
        )
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
        model.var_unified_flow_dv  = pe.Var (model.unified_flow_ids, within = pe.PositiveReals)
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


        def c4_rule( model,a):
            return model.var_act_indic[a] >=  model.var_activity_utilization[a]
        model.c4 =pe.Constraint ( model.all_act_windids,  rule=c4_rule)  

        def c5a_rule( model,a):
            return model.par_act_capacity[a] * model.var_activity_utilization[a] - sum(model.par_partial_flow_capacity[i] * model.var_partial_flow_utilization[i] for i in inflows_ids_by_act_windid[a]) >= 0 
        model.c5a =pe.Constraint ( model.all_act_windids,  rule=c5a_rule)  

        def c5b_rule( model,a):
            return model.par_act_capacity[a] * model.var_activity_utilization[a] - sum(model.par_partial_flow_capacity[o] * model.var_partial_flow_utilization[o] for o in outflows_ids_by_act_windid[a]) >= 0 
        model.c5b =pe.Constraint ( model.all_act_windids,  rule=c5b_rule)  


        #  this is hacky, but okay for now... encase all activities within an outer list so it looks like all the activities for a constellation of one satellite. this is needed for gen_sat_act_duration_constraints() call below. todo: technically this is a bad design and we shouldn't have to jimmy an argument to pass it
        sat_acts = [all_acts]

        #  6 is activity minimum time duration
        model.c6  = pe.ConstraintList()
        # pass the model objects getter function so it can be called in place
        self.gen_sat_act_duration_constraints(
            model,
            model.c6,
            sat_acts,
            num_sats=1,
            act_model_objs_getter=self.get_act_model_objs
        )

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

        ##############################
        #  Make objective
        ##############################


        #  determine which time points to use as "spot checks" on resource margin. These are the points that will be used in the objective function for maximizing resource margin
        # timepoint_spacing = ceil(es_num_timepoints/self.resource_margin_obj_num_timepoints)
        # # need to turn the generator into a list for slicing
        # #  note: have to get the generator again
        # decimated_tp_indcs = list(self.es_time_getter_dc.get_tp_indcs())[::timepoint_spacing]
        # rsrc_norm_f = len(decimated_tp_indcs) * len(model.sat_indcs)

        def obj_rule(model):
            #  note the first two objectives are for all observations, not just mutable observations

            # obj [1]
            total_dv_term = self.obj_weights['obs_dv'] * 1/model.par_max_possible_unified_flow_capacity * sum(model.var_unified_flow_dv[u] for u in model.unified_flow_ids) 

            # # obj [2]
            # latency_term = self.obj_weights['route_latency'] * 1/len(model.obs_windids) * sum(model.var_latency_sf_obs[o] for o in model.obs_windids)
            
            # # obj [5]
            # energy_margin_term = self.obj_weights['energy_storage'] * 1/rsrc_norm_f * sum(model.var_sats_estore[sat_indx,tp_indx]/model.par_sats_estore_max[sat_indx] for tp_indx in decimated_tp_indcs for sat_indx in model.sat_indcs)

            # # obj [6]
            # existing_routes_term = self.obj_weights['existing_routes'] * 1/sum_existing_route_utilization * sum(model.var_existing_dmr_utilization_reward[p] for p in model.existing_dmr_ids) if len(model.existing_dmr_ids) > 0 else 0

            return total_dv_term #+ latency_term + energy_margin_term + existing_routes_term
            
        model.obj = pe.Objective( rule=obj_rule, sense=pe.maximize )

        self.model_constructed = True

    def extract_updated_routes( self, existing_planned_routes_by_id, utilization_by_planned_route_id, latest_dr_uid,lp_agent_ID,verbose = False):
        #  note that we don't update any scheduled data volumes for routes or Windows, or any of the window timing here. the current local planner does not do this, it can only update the utilization fraction for an existing route

        # inflow_by_id = [inflow.ID:inflow for inflow in inflows]
        # outflow_by_id = [outflow.ID:outflow for outflow in outflows]

        updated_routes = []
        updated_utilization_by_route_id = {}

        # TODO: also need to update utilization for routes that did not end up scheduled, and have had their util reduced - from those in utilization_by_planned_route_id...

        for flow in self.unified_flows:
            #  if this unified flow possibility was actually chosen
            if pe.value(self.model.var_unified_flow_dv[flow.ID]) >= self.min_obs_dv_dlnk_req:
                flow_dv = pe.value(self.model.var_unified_flow_dv[flow.ID])

                #  if the inflow ID is the same as the outflow ID, this must be an existing/ already planned route
                if flow.inflow.rt_ID == flow.outflow.rt_ID:
                    rt_id = flow.outflow.rt_ID
                    rt = existing_planned_routes_by_id[rt_id]
                    new_utilization = flow_dv/rt.scheduled_dv

                    #  do some sanity checks:
                    # - the route is in the existing planned routes
                    # - the new utilization for the route is less than or equal to the previous utilization
                    # -  we have not already added this route to updated routes ( that would imply that the route is present more than once in the unified flows which should not be possible)
                    assert(rt_id in existing_planned_routes_by_id.keys())
                    assert( new_utilization <= utilization_by_planned_route_id[rt_id])
                    assert(rt not in updated_routes)

                    updated_routes.append(rt)
                    updated_utilization_by_route_id[rt_id] = new_utilization


                #  if we have an entirely new matching of an inflow to an outflow, which constitutes a new route
                else:
                    inflow = flow.inflow
                    outflow = flow.outflow

                    rt,latest_dr_uid = self.graft_routes(inflow,outflow,flow_dv,latest_dr_uid,lp_agent_ID)
                    updated_routes.append(rt)
                    #  we have created a new data route for this slice of data volume, so by definition the utilization is 100%
                    updated_utilization_by_route_id[rt.ID] = 1.0

                # note that we don't consider the case where an inflow data container is mapped to the outflow data route that was actually INTENDED TO TAKE (this won't get caught by flow.inflow.rt_ID == flow.outflow.rt_ID check, because the rt_ID for a data container is not the planned route container, but rather the actual path it has taken, with its own unique ID). this produces some clutter, but it is fine as long as we mark that utilization has gone to zero for the original route.

        debug_tools.debug_breakpt()

        return updated_routes, updated_utilization_by_route_id



    def graft_routes(self,inflow,outflow,remaining_unified_flow_dv,latest_dr_uid,lp_agent_ID):
        """Combine the underlying data routes within the inflow and outflow"""

        #  each partial flow is either a simple DataRoute or a DataMultiRoute. this code handles both cases

        # reduce the flows to simple DataRoute objects, with the amount of data volume for each data route
        inflow_dvs_by_dr=  {dr:dv for (dr,dv) in inflow.get_simple_drs_dvs()}
        outflow_dvs_by_dr=  {dr:dv for (dr,dv) in outflow.get_simple_drs_dvs()}

        #  putting in a reminder to test this code, because I'm afraid I won't end up testing it...
        if len(inflow_dvs_by_dr.keys()) > 1 or len(outflow_dvs_by_dr.keys()) > 1:
            print('need to test this case!')
            debug_tools.debug_breakpt()

        inflow_dr_winds_by_dr = {dr:dr.get_inflow_winds_rx_sat(self.sat_indx) for dr in inflow_dvs_by_dr.keys()}
        outflow_dr_winds_by_dr = {dr:dr.get_outflow_winds_tx_sat(self.sat_indx) for dr in outflow_dvs_by_dr.keys()}

        ro_id = RoutingObjectID(creator_agent_ID=lp_agent_ID, creator_agent_ID_indx=latest_dr_uid)
        latest_dr_uid += 1

        def make_new_route(inflow_winds,outflow_winds,dv):
            all_rt_winds = inflow_winds+outflow_winds
            #  give all of the data routes the same id for now, because will combine them into a single data multi-route
            dr = DataRoute(
                agent_ID=None,
                agent_ID_index=None,
                route=all_rt_winds,
                window_start_sats=DataRoute.determine_window_start_sats(all_rt_winds),
                dv=dv,
                ro_ID=ro_id
            )
            #  throw in a validation check for the data route, just in case
            dr.validate(dv_epsilon=self.dv_epsilon)

            return dr

        inflow_drs_queue = list(inflow_dvs_by_dr.keys())
        outflow_drs_queue = list(outflow_dvs_by_dr.keys())

        #  loops through, matching every inflow route to an outflow route and assigning a slice of data volume to a newly created data route that grafts the two flows together. do this for as long as there is remaining data volume in the unified flow
        new_drs = []
        curr_inflow_dr = inflow_drs_queue.pop(0)
        curr_outflow_dr = outflow_drs_queue.pop(0)
        while remaining_unified_flow_dv > self.dv_epsilon:
            
            delta_dv = min(inflow_dvs_by_dr[curr_inflow_dr],outflow_dvs_by_dr[curr_outflow_dr],remaining_unified_flow_dv)

            new_dr = make_new_route(
                inflow_dr_winds_by_dr[curr_inflow_dr],
                outflow_dr_winds_by_dr[curr_outflow_dr],
                delta_dv
            )
            new_drs.append(new_dr)

            inflow_dvs_by_dr[curr_inflow_dr] -= delta_dv
            outflow_dvs_by_dr[curr_outflow_dr] -= delta_dv
            remaining_unified_flow_dv -= delta_dv

            #  we should only have inflows or outflows left to pop from the queues if we have remaining data volume
            if remaining_unified_flow_dv > self.dv_epsilon:
                if inflow_dvs_by_dr[curr_inflow_dr] < self.dv_epsilon:
                    curr_inflow_dr = inflow_drs_queue.pop(0)
                if outflow_dvs_by_dr[curr_outflow_dr] < self.dv_epsilon:
                    curr_outflow_dr = outflow_drs_queue.pop(0)

        # combine new drs into DMR
        dmr = DataMultiRoute(ro_id, data_routes=new_drs)
        # note: setting this for convenience. Also HAVE TO set the utilization for this route later because that * DMR data_vol is the real number to be used
        dmr.set_scheduled_dv_frac(1.0)

        return dmr, latest_dr_uid



    