from circinus_tools.plotting import plot_tools as pltl
from circinus_tools  import io_tools

from circinus_tools import debug_tools

class SimPlotting():
    def __init__(self,sim_params):

        self.output_path = sim_params['output_path']
        plot_params = sim_params['const_sim_inst_params']['sim_plot_params']
        sat_params = sim_params['orbit_prop_params']['sat_params']
        gs_params = sim_params['orbit_prop_params']['gs_params']
        sim_run_params = sim_params['const_sim_inst_params']['sim_run_params']
        # self.obs_params = gp_params['orbit_prop_params']['obs_params']
        self.sat_id_order = sat_params['sat_id_order']
        self.gs_id_order = gs_params['gs_id_order']
        self.input_plot_params = plot_params

        self.plots_start_dt = plot_params['start_utc_dt']
        self.plots_end_dt = plot_params['end_utc_dt']
        self.plots_base_dt = sim_run_params['start_utc_dt']

        #  holds power parameters for each satellite ID,  after parsing
        self.parsed_power_params_by_sat_id = {}
        for sat_id in self.sat_id_order:
            self.parsed_power_params_by_sat_id[sat_id] = {}
            sat_edot_by_mode,sat_batt_storage,power_units,_,_ = \
                io_tools.parse_power_consumption_params(sat_params['power_params_by_sat_id'][sat_id])
            self.parsed_power_params_by_sat_id[sat_id] = {
                "sat_edot_by_mode": sat_edot_by_mode,
                "sat_batt_storage": sat_batt_storage,
                "power_units": power_units,
            }


        self.parsed_data_params_by_sat_id = {}
        for sat_id in self.sat_id_order:
            self.parsed_data_params_by_sat_id[sat_id] = {}

            sat_data_storage_params = sat_params['data_storage_params_by_sat_id'][sat_id]

            self.parsed_data_params_by_sat_id[sat_id] = {
                "d_min": sat_data_storage_params['d_min'],
                "d_max": sat_data_storage_params['d_max']
            }


        # self.plot_fig_extension=plot_params['plot_fig_extension']
        # self.time_units=plot_params['time_units']
        # self.energy_usage_plot_params=plot_params['energy_usage_plot_params']
        # self.data_usage_plot_params=plot_params['data_usage_plot_params']
        # self.obs_aoi_metrics_plot_params=plot_params['obs_aoi_metrics_plot_params']
        # self.cmd_aoi_metrics_plot_params=plot_params['cmd_aoi_metrics_plot_params']
        # self.tlm_aoi_metrics_plot_params=plot_params['tlm_aoi_metrics_plot_params']

        # self.all_targ_IDs = [targ['id'] for targ in self.obs_params['targets']]

        # self.power_params = sat_params['power_params_sorted']
        # self.data_storage_params = sat_params['data_storage_params_sorted']
        # self.sats_emin_Wh = [p_params['battery_storage_Wh']['e_min'][p_params['battery_option']] for p_params in self.power_params]
        # self.sats_emax_Wh = [p_params['battery_storage_Wh']['e_max'][p_params['battery_option']] for p_params in self.power_params]
        # self.sats_dmin_Gb = [ds_params['data_storage_Gbit']['d_min'][ds_params['storage_option']] for ds_params in self.data_storage_params]
        # self.sats_dmax_Gb = [ds_params['data_storage_Gbit']['d_max'][ds_params['storage_option']] for ds_params in self.data_storage_params]

    def get_label_getters(self):
        def xlnk_label_getter(xlnk,sat_indx):
            # dr_id = None
            # if route_ids_by_wind:
            #     dr_indcs = route_ids_by_wind.get(xlnk,None)
            #     if not dr_indcs is None:
            #         dr_id = dr_indcs[xlnk_route_index_to_use]

            # other_sat_indx = xlnk.get_xlnk_partner(sat_indx)
            # if not dr_id is None:
            #     label_text = "%d,%d" %(dr_id.get_indx(),other_sat_indx)
            #     label_text = "%s" %(dr_indcs)
            # else:         
            #     label_text = "%d" %(other_sat_indx)

            # return label_text
            rx_or_tx = 'rx' if xlnk.is_rx(sat_indx) else 'tx'
            # return "x%d,%s%d,dv %d/%d (%d)"%(xlnk.window_ID,rx_or_tx,xlnk.get_xlnk_partner(sat_indx),xlnk.executed_data_vol,xlnk.executable_data_vol,xlnk.data_vol) 
            return "x%d,%s%d,dv %d/%d"%(xlnk.window_ID,rx_or_tx,xlnk.get_xlnk_partner(sat_indx),xlnk.executed_data_vol,xlnk.executable_data_vol) 

        def dlnk_label_getter(dlnk):
            # return "d%d,g%d,dv %d/%d (%d)"%(dlnk.window_ID,dlnk.gs_indx,dlnk.executed_data_vol,dlnk.executable_data_vol,dlnk.data_vol) 
            return "d%d,g%d,dv %d/%d"%(dlnk.window_ID,dlnk.gs_indx,dlnk.executed_data_vol,dlnk.executable_data_vol)

        def obs_label_getter(obs):
            # return "o%d, dv %d/%d (%d)"%(obs.window_ID,obs.executed_data_vol,obs.executable_data_vol,obs.data_vol)
            return "o%d, dv %d/%d"%(obs.window_ID,obs.executed_data_vol,obs.executable_data_vol)

        return obs_label_getter,dlnk_label_getter,xlnk_label_getter

    XLNK_COLORS = ['#FF0000','#FF3399','#990000','#990099','#FF9900']

    def get_color_getters(self):

        def obs_color_getter(obs):
            if obs.wind_obj_type == 'injected':
                return "#CC33FF"

            return "#00FF00"

        def dlnk_color_getter(obs):
            return "#0000FF"

        def xlnk_color_getter(obs):
            return "#FF0000"
            
        # def xlnk_color_getter(xlnk):
        #     xlnk_color_indx = 0
        #     if route_ids_by_wind:
        #         dr_indcs = route_ids_by_wind.get(xlnk,None)
        #         if not dr_indcs is None:
        #             dr_id = dr_indcs[xlnk_route_index_to_use]
        #             xlnk_color_indx = dr_id.get_indx() %  xlnk_color_rollover
        #     return xlnk_colors[xlnk_color_indx]

        return obs_color_getter,dlnk_color_getter,xlnk_color_getter

    def get_time_getters(self):
        def get_start(wind):
            return wind.executed_start
        def get_end(wind):
            return wind.executed_end

        return get_start,get_end

    def get_time_getters_choices(self):
        def get_start(wind):
            return wind.start
        def get_end(wind):
            return wind.end

        return get_start,get_end

    def sim_plot_all_sats_acts(self,
            sats_ids_list,
            sats_obs_winds_choices,
            sats_obs_winds,
            sats_dlnk_winds_choices,
            sats_dlnk_winds, 
            sats_xlnk_winds_choices,
            sats_xlnk_winds,
            sats_obs_winds_failed = [],
            sats_dlnk_winds_failed = [],
            sats_xlnk_winds_failed = []):

        plot_params = {}
        plot_params['route_ids_by_wind'] = None
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Planned and Executed Sat Acts'
        plot_params['y_label'] = 'Satellite Index'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['plot_original_times_choices'] = True
        plot_params['plot_executed_times_regular'] = True
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_sats_acts.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.input_plot_params['sat_acts_plot']['time_units']
        plot_params['agent_id_order'] = self.sat_id_order

        plot_params['plot_xlnks_choices'] = True
        plot_params['plot_dlnks_choices'] = True
        plot_params['plot_obs_choices'] = True
        plot_params['plot_xlnks'] = True
        plot_params['plot_dlnks'] = True
        plot_params['plot_obs'] = True
        plot_params['plot_xlnks_failed'] =  bool(sats_xlnk_winds_failed)
        plot_params['plot_obs_failed'] = bool(sats_obs_winds_failed)
        plot_params['plot_dlnks_failed'] = bool(sats_dlnk_winds_failed)
        plot_params['plot_include_obs_labels'] = self.input_plot_params['sat_acts_plot']['include_obs_labels']
        plot_params['plot_include_xlnk_labels'] = self.input_plot_params['sat_acts_plot']['include_xlnk_labels']
        plot_params['plot_include_dlnk_labels'] = self.input_plot_params['sat_acts_plot']['include_dlnk_labels']

        plot_params['obs_choices_legend_name'] =  "O plan"
        plot_params['obs_exe_legend_name'] =  "O exec."
        plot_params['dlnk_choices_legend_name'] =  "D plan"
        plot_params['dlnk_exe_legend_name'] =  "D exec."
        plot_params['xlnk_choices_legend_name'] =  "X plan"
        plot_params['xlnk_exe_legend_name'] =  "X exec."

        plot_params['label_fontsize'] =  "12"
        

        plot_params['xlnk_route_index_to_use'] = 0
        plot_params['xlnk_color_rollover'] = 5
        plot_params['xlnk_colors'] = self.XLNK_COLORS

        obs_label_getter,dlnk_label_getter,xlnk_label_getter = self.get_label_getters()
        plot_params['obs_label_getter_func'] = obs_label_getter
        plot_params['dlnk_label_getter_func'] = dlnk_label_getter
        plot_params['xlnk_label_getter_func'] = xlnk_label_getter

        obs_color_getter,dlnk_color_getter,xlnk_color_getter = self.get_color_getters()
        plot_params['obs_color_getter_func'] = obs_color_getter
        plot_params['dlnk_color_getter_func'] = dlnk_color_getter
        plot_params['xlnk_color_getter_func'] = xlnk_color_getter

        start_getter_reg,end_getter_reg = self.get_time_getters()
        plot_params['start_getter_reg'] = start_getter_reg
        plot_params['end_getter_reg'] = end_getter_reg
        start_getter_choices,end_getter_choices = self.get_time_getters_choices()
        plot_params['start_getter_choices'] = start_getter_choices
        plot_params['end_getter_choices'] = end_getter_choices

        plot_params['failed_acts'] =  {
            "dlnks": sats_dlnk_winds_failed,
            "xlnks": sats_xlnk_winds_failed,
            "obs": sats_obs_winds_failed
        }


        pltl.plot_all_agents_acts(
            sats_ids_list,
            sats_obs_winds_choices,
            sats_obs_winds,
            sats_dlnk_winds_choices,
            sats_dlnk_winds, 
            sats_xlnk_winds_choices,
            sats_xlnk_winds,
            plot_params)


    def sim_plot_all_gs_acts(self,
            gs_ids_list,
            gs_dlnk_winds_choices,
            gs_dlnk_winds,
            gs_dlnk_winds_failed=[]):

        plot_params = {}
        plot_params['route_ids_by_wind'] = None
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'Executed, Planned, and Failed GS Downlinks' if gs_dlnk_winds_failed else 'Executed and Planned GS Downlinks'
        plot_params['y_label'] = 'Ground Station Index'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['plot_include_labels'] = self.input_plot_params['gs_acts_plot']['include_labels']
        plot_params['plot_original_times_choices'] = True
        plot_params['plot_executed_times_regular'] = True
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_gs_dlnks.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.input_plot_params['gs_acts_plot']['time_units']
        plot_params['agent_id_order'] = self.gs_id_order
        plot_params['plot_include_dlnk_labels'] = True
        
        # "Planned"
        plot_params['plot_dlnks_choices'] = True


        # "Executed"
        plot_params['plot_dlnks'] = True

        # "Failed"
        plot_params['plot_dlnks_failed'] = bool(gs_dlnk_winds_failed)
        plot_params['failed_acts'] =  {
            "dlnks": gs_dlnk_winds_failed
        }
        # set other acts to False (gs doesn't have xlnks or obs)
        non_relevant_act_strs = ['xlnks','obs']
        for act in non_relevant_act_strs:
            plot_params['plot_%s_choices' % act] = False
            plot_params['plot_%s' % act] = False
            plot_params['plot_%s_failed' % act] = False


        plot_params['xlnk_route_index_to_use'] = 0
        plot_params['xlnk_color_rollover'] = 5
        plot_params['xlnk_colors'] = ['#FF0000','#FF3399','#990000','#990099','#FF9900']

        obs_label_getter,dlnk_label_getter,xlnk_label_getter = self.get_label_getters()
        plot_params['obs_label_getter_func'] = obs_label_getter
        plot_params['dlnk_label_getter_func'] = dlnk_label_getter
        plot_params['xlnk_label_getter_func'] = xlnk_label_getter

        start_getter_reg,end_getter_reg = self.get_time_getters()
        plot_params['start_getter_reg'] = start_getter_reg
        plot_params['end_getter_reg'] = end_getter_reg
        start_getter_choices,end_getter_choices = self.get_time_getters_choices()
        plot_params['start_getter_choices'] = start_getter_choices
        plot_params['end_getter_choices'] = end_getter_choices

        pltl.plot_all_agents_acts(
            gs_ids_list,
            [],
            [],
            gs_dlnk_winds_choices,
            gs_dlnk_winds, 
            [],
            [],
            plot_params)


    def sim_plot_all_sats_energy_usage(self,
            sats_ids_list,
            energy_usage,
            ecl_winds):


        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Executed Energy Storage Utilization'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_sats_energy.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.input_plot_params['energy_usage_plot']['time_units']
        plot_params['sat_id_order'] = self.sat_id_order

        plot_params['sats_emin_Wh'] = [self.parsed_power_params_by_sat_id[sat_id]['sat_batt_storage']['e_min'] for sat_id in self.sat_id_order]
        plot_params['sats_emax_Wh'] = [self.parsed_power_params_by_sat_id[sat_id]['sat_batt_storage']['e_max'] for sat_id in self.sat_id_order]

        plot_params['energy_usage_plot_params'] = self.input_plot_params['energy_usage_plot']

        pltl.plot_energy_usage(
            sats_ids_list,
            energy_usage,
            ecl_winds,
            plot_params)


    def sim_plot_all_sats_data_usage(self,
            sats_ids_list,
            data_usage,
            ecl_winds):


        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Executed Data Storage Utilization'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_sats_data.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.input_plot_params['data_usage_plot']['time_units']
        plot_params['sat_id_order'] = self.sat_id_order

        plot_params['sats_dmin_Gb'] = [self.parsed_data_params_by_sat_id[sat_id]['d_min'] for sat_id in self.sat_id_order]
        plot_params['sats_dmax_Gb'] = [self.parsed_data_params_by_sat_id[sat_id]['d_max'] for sat_id in self.sat_id_order]

        plot_params['data_usage_plot_params'] = self.input_plot_params['data_usage_plot']

        pltl.plot_data_usage(
            sats_ids_list,
            data_usage,
            ecl_winds,
            plot_params)


    def sim_plot_all_sats_failures_on_data_usage(self,
            sats_ids_list,
            exec_failures_dicts_list,
            data_usage):

        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Activity Failures vs. Data Storage'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_sats_exec_failures.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.input_plot_params['failures_on_data_usage_plot']['time_units']
        plot_params['sat_id_order'] = self.sat_id_order

        plot_params['sats_dmin_Gb'] = [self.parsed_data_params_by_sat_id[sat_id]['d_min'] for sat_id in self.sat_id_order]
        plot_params['sats_dmax_Gb'] = [self.parsed_data_params_by_sat_id[sat_id]['d_max'] for sat_id in self.sat_id_order]

        plot_params['failures_on_data_usage_plot_params'] = self.input_plot_params['failures_on_data_usage_plot']

        pltl.plot_failures_on_data_usage(
            sats_ids_list,
            data_usage,
            exec_failures_dicts_list,
            plot_params)


    def plot_obs_aoi_at_collection(self,
            targ_ids_list,
            aoi_curves_by_targ_id):

        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Executed Observation Target AoI, at collection'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_obs_aoi_collection.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['ylabel'] = 'Target Index,\nAoI (hours)\n'
        plot_params['time_units'] = self.input_plot_params['obs_aoi_plot']['x_axis_time_units']

        plot_params['plot_bound_min_aoi_hours'] = self.input_plot_params['obs_aoi_plot']['plot_bound_min_aoi_hours']
        plot_params['plot_bound_max_aoi_hours'] = self.input_plot_params['obs_aoi_plot']['plot_bound_max_aoi_hours']
        
        plot_params['include_legend'] = False


        pltl.plot_aoi_by_item(
            targ_ids_list,
            aoi_curves_by_targ_id,
            plot_params
        )

    def plot_obs_aoi_w_routing(self,
            targ_ids_list,
            aoi_curves_by_targ_id):

        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Executed Observation Target AoI, with routing'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_obs_aoi_routing.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['ylabel'] = 'Target Index,\nAoI (hours)\n'
        plot_params['time_units'] = self.input_plot_params['obs_aoi_plot']['x_axis_time_units']

        plot_params['plot_bound_min_aoi_hours'] = self.input_plot_params['obs_aoi_plot']['plot_bound_min_aoi_hours']
        plot_params['plot_bound_max_aoi_hours'] = self.input_plot_params['obs_aoi_plot']['plot_bound_max_aoi_hours']

        plot_params['include_legend'] = False

        pltl.plot_aoi_by_item(
            targ_ids_list,
            aoi_curves_by_targ_id,
            plot_params
        )

    def plot_sat_cmd_aoi(self,
            sat_ids_list,
            aoi_curves_by_sat_id,
            all_downlink_winds = [],
            gp_replan_freq = None):

        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['plot_title'] = 'CIRCINUS Sim: Satellite CMD Uplink AoI'
        plot_params['fig_name'] = self.output_path+'plots/csim_sat_cmd_aoi.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['ylabel'] = 'Satellite Index,\nAoI (hours)\n'
        plot_params['time_units'] = self.input_plot_params['sat_cmd_aoi_plot']['x_axis_time_units']

        """ plot_params['plot_bound_min_aoi_hours'] = self.input_plot_params['sat_cmd_aoi_plot']['plot_bound_min_aoi_hours']
        plot_params['plot_bound_max_aoi_hours'] = self.input_plot_params['sat_cmd_aoi_plot']['plot_bound_max_aoi_hours'] """

        plot_params['plot_bound_min_aoi_hours'] = 0
        plot_params['plot_bound_max_aoi_hours'] = max([max(x['y']) for x in aoi_curves_by_sat_id.values()])

        plot_params['include_legend'] = False

        pltl.plot_aoi_by_item(
            sat_ids_list,
            aoi_curves_by_sat_id,
            plot_params,
            all_downlink_winds,
            gp_replan_freq
        )

    def sim_plot_all_sats_failures_on_cmd_aoi(self,
            sats_ids_list,
            non_exec_failures_dicts_list,
            cmd_aoi_curves_by_sat_id):
        # TODO: implement similar to failures on data_usage
        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Non-Exec Failures againt Satellite CMD Uplink AoI'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_non-exec_failures_on_sat_cmd_aoi.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['ylabel'] = 'Satellite Index,\nAoI (hours)\n'
        plot_params['time_units'] = self.input_plot_params['sat_cmd_aoi_plot']['x_axis_time_units']

        # TODO: do this dynamically based on max time in cmd_aoi_curves_by_sat_id
        plot_params['plot_bound_min_aoi_hours'] = 0
        plot_params['plot_bound_max_aoi_hours'] = max([max(x['y']) for x in cmd_aoi_curves_by_sat_id.values()])

        plot_params['include_legend'] = False

        pltl.plot_failures_on_aoi(
            sats_ids_list,
            non_exec_failures_dicts_list,
            cmd_aoi_curves_by_sat_id,
            plot_params
        ) 

    def plot_sat_tlm_aoi(self,
            sat_ids_list,
            aoi_curves_by_sat_id):

        plot_params = {}
        plot_params['plot_start_dt'] = self.plots_start_dt
        plot_params['plot_end_dt'] = self.plots_end_dt
        plot_params['base_time_dt'] = self.plots_base_dt

        plot_params['plot_title'] = 'CIRCINUS Sim: Satellite TLM Downlink AoI'
        plot_params['plot_size_inches'] = (18,9)
        plot_params['show'] = False
        plot_params['fig_name'] = self.output_path+'plots/csim_sat_tlm_aoi.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['ylabel'] = 'Satellite Index,\nAoI (hours)\n'
        plot_params['time_units'] = self.input_plot_params['sat_tlm_aoi_plot']['x_axis_time_units']

        plot_params['plot_bound_min_aoi_hours'] = self.input_plot_params['sat_tlm_aoi_plot']['plot_bound_min_aoi_hours']
        plot_params['plot_bound_max_aoi_hours'] = self.input_plot_params['sat_tlm_aoi_plot']['plot_bound_max_aoi_hours']

        plot_params['include_legend'] = False

        pltl.plot_aoi_by_item(
            sat_ids_list,
            aoi_curves_by_sat_id,
            plot_params
        )