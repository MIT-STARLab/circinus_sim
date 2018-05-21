from circinus_tools.plotting import plot_tools as pltl

class SimPlotting():
    def __init__(self,sim_params):

        plot_params = sim_params['const_sim_inst_params']['sim_plot_params']
        sat_params = sim_params['orbit_prop_params']['sat_params']
        # self.obs_params = gp_params['orbit_prop_params']['obs_params']
        self.sat_id_order = sat_params['sat_id_order']
        self.plot_params = plot_params

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

    def sim_plot_all_sats_acts(self,
            sats_ids_list,
            sats_obs_winds_choices,
            sats_obs_winds,
            sats_dlnk_winds_choices,
            sats_dlnk_winds, 
            sats_xlnk_winds_choices,
            sats_xlnk_winds,
            plot_start_dt,
            plot_end_dt,
            base_time_dt):

        plot_params = {}
        plot_params['route_ids_by_wind'] = None
        plot_params['plot_start_dt'] = plot_start_dt
        plot_params['plot_end_dt'] = plot_end_dt
        plot_params['base_time_dt'] = base_time_dt

        plot_params['plot_title'] = 'Executed and Planned Sat Acts'
        plot_params['plot_size_inches'] = (18,12)
        plot_params['plot_include_labels'] = self.plot_params['sat_acts_plot']['include_labels']
        plot_params['plot_original_times_choices'] = True
        plot_params['plot_original_times_regular'] = False
        plot_params['show'] = False
        plot_params['fig_name'] = 'plots/sim_exec_planned_acts.pdf'
        plot_params['plot_fig_extension'] = 'pdf'

        plot_params['time_units'] = self.plot_params['sat_acts_plot']['time_units']
        plot_params['sat_id_order'] = self.sat_id_order

        plot_params['plot_xlnks_choices'] = True
        plot_params['plot_dlnks_choices'] = True
        plot_params['plot_obs_choices'] = True
        plot_params['plot_xlnks'] = True
        plot_params['plot_dlnks'] = True
        plot_params['plot_obs'] = True

        plot_params['xlnk_route_index_to_use'] = 0
        plot_params['xlnk_color_rollover'] = 5
        plot_params['xlnk_colors'] = ['#FF0000','#FF3399','#990000','#990099','#FF9900']

        pltl.plot_all_sats_acts(
            sats_ids_list,
            sats_obs_winds_choices,
            sats_obs_winds,
            sats_dlnk_winds_choices,
            sats_dlnk_winds, 
            sats_xlnk_winds_choices,
            sats_xlnk_winds,
            plot_params)