import sys
import json

sys.path.append ('..')
from circinus_tools.plotting import plot_tools as pltl


data = []
with open('inp/exec_obs_lat_cdf_data_walker_dlnk_xlnk_constr.json','r') as f:
    data.append(json.load(f))
with open('inp/exec_obs_lat_cdf_data_walker_dlnk_xlnk.json','r') as f:
    data.append(json.load(f))
with open('inp/exec_obs_lat_cdf_data_walker_dlnk.json','r') as f:
    data.append(json.load(f))
legend_labels = ['Dlnk Only','Dlnk + Xlnk','Dlnk + Xlnk, constrained']

lat_hist_x_range = (0,150) # minutes
lat_hist_num_bins = 100

# plot obs latency histogram, executed routes
pltl.plot_histogram(
    data=data,
    num_data_series=len(data),
    num_bins = lat_hist_num_bins,
    plot_type = 'cdf',
    x_range = lat_hist_x_range,
    x_title='Latency (mins)',
    y_title='Fraction of Obs Windows',
    # plot_title = 'CIRCINUS Sim: Initial Latency Histogram, executed (dv req %.1f Mb)'%(mc.min_obs_dv_dlnk_req),
    plot_title = 'CIRCINUS Sim: Initial Latency CDF, executed',
    legend_labels=legend_labels,
    plot_size_inches = (12,4),
    show=False,
    fig_name='plots/csim_obs_lat_combined_cdf.pdf'
)



