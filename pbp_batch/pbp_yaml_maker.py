import yaml
import os
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from argparse import Namespace
import soundfile as sf
#from matplotlib.sphinxext.mathmpl import latex_math
from numba import long_
from pbp.main_meta_generator import run_main_meta_generator
from pbp.main_hmb_generator import run_main_hmb_generator
from pbp.main_plot import run_main_plot
#from pbp.plotting import plot_dataset_summary
import xarray as xr
import matplotlib
#from pbp.simpleapi import HmbGen
import pandas as pd
import matplotlib.pyplot as plt
from multiprocessing import Process
from collections import defaultdict
import shutil

#from prefect import flow, task
#from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta


def configuration():
    # Deployment ID:
    deploymentID = 'NEFSC_SBNMS_201811_SB03'

    # default parameters:
    default_gain_setting = 'GAIN_HIGH'  # default is deployment gain setting not in Makara
    fmin_hz = 10
    audio_file_format = '.wav'

    # Specify the paths to Makara and servers paths
    processing_queue_dir = r'C:\Users\xavier.mouy\Documents\GitHub\pbp-batch\test_outputs\results\processing_queue'
    audio_root_dir = r'F:'
    results_root_dir = r'C:\Users\xavier.mouy\Documents\GitHub\pbp-batch\test_outputs\results'
    makara_xlsfile_path = r'C:\Users\xavier.mouy\Documents\Projects\2024_NERACOOS_Soundscape_GOM\Analysis\MAKARA_metadata_dump_20241031.xlsx'
    variableAttributes_template_path = r'C:\Users\xavier.mouy\Documents\GitHub\pbp-batch\yaml_templates\variableAttributes_template.yaml'
    globalAttributes_template_path = r'C:\Users\xavier.mouy\Documents\GitHub\pbp-batch\yaml_templates\pbpbatch_globalAttributes_template.yaml'

    return deploymentID, audio_file_format, processing_queue_dir, default_gain_setting, fmin_hz, audio_root_dir, results_root_dir, makara_xlsfile_path, variableAttributes_template_path, globalAttributes_template_path

class QuotedStr(str):
    pass

def quoted_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

def format_path(path):
    p = Path(path)
    # Convert to Windows-style path
    return str(p.as_posix()).replace('/', '\\') if os.name == 'nt' else str(p)

#@task(retries=5, retry_delay_seconds=20, name='Write temp. YAML file')
def write_pbp_glabalAttributes_file(template_dict=None,deploymentID=None,lat=None,lon=None,start_datetime=None,end_datetime=None,prefix=None,fmin_hz=None,fmax_hz=None,instrument=None,device=None,sysgain=None,meta_dir=None,nc_dir=None,json_dir=None,audio_dir=None,xnl_dir=None,logs_dir=None, water_depth_m=None):

    # Update template with values from current yaml file
    template_dict['date_created'] = str(datetime.now().strftime("%Y%m%d"))
    template_dict['time_coverage_start'] = str(pd.to_datetime(str(start_datetime)).strftime('%Y%m%d'))
    template_dict['time_coverage_end'] = str(pd.to_datetime(str(end_datetime)).strftime('%Y%m%d'))
    template_dict['geospatial_bounds'] = 'POINT (' + str(lat) + ' ' + str(lon)  + ")"
    template_dict['platform'] = deploymentID
    template_dict['instrument'] = device
    template_dict['comment'] = 'Water depth at the recording site is ' + str(water_depth_m) + 'm'
    template_dict['pbp_job_agent']['output_prefix'] = QuotedStr(deploymentID + '_')
    template_dict['pbp_job_agent']['recorder'] = QuotedStr('SOUNDTRAP')
    template_dict['pbp_job_agent']['audio_base_dir'] = QuotedStr(Path(audio_dir).absolute().as_posix())
    template_dict['pbp_job_agent']['json_base_dir'] = QuotedStr(Path(json_dir).absolute().as_posix())
    template_dict['pbp_job_agent']['xml_dir'] = QuotedStr(Path(xnl_dir).absolute().as_posix())
    template_dict['pbp_job_agent']['start'] = QuotedStr(template_dict['time_coverage_start'])
    template_dict['pbp_job_agent']['end'] = QuotedStr(template_dict['time_coverage_end'])
    template_dict['pbp_job_agent']['prefix'] = QuotedStr(prefix)
    template_dict['pbp_job_agent']['nc_output_dir'] = QuotedStr(str(Path(nc_dir).absolute().as_posix()))
    template_dict['pbp_job_agent']['global_attrs'] = QuotedStr(Path(os.path.join(meta_dir,'globalAttributes_'+ deploymentID + '.yaml')).absolute().as_posix())
    template_dict['pbp_job_agent']['variable_attrs'] = QuotedStr(str(Path(os.path.join(meta_dir,'variableAttributes_'+ deploymentID + '.yaml')).absolute().as_posix()))
    template_dict['pbp_job_agent']['sensitivity_flat_value'] = QuotedStr(sysgain)
    template_dict['pbp_job_agent']['subset_to'] = QuotedStr(str(fmin_hz) + ' '  + str(fmax_hz))
    template_dict['pbp_job_agent']['latlon'] = QuotedStr(str(lat) + ' ' + str(lon))
    template_dict['pbp_job_agent']['title'] = QuotedStr(deploymentID)
    template_dict['pbp_job_agent']['ylim'] = QuotedStr(str(fmin_hz) + ' ' + str(fmax_hz))
    template_dict['pbp_job_agent']['cmlim'] = QuotedStr(template_dict['pbp_job_agent']['cmlim'])
    template_dict['pbp_job_agent']['meta_output_dir'] = QuotedStr(Path(meta_dir).absolute().as_posix())
    template_dict['pbp_job_agent']['log_dir'] = QuotedStr(Path(logs_dir).absolute().as_posix())
    template_dict['pbp_job_agent']['voltage_multiplier'] = QuotedStr('')
    template_dict['pbp_job_agent']['sensitivity_uri'] = QuotedStr('')
    # water depth

    print(' ')
    print('-------------------------------------------------------')
    print('----------------------- Summary -----------------------')
    print(' ')
    for key, value in template_dict.items():
        print(f"{key}: \t{value}")
    for key, value in template_dict['pbp_job_agent'].items():
        print(f"{key}: \t{value}")

    # write GlobalAttribute file for pbp
    if os.name == "posix":
        output_file = Path(template_dict['pbp_job_agent']['global_attrs']).as_posix()
    else:
        output_file = str(Path(template_dict['pbp_job_agent']['global_attrs']))
    with open(output_file, "w") as file:
        yaml.dump(template_dict, file, default_flow_style=False, sort_keys=False)
    return template_dict

    print(f"Generated YAML saved to: {output_file}")

#@task(retries=5, retry_delay_seconds=5, name="Load YAML file")
def load_yaml_file(yaml_file):
    #yaml_file = str(yaml_file)
    with open(yaml_file, "r") as file:
        config = yaml.safe_load(file)
    return config
def write_yaml_file(config, outfile_path):
    stream = file(outfile_path, 'w')
    yaml.dump(data, stream)  # Write a YAML representation of data to 'document.yaml'.

def search_folder_path(folder_name=None, search_root=None):
    matches = [p for p in Path(search_root+os.sep).rglob(folder_name) if p.is_dir()]
    return matches

def find_folder_with_most_audio_files(search_root=None, files_extension='.wav'):
    search_root = Path(search_root)
    wav_counts = defaultdict(int)

    for wav_file in search_root.rglob("*" + files_extension):
        parent_folder = wav_file.parent
        wav_counts[parent_folder] += 1

    if not wav_counts:
        return None, 0  # No wav files found

    # Find the folder with the most wav files
    most_wavs_folder = max(wav_counts, key=wav_counts.get)
    return most_wavs_folder, wav_counts[most_wavs_folder]

def get_nth_wav_file(folder_path, n=0):
    wav_files = sorted(Path(folder_path).glob("*.wav"))
    if len(wav_files) >= n:
        return wav_files[n - 1]  # n is 1-based
    else:
        return None

def get_metadata_from_makara(xls_file=None, deploymentID=None, sysgain_setting=None):

    # init
    makara = defaultdict(int)

    # Read the XLSX file into a DataFrame
    deployments = pd.read_excel(xls_file, sheet_name='deployments')
    recordings = pd.read_excel(xls_file, sheet_name='recordings')
    devices = pd.read_excel(xls_file, sheet_name='devices')

    # convert dates
    recordings['recording_usable_start_datetime'] = pd.to_datetime(recordings['recording_usable_start_datetime'])
    recordings['recording_usable_end_datetime'] = pd.to_datetime(recordings['recording_usable_end_datetime'])

    # Select deployment
    deployment = deployments[deployments['deployment_code']==deploymentID]
    recording = recordings[recordings['deployment_code']==deploymentID]

    # create output dict
    makara['lat'] = deployment['deployment_latitude'].values[0]
    makara['lon'] = deployment['deployment_longitude'].values[0]
    makara['depth'] = deployment['deployment_water_depth_m'].values[0]
    makara['site_code'] = deployment['site_code'].values[0]
    makara['start_datetime']= recording['recording_usable_start_datetime'].values[0]
    makara['end_datetime'] = recording['recording_usable_end_datetime'].values[0]
    makara['sampling_rate'] = recording['recording_sample_rate_khz'].values[0]*1000
    makara['device'] = recording['recording_device_code'].values[0]

    # retrieve device SN and sysgain
    device = devices[devices['device_code'] == makara['device']]
    try:
        sysgain_setting_makara = eval(recording['recording_settings_json'])
    except:
        sysgain_setting_makara = None
    if sysgain_setting_makara is None:
        print('Warning: sysgain value not found in Makara for that deployment. Using default gain settings: ' + sysgain_setting)
        gain = eval(device['device_specifications_json'].values[0])
        sysgain = gain[sysgain_setting]
    else:
        sysgain = sysgain_setting_makara['GAIN']
    makara['sysgain'] = sysgain
    makara['instrument'] = device['device_type_code']

    return dict(makara)

def main():
    yaml.add_representer(QuotedStr, quoted_str_representer)

    # load configuration
    deploymentID, audio_file_format, processing_queue_dir, default_gain_setting, fmin_hz, audio_root_dir, results_root_dir, makara_xlsfile_path, variableAttributes_template_path, globalAttributes_template_path = configuration()

    # defines all output paths:
    deployment_outdir_path = os.path.join(results_root_dir,deploymentID)
    meta_outdir_path = os.path.join(deployment_outdir_path,'META')
    nc_outdir_path = os.path.join(deployment_outdir_path, 'NC')
    json_outdir_path = os.path.join(deployment_outdir_path, 'JSON')
    logs_outdir_path = os.path.join(deployment_outdir_path, 'JOB_AGENT_LOGS')

    # creates all output folders
    os.makedirs(meta_outdir_path, exist_ok=True)
    os.makedirs(nc_outdir_path, exist_ok=True)
    os.makedirs(json_outdir_path, exist_ok=True)

    # search for audio dir
    tmp_audio_dir = search_folder_path(folder_name=deploymentID,search_root=audio_root_dir)
    audio_folder_path, audio_count = find_folder_with_most_audio_files(search_root=str(tmp_audio_dir[0]), files_extension=audio_file_format)
    if audio_folder_path:
        print(f" Audio folder: {audio_folder_path} ({audio_count} files)")
    else:
        print("No audio folder found.")

    # search for xml dir
    xml_folder_path, xml_count = find_folder_with_most_audio_files(search_root=str(tmp_audio_dir[0]),                                                                   files_extension='.xml')
    if audio_folder_path:
        print(f" XML folder: {xml_folder_path} ({xml_count} files)")
    else:
        print("No XML folder found.")

    # Find audio files prefix
    example_of_audio_file_name = get_nth_wav_file(audio_folder_path, n=10)
    audio_files_prefix = example_of_audio_file_name.stem.split('.')[0]

    # Find sampling rate
    info = sf.info(os.path.join(audio_folder_path,example_of_audio_file_name))
    sampling_rate = info.samplerate

    # get metadata from Makara
    makara = get_metadata_from_makara(xls_file=makara_xlsfile_path, deploymentID=deploymentID, sysgain_setting=default_gain_setting)

    # load template of PBP Global Atributes yaml
    globalAttributes_dict = load_yaml_file(globalAttributes_template_path)

    # Define frequency boundaries

    if makara['sampling_rate'] != sampling_rate:
        print('Warning! The sampling rate measured from audio file and from the one Makara are not the same. Using the measured sampling rate:' + str(sampling_rate))
    fmax_hz = int(sampling_rate/2)

    # write global attribute yanl file for the selecte deployment
    global_attrs_dict = write_pbp_glabalAttributes_file (
        template_dict = globalAttributes_dict,
        deploymentID=deploymentID,
        lat=makara['lat'],
        lon=makara['lon'],
        start_datetime = makara['start_datetime'],
        end_datetime=makara['end_datetime'],
        prefix=audio_files_prefix,
        fmin_hz=fmin_hz,
        fmax_hz = fmax_hz,
        instrument = makara['instrument'],
        device = makara['device'],
        sysgain = makara['sysgain'],
        meta_dir = meta_outdir_path,
        nc_dir = nc_outdir_path,
        json_dir = json_outdir_path,
        audio_dir = audio_folder_path,
        xnl_dir = xml_folder_path,
        logs_dir = logs_outdir_path,
        water_depth_m = makara['depth'],
    )

    # copy variables attributes yaml file
    shutil.copy(variableAttributes_template_path , global_attrs_dict['pbp_job_agent']['variable_attrs'])

    # copy Global attributes yaml file to central folder (if user entered path)
    if processing_queue_dir:
        shutil.copy(global_attrs_dict['pbp_job_agent']['global_attrs'],os.path.join(processing_queue_dir,os.path.split(global_attrs_dict['pbp_job_agent']['global_attrs'])[1]))

    print('YAML file created!')













if __name__ == "__main__":
    main()